# Resharding impact on clients, by proxy policy

Measures what connected .NET clients (StackExchange.Redis / NRedisStack) actually
experience when a Redis Enterprise database is resharded, and how that changes with
`proxy_policy`.

## The question

A database with its master shard and endpoint on node 1 is resharded; the new masters
and the endpoint migrate to node 3. How long are clients disconnected, how many
commands are dropped, how many need resending, and does `proxy_policy` change it?

Resharding produces up to three distinct disruption events, and the test separates them:

| Event | Expected under `single` | Expected under `all-*` |
|---|---|---|
| A. Slot/shard migration to new masters | absorbed by the proxy; latency only | same |
| B. Endpoint/proxy re-bind to node 3 | **hard cliff** - the sole active proxy moves node, so every client TCP connection dies | partial or none; a proxy already exists there |
| C. Proxy reconfiguration (new shard map) | brief | staggered across N proxies |

Hypothesis to falsify: `single` gives one total-outage cliff, while
`all-master-shards` / `all-nodes` give a smaller, staggered, partial impact.

## Design

- **Arms**: `control` (no reshard, the noise floor), `single`, `all-master-shards`, `all-nodes`.
- **Per arm**: create a fresh DB (1 master + 1 replica, `shards_placement: dense`,
  `proxy_policy` set at create time), reshard 1 -> 2 shards, delete it. Shard count
  cannot be *reduced* on a normal database, hence create/delete per arm.
- `proxy_policy` is set at create time on purpose: changing it on a live database is
  itself disruptive and would pollute the measurement.
- Load is **open loop**. A closed loop would absorb an outage as latency and hide the
  throughput hole.
- Client pinned to **StackExchange.Redis 2.8.x** (not 3.x). v3 is a new major with a
  rewritten transport; 2.8.x is what production .NET apps run, so findings transfer.
  Observed difference: on 3.1.13 failing ops park in a backlog until `syncTimeout`,
  whereas 2.8.58 fails fast - the client major version changes the failure signature.
- NRedisStack layers on SE.Redis and shares its connection stack, so connection-level
  findings apply to both. This is not two independent client tests.

## What gets measured

- **Outage windows** from a 10 ms PING probe on its own multiplexer.
- **Every operation classified**: `ok` / `fail_definite` (provably never sent, safe to
  retry) / `fail_ambiguous` (may have been applied) / `fail_server`.
- **Phantom writes** - INCRs the server applied but never acknowledged. The metric most
  people miss: a naive application retry double-applies them.
- **Lost writes** - acknowledged INCRs missing from the server. Must always be 0.
- **Throughput hole**, latency p50/p99/p99.9/max pre/during/post, reconnect counts.
- **`proxy_conns.csv`** - our own client connections per proxy address over time, via
  `bdb-cli <uid> --all-proxies client list`. This is the most direct evidence for the
  proxy-policy question: under `single` all connections sit on one proxy and must move
  wholesale; under `all-nodes` they can partially survive.
- Server-side topology timeline, plus `rladmin status extra all` before and after.

## Layout

```
reshard_bundle.sh      THE DELIVERABLE. One pasteable file for the node.
                       Generated - do not edit by hand.
harness/               C# client harness (the real SE.Redis / NRedisStack client)
  Program.cs           config, multiplexers, SE.Redis event hooks, DNS watcher,
                       reconciliation
  Infra.cs             UTC-anchored monotonic clock, 1 ms timer resolution,
                       CSV sinks, exception -> outcome classifier, marker tail
  Roles.cs             availability probe, load generator, correctness worker,
                       connectivity watcher
orchestrator/
  node_driver.py       runs ON a node: REST orchestration, topology polling,
                       ccs-cli / bdb-cli / rladmin capture. Never touches the
                       data path, so measured behaviour is the .NET client's.
  driver.py            same job but run remotely from Windows; usable once the
                       cluster is reachable from a desktop
tools/make_bundle.py   regenerates reshard_bundle.sh from harness/ + orchestrator/
prebuilt/
  ReshardProbe-linux-x64   35 MB self-contained binary; fallback when the node
                           has no outbound HTTPS (no .NET install needed)
validation/            how the harness was proven correct (see its README)
superseded/
  reshard_test.py      stdlib-only Python version with its own RESP client.
                       Rejected: it would not exercise the real .NET client.
                       Kept only as a no-dependency fallback.
docs/PLAN.md           original plan and decision record
```

## First results (2026-08-20, RE 8.0.10, 5-node cluster)

`control` and `single` completed; `all-master-shards` and `all-nodes` failed on a
database-naming bug (now fixed) and need re-running.

**Under `single`, resharding 1 -> 2 caused no measurable client impact.**

| arm | attempted | acked | ambiguous | phantom | lost | reshard |
|---|---|---|---|---|---|---|
| control | 36425 | 36425 | 0 | 0 | 0 | - |
| single | 39688 | 39688 | 0 | 0 | 0 | 13.5 s |

The reason is visible in the topology: both new masters moved to node3, but **the
endpoint stayed on node1**.

```
masters : node3, node3
replicas: node1, node2
endpoint: addrs=['172.16.22.11']   (node1, unchanged)
```

So event B (endpoint re-bind) never happened. The proxy on node1 kept serving and simply
forwarded to shards on node3, at the cost of an extra network hop. This **falsifies** the
hypothesis that `single` produces an outage cliff on reshard: with this policy the
endpoint does not follow the master shards, so connections survive.

It also means resharding alone does not reproduce "the endpoint migrated to node 3".
Whatever moved the endpoint in the original observation was something else - an explicit
`rladmin bind`, a node failure, or a later rebalance - and measuring endpoint migration
needs that step performed deliberately.

## Status

Harness validated end-to-end; two of four arms run against the cluster.

Validated against real Redis 7.0.15 with an injected `CLIENT PAUSE` fault:

| Check | Result |
|---|---|
| Injected 8000 ms stall | measured as an **8988 ms** outage window |
| `fail_ambiguous` (INCR) | 8 |
| `phantom_writes` | **8** - matches exactly |
| `lost_writes` | **0** |
| Server truth | 567x4 = 2268 = `attempted`, so every ambiguous op was in fact applied |
| Probe cadence | 600 pings in 6.000 s at 10 ms, no drift |
| Clean path | 598 acked = 299+299 server truth, 0 phantom, 0 lost |

Four real bugs were found by that validation and fixed: a startup crash on an
unreachable endpoint; an 11-minute reconcile loop (now hard-bounded); **`events.csv`
buffered rather than flushed**, which would have hung every arm because the driver
tails that file for `warmup_complete`; and the Linux apphost needing `DOTNET_ROOT`.

## Known caveats

- **Run on a node that hosts no shards of the test database.** The load generator would
  otherwise compete for CPU with the shard being measured. `node_driver.py` refuses if
  the local node holds a master *or replica* shard; `--allow-colocated` overrides.
  A local *proxy* only warns, because under `all-nodes` every node has one by definition
  and refusing would make the policy untestable.
- **Under `all-nodes` the client may connect to the local proxy.** The endpoint resolves
  to every node, so the client can end up served by the proxy on the machine it runs on,
  which a reshard elsewhere would never disturb. Check `proxy_conns.csv` for which proxy
  address actually served our connections before reading "no impact" as a property of
  the policy.
- Running on a cluster node keeps real cluster DNS and a real cross-node hop, but
  absolute latency is lower than a real remote app would see. The *comparison between
  policies* is unaffected.
- **Database names cannot contain underscores** (`^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]$`).
  The name is built from the hyphenated policy, while the underscored tag names only the
  output directory. `create_db` validates up front so the error names the cause.
- **Flex-shard / ASM databases**: scale-out completion is `is_balanced` in CCS, which
  REST cannot see. `node_driver.py` reads it via `ccs-cli` when `oss_sharding` is true.
  If that fails it warns and falls back to shard count, which can stop measuring early
  and under-report impact.
- `configCheckSeconds` stays at the default 60. It is likely the biggest client-side
  lever on recovery time and deserves a follow-up arm once baselines exist.
- The 1 -> 2 reshard also flips `sharding: true` and installs `shard_key_regex`, so
  "enable sharding" and "add a shard" happen together. That is the realistic customer
  path; a pure 2 -> 4 arm would separate them.
- **REST mutations are master-only.** The orchestrator resolves the master up front
  (`/v1/nodes` `role`, else the `ROLE` column of `rladmin status nodes`) and still
  follows a `307` as a backstop if it moves mid-run. When reading `rladmin` output
  note that the leading `*` marks the local node, not the master. Python's urllib refuses to follow redirects for
  non-GET methods, so this had to be handled explicitly. Covered by
  `validation/test_redirect.py`.
- The cluster was not reachable from the Windows workstation during development
  (the lab DNS zone was absent from the VPN's resource list, and there was no route to
  the lab subnet), which is why everything runs from a node.

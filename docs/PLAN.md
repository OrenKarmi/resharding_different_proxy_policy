# Measuring client impact of database resharding under different proxy policies

## Context

When a Redis Enterprise database with its master shard and endpoint on node 1 is resharded,
the new masters and the endpoint migrate to node 3. We need to quantify what connected .NET
clients (StackExchange.Redis / NRedisStack) actually experience: how long they are
disconnected, how many commands are dropped, how many have an *unknown* outcome, and how
that changes with `proxy_policy`.

The hypothesis to falsify: **`single` produces one total-outage cliff** (the sole active proxy
moves node, so every client TCP connection dies), while **`all-master-shards` / `all-nodes`
produce a smaller, staggered, partial impact** (a proxy already exists on the target node).

Resharding produces up to three distinct disruption events, and the test must separate them:

| Event | Expected under `single` | Expected under `all-*` |
|---|---|---|
| A. Slot/shard migration to new masters | absorbed by proxy; latency only | same |
| B. Endpoint/proxy rebind to node 3 | **hard cliff** — all connections dropped | partial or none |
| C. Proxy reconfiguration (new shard map) | brief | staggered across N proxies |

Intended outcome: a per-policy report with outage duration, dropped/ambiguous command counts,
throughput hole, and recovery time — plus a defensible client-configuration recommendation.

## Decisions already made

- OSS Cluster API **disabled** — single logical endpoint, proxy does all routing. Keeps the
  measurement focused on the proxy/endpoint variable.
- Harness runs **on this Windows machine**, off-cluster, over the same DNS path a real app uses.
- **StackExchange.Redis pinned to 2.8.x** (not the 3.1.13 NuGet resolved by default). v3 is a new
  major with a rewritten transport; 2.8.x is what production .NET apps run, so findings transfer.
- Arms: `single`, `all-master-shards`, `all-nodes` + a **no-reshard control**. No repeats.
- Per arm: create a fresh DB, reshard it, delete it. Avoids the fact that shard count cannot be
  reduced on a non-flex database.
- **Source DB spec per arm**: 1 master + 1 replica (`replication: true`), `shards_placement: dense`,
  `proxy_policy` set to the arm under test. Reshard target: **2 shards** (→ 2 masters + 2 replicas),
  which reproduces the observed "2 new masters + endpoint on node 3".

## Prerequisites — still needed from the user

Nothing below can run without these:

1. Cluster management address + REST port (usually 9443) and admin user/password.
2. Node inventory (the 3 node IPs/FQDNs) to confirm which node the endpoint lands on.
3. DB password to set on the created databases (or "no auth"), and whether TLS should be enabled.
4. `memory_size` to use for the test DBs.

## Already built and verified

- .NET SDK 8.0.424 installed user-local at `%USERPROFILE%\.dotnet` (no admin, removable).
- Harness project at `scratchpad/ReshardProbe/` — builds clean, smoke-tested against a dead port.
  - `Infra.cs` — single UTC-anchored monotonic clock; 1 ms Windows timer resolution via
    `timeBeginPeriod` + hybrid spin wait (a 10 ms probe cadence is impossible with `Task.Delay`
    alone); channel-backed CSV sinks so disk I/O never lands inside a measured latency;
    exception→outcome classifier; marker-file tail for correlating the reshard trigger.
  - `Roles.cs` — availability probe, load generator, correctness worker, connectivity watcher.
  - `Program.cs` — config, three separate multiplexers, SE.Redis event hooks, DNS watcher,
    reconciliation, `meta.json` / `reconcile.json`.
- Two real bugs already found and fixed by the smoke test:
  - startup counter-reset was unguarded → an unreachable endpoint crashed the process instead of
    recording the outage. Now retries, then falls back to baselining.
  - reconcile retry loop ran 11 minutes against a dead endpoint. Now bounded by
    `--reconcile-budget-sec` (default 120).

## Remaining work

### 1. Pin the client version and rebuild
`ReshardProbe.csproj`: change `StackExchange.Redis` from `3.1.13` to the latest `2.8.*`.
Keep `NRedisStack` (its version must be compatible with SE.Redis 2.x — may need to drop from
1.7.3). Rebuild; the last config edit (`ReconcileBudgetSec`) is not yet compiled.

NRedisStack layers on SE.Redis and shares its connection stack, so connection-level findings apply
to both clients. The report must state this explicitly rather than implying two independent tests.

### 2. Driver script — `scratchpad/driver.py` (Python 3.14, stdlib only)

Per arm, via the Redis Enterprise REST API (`urllib` + basic auth, TLS verify off for self-signed):

1. `POST /v1/bdbs` — `{name, type: "redis", memory_size, shards_count: 1, replication: true,
   shards_placement: "dense", proxy_policy: <arm>, authentication_redis_pass}`
2. Poll `GET /v1/bdbs/<uid>` until `status == "active"`; record endpoint DNS name/port.
3. Record pre-state: `GET /v1/shards` (master/replica → node) and endpoint placement.
   Assert master + endpoint are on node 1; abort the arm if not, since that is the premise.
4. Launch `ReshardProbe.exe` with `--markers markers.txt`, wait for `warmup_complete`.
5. Append trigger timestamp to `markers.txt`, then
   `PUT /v1/bdbs/<uid>` — `{sharding: true, shards_count: 2, shard_key_regex: DEFAULT_REGEX_RULES}`.
   Regex must be the repo's value, from
   [common.py:5](rl-automation/rs_test_helpers/rs_test_helpers/infra/database/common.py#L5):
   `[{"regex": ".*\\{(?<tag>.*)\\}.*"}, {"regex": "(?<tag>.*)"}]`
6. Poll every 500 ms → `topology.csv`: shard count, per-shard node/role, endpoint node,
   `status`. Append a marker at each transition. This is what separates events A, B and C.
7. Wait for completion, then keep load running 120 s+ (recovery tails matter).
8. Stop harness, `DELETE /v1/bdbs/<uid>`, wait for removal.

**Completion signal:** for a normal database, `shards_count == 2` and `status == "active"`.
If `GET /v1/bdbs/<uid>` reports `oss_sharding: true` (flex-shard/ASM), scale-out completion is
*not* shard count — it is `is_balanced == "enabled"`, per
[reshard_helper.py:619](rl-automation/rs_test_helpers/rs_test_helpers/dmc/reshard_helper.py#L619).
Detect and branch, or the run stops measuring too early and under-reports impact.

The control arm runs identically but skips steps 5–7's trigger.

### 3. Analysis script — `scratchpad/analyze.py`

Merges `probe.csv`, `ops.csv`, `events.csv`, `topology.csv`, `reconcile.json` per arm:

- **Outage windows** from the 10 ms probe: maximal spans with no successful PING → count,
  longest, total. Aligned against the event-B marker.
- **Connections dropped** from `ConnectionFailed` / `ConnectionRestored` / `isconnected_*` edges.
- **Command outcomes**: `ok` / `fail_definite` / `fail_ambiguous` / `fail_server`, with the error
  taxonomy (`RedisTimeoutException`, `RedisConnectionException` + `FailureType`, `SocketException`,
  `MOVED`/`LOADING`/proxy routing errors).
- **Correctness**: `phantom_writes` (server applied, client never acked — unsafe to retry) and
  `lost_writes` (must be 0), from the INCR reconciliation.
- **Throughput hole**: ops/s in 100 ms buckets; lost ops = ∫(baseline − actual).
- **Latency**: p50/p99/p99.9/max per bucket; pre / during / post.
- **Time-to-recovery** vs **time-to-latency-baseline** (usually much longer, and the honest number).
- **DNS timeline** — when the endpoint FQDN started resolving to node 3 vs the server-side rebind.
- Control-arm values are **subtracted as the noise floor**; without this a reshard-caused timeout
  cannot be distinguished from ordinary jitter.

### 4. Run the matrix and write the report

Per arm: 60 s warmup → trigger → wait for completion → 120 s tail. Roughly 6–10 min per arm.
Output a comparison table across the three policies plus the control, the event-A/B/C timeline
per policy, and client-config recommendations grounded in what was measured.

## Key risks

- **Changing `proxy_policy` is itself disruptive.** Avoided by construction: the policy is set at
  DB-create time, before any client connects.
- **Sequencing:** `SyncTimeout` (5000 ms default) is the floor on how long an ambiguous op hangs,
  and `KeepAlive` (60 s default) bounds dead-socket detection if the old proxy vanishes without a
  TCP RST. Both are pinned and reported, not left implicit.
- `configCheckSeconds` default 60 governs how fast the multiplexer notices a topology change.
  Not a separate arm per the user's choice, but it must be reported as a likely dominant lever.
- Load is **open-loop on purpose** — a closed loop would absorb an outage as latency and hide the
  throughput hole.

## Verification

1. `dotnet build -c Release` clean, and `meta.json` confirms SE.Redis is 2.8.x, not 3.x.
2. Re-run the dead-port smoke test: must exit 0, write all five outputs, and finish within the
   reconcile budget (regression test for both fixed bugs).
3. Dry-run the driver against the real cluster with `--dry-run`: create the DB, read placement,
   delete it — no reshard, no harness. Confirms REST auth, the DB spec, and that master+endpoint
   really do land on node 1.
4. Run the **control arm first**. Expect ~0 outage and near-zero errors; a noisy control means the
   harness or network is the problem, not resharding, and must be fixed before the real arms.
5. Then the three policy arms. Sanity checks: `lost_writes == 0` in every arm; `topology.csv` shows
   the endpoint actually moving to node 3; the `single` arm shows a probe outage aligned with the
   endpoint-rebind marker.

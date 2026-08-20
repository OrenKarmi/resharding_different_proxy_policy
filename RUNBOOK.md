# Runbook - steps to execute

Everything runs on a Redis Enterprise cluster node.

## Before you start

Pick a node that does **not** host the test database. The test DB is small
(1 shard + 1 replica, dense placement), so it will land on one or two nodes; run from a
different one. `check` and `dryrun` below tell you where it landed, and the tooling
refuses to run co-located unless you pass `--allow-colocated`.

Requirements on that node: `python3`, `curl` or `wget`, and outbound HTTPS to
`dot.net` + `nuget.org` **during step 2 only**. No root needed; nothing is installed
system-wide.

---

## Step 1 - get the code onto the node

Clone the repo on the node:

```bash
git clone https://github.com/orenkarmi/resharding_different_proxy_policy.git
cd resharding_different_proxy_policy
```

Everything you need is `reshard_bundle.sh`; the rest of the repo is the source of truth
it was generated from, plus docs and validation.

If the node cannot reach GitHub, the fallback is to paste the single file instead:

```bash
cat > reshard_bundle.sh <<'ENDOFBUNDLE'
   ... paste the entire contents of reshard_bundle.sh here ...
ENDOFBUNDLE

bash -n reshard_bundle.sh      # expect no output
```

## Step 2 - build

```bash
bash reshard_bundle.sh setup
```

Extracts the C# sources, installs the .NET SDK to `$HOME/.dotnet`, and builds the
harness. Expect it to end with `[bundle] OK. Harness built at ...`.

**If this fails on the download**, the node has no outbound HTTPS. Stop and tell me;
the fallback is `prebuilt/ReshardProbe-linux-x64`, a 35 MB self-contained binary that
needs no .NET at all.

## Credentials

The examples below read credentials from the shell so they never land in a file or in
shell history shared with a repo. Set them once per session on the node:

```bash
export RL_USER='admin@example.org'      # cluster REST user
export RL_PASS='...'                    # cluster REST password
```

The orchestrator also accepts `RL_REST_USER` / `RL_REST_PASSWORD` directly, in which
case you can omit `--user` / `--password` entirely.

## Step 3 - check connectivity and inventory

```bash
bash reshard_bundle.sh check --user "$RL_USER" --password "$RL_PASS"
```

Prints the cluster name, which node you are on, all nodes, and existing databases.
Read-only; creates nothing.

## A note on the cluster master

Redis Enterprise serves REST **reads** from any node but only accepts **mutations**
(creating/resharding/deleting a database) on the cluster master, answering `307`
elsewhere. The orchestrator now follows that redirect automatically, preserving the
method and body, and remembers the master for subsequent calls - so you can run from
any node, and a master failover mid-matrix costs one redirect rather than a failed run.

You will see a line like this in the output, which is expected and harmless:

```
REST POST https://<node-ip>:9443/v1/bdbs -> HTTP 307, following redirect to https://<master-ip>:9443
```

If you would rather aim at the master directly, `--rest-host <master-ip>` still works.
It only affects REST orchestration and never the measured data path.

## Step 4 - dry run

```bash
bash reshard_bundle.sh dryrun --user "$RL_USER" --password "$RL_PASS" \
    --db-password 'testpass'
```

Creates one DB, reports shard/endpoint placement, resolves the endpoint FQDN, does a
real SET/GET round-trip, then deletes it. Confirms the premise of the whole test:
master + endpoint start on a single node.

**Check the output for:**
- `this node is not hosting the DB - good for the real run` - if instead it warns that
  this node hosts the master/endpoint, switch nodes.
- `oss_sharding=True` - if present, tell me. It means flex-shard/ASM, and completion
  detection needs `ccs-cli`; results could otherwise under-report impact.

## Step 5 - run the matrix

```bash
bash reshard_bundle.sh matrix --user "$RL_USER" --password "$RL_PASS" \
    --db-password 'testpass'
```

Runs four arms in order: `control`, `single`, `all-master-shards`, `all-nodes`.
Roughly 8-10 minutes per arm, so **35-45 minutes total**. It prints progress per arm.

Per arm it creates the DB, warms up 60 s, triggers the reshard, polls topology until
complete, holds 120 s for the recovery tail, stops the harness cleanly so reconciliation
runs, then deletes the DB.

To run a single arm instead:

```bash
bash reshard_bundle.sh arm --policy single --user "$RL_USER" \
    --password "$RL_PASS" --db-password 'testpass'
```

## Step 6 - collect

```bash
bash reshard_bundle.sh collect
```

Prints the path of a `reshard_results_<timestamp>.tgz` in `$HOME`. Copy that back and
give it to me, and I will produce the cross-policy comparison.

## Step 7 - clean up (optional)

```bash
rm -rf $HOME/.dotnet $HOME/reshard_probe
```

The test databases are deleted automatically at the end of each arm. If a run was
interrupted, check for leftovers named `reshardtest-*`:

```bash
rladmin status databases | grep reshardtest
```

---

## Useful options

| Option | Default | Why change it |
|---|---|---|
| `--memory-size` | 1073741824 (1 GB) | smaller if the cluster is tight on RAM |
| `--target-shards` | 2 | 4 gives a longer migration phase, separating events A and B |
| `--load-rate` | 200 (per connection) | lower if you are worried about node load |
| `--load-connections` | 4 | more connections = clearer view of partial survival |
| `--warmup` / `--tail` | 60 / 120 s | longer tail if recovery looks incomplete |
| `--config-check-sec` | 60 | SE.Redis topology-refresh interval; likely the biggest client-side lever on recovery time. Worth a follow-up arm at 5. |
| `--keep-db` | off | leave the DB in place to inspect after a failure |
| `--allow-colocated` | off | run anyway on a node hosting the test DB (degrades latency numbers) |
| `--rest-host` | localhost | aim REST at a specific node, e.g. the cluster master. Orchestration only; never affects the data path. |
| `--no-proxy-conns` | off | skip `bdb-cli` sampling; loses the clearest endpoint-migration evidence |

## If something goes wrong

- **`warmup_complete` never appears** - the harness could not reach the endpoint. Check
  `results/<arm>/harness_stdout.log`.
- **An arm fails mid-matrix** - the remaining arms still run; the failure is recorded in
  `results/matrix_results.json`. Leftover DBs are still deleted by the cleanup path.
- **`HTTP 307` on a create/reshard/delete** - should now be followed automatically. If
  it still fails, pass `--rest-host <master-ip>` (the master IP appears in the 307 body).
- **Nothing in `proxy_conns.csv`** - `bdb-cli` was not runnable. Not fatal; the rest of
  the measurement is unaffected.

Send me the tarball plus anything unexpected in the console output.

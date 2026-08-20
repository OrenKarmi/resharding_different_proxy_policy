# Validation

How the harness was proven correct before ever touching the cluster. Everything here
runs against a local Redis in WSL, not against a Redis Enterprise cluster.

## Why fault injection

The centrepiece metric is **phantom writes**: operations the server applied but never
acknowledged. Proving that arithmetic needs a fault whose ground truth is known in
advance. `CLIENT PAUSE <ms>` with `ms` greater than the client's `syncTimeout` does it
exactly: commands sent during the pause time out client-side, but the server still
executes them when the pause lifts. Expected outcome is therefore
`phantom_writes == fail_ambiguous` and `lost_writes == 0`.

## Scripts

| Script | Purpose |
|---|---|
| `start_redis.sh` | start a clean Redis in WSL for validation |
| `setup_wsl.sh` | install .NET in WSL and build the harness from `harness/` |
| `validate_wsl.sh` | full run with `CLIENT PAUSE` injection against the dev build |
| `validate_bundle.sh` | the same, but against the harness built from `reshard_bundle.sh` |
| `test_bundle.sh` | prove the self-contained binary runs with no .NET on `PATH` |
| `resp_probe.py` | short-lived connection behaviour, independent of any client library |
| `longevity.py` | how long a persistent connection survives |
| `inspect.sh` | dump a run's outputs for debugging |
| `test_redirect.py` | 307 redirect handling: method/body/auth preserved, master cached, loops bounded. No cluster needed. |

## Results

Injected 8000 ms stall, `syncTimeout` 3000 ms:

```
outage window measured   8988 ms
fail_ambiguous (INCR)    8
phantom_writes           8      <- matches exactly
lost_writes              0
attempted / applied      2268 / 2268
server truth             567 x 4 workers = 2268
```

Clean path, no fault: 598 attempted, 598 acked, server truth 299+299=598, 0 phantom,
0 lost. Probe cadence 600 pings in 6.000 s at 10 ms with no drift. `socks=1` per
multiplexer, i.e. one stable connection and no reconnect churn.

Unreachable endpoint: all operations classified `fail_definite`, never falsely
`ambiguous`.

## Bugs this found

1. **Startup crash on an unreachable endpoint.** The counter reset was unguarded, so the
   process died instead of recording the outage. Now retries, then falls back to
   baselining pre-run counter values.
2. **11-minute reconcile loop.** 12 retries x 30 s timeouts against a dead endpoint.
   Now hard-bounded by `--reconcile-budget-sec` (62 s observed).
3. **`events.csv` was buffered, not flushed.** The driver tails that file waiting for
   `warmup_complete`, so *every arm would have hung* and then aborted. That one would
   have wasted an entire cluster session. Now written with `AutoFlush`.
4. **Linux apphost needs `DOTNET_ROOT`.** `PATH` alone is not enough; the binary exits
   with "You must install .NET to run this application".

## Bug found on the cluster (2026-08-20)

`dryrun` failed from node `re-n5` with `DB create failed HTTP 307`. Redis Enterprise
only accepts REST mutations on the cluster master and redirects elsewhere, but
`urllib.request` follows redirects for GET/HEAD only, so the POST surfaced the 307 as a
failure. `check` had passed because it is all GETs.

`test_redirect.py` covers the fix without a cluster: two local HTTP servers, one
307-ing to the other, asserting the method stays `POST`, the JSON body and
`Authorization` header survive, `self.base` is updated to the redirect origin so later
mutations go direct, and a self-redirect loop terminates at the hop cap. That last
assertion matters most - a redirect mishandled as a GET would silently turn "create
database" into a no-op that still looks successful.

Writing the test also exposed two test-harness faults worth noting: an HTTP server that
replies without draining the request body makes the client's next write reset the
connection (masking the hop cap), and the original hop-cap path returned a bare 307 with
an empty body instead of a diagnosable message.

## Three bugs found while fixing the 307 (2026-08-20)

Reported as "the master moved to node 5, the fix did not work". Two of those premises
were wrong and one uncovered real defects.

1. **The master had not moved.** `rladmin status nodes` showed `ROLE=master` on node 1;
   the `*` beside node 5 marks the *local* node. The 307 target was correct all along.
2. **The bundle ran stale code.** `run_driver` executed the `node_driver.py` that a
   previous `setup` had extracted into `$WORKDIR`, so a cloned fix had no effect. It now
   re-extracts on every invocation. This is why a fixed bug appeared to persist.
3. **`SystemExit` escaped master discovery.** `Rest.get` raises `SystemExit`, which does
   not subclass `Exception`, so `except Exception` did not catch it and a REST failure
   aborted the run instead of falling back to `rladmin`.

Also fixed along the way: a literal CR byte got into the generator instead of the two
characters ``, producing `sed: unterminated 's' command` on every run; and the first
staleness check compared mtimes, which `extract()` always bumps, so it warned on every
clean run. It now compares a digest of the sources against one recorded at build time.

## A wrong hypothesis worth recording

When the first run against a live Redis failed completely, the suspicion was that
`Pacer`'s spin-wait was starving the thread pool SE.Redis needs for I/O completions.
That was **wrong**: `WORKER: (Busy=2, Free=32765)` showed the pool idle, and the real
signal was `SocketClosed (ReadEndOfStream) ... last: ECHO`, i.e. the server closing the
connection.

Root cause: the **WSL2 localhost relay** aborts persistent connections at ~15 s
(`WinError 10053`) and degrades further under reconnect churn, while plain Python
sockets still connect. Redis itself was innocent (`timeout 0`, `redis-cli` fine).
Reproduced at 14.90 s and 15.10 s by `longevity.py`.

Fix: run the harness *inside* WSL over Linux loopback rather than through the relay.
This is a property of the dev environment only and does not affect the cluster test.

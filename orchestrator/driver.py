#!/usr/bin/env python3
"""
Driver for the proxy-policy / resharding client-impact test.

Creates a fresh 1-shard database per arm, runs the ReshardProbe client harness
against it, triggers a reshard to 2 shards, records the server-side topology
timeline, then deletes the database so the next arm starts clean. Resharding is
one-way on a non-flex database, hence create/delete per arm.

REST access is stdlib-only (urllib + ssl), modelled on the RestClient in
~/Code/redis_rebalance/balance.py, which was written against this same cluster API.

Subcommands:
  check    Connectivity + cluster/node inventory. Read-only, mutates nothing.
  dryrun   Create a DB, report shard/endpoint placement, delete it. No load.
  arm      One full arm: create -> load -> reshard -> observe -> delete.
  matrix   The control arm followed by the three proxy-policy arms.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

# Matches rs_test_helpers/infra/database/common.py:5 — the hashing policy applied
# when sharding is enabled. Must be identical across arms or slot remapping gets
# conflated with the proxy_policy variable under test.
DEFAULT_REGEX_RULES = [{"regex": r".*\{(?<tag>.*)\}.*"}, {"regex": r"(?<tag>.*)"}]

# rs_test_helpers/infra/database/common.py:11
POLICIES = ["single", "all-master-shards", "all-nodes"]

CONTROL_ARM = "control"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(msg: str) -> None:
    print(f"[{utc_now()}] {msg}", flush=True)


class RestClient:
    """Redis Enterprise Software REST API client. Read-only GETs raise on failure;
    mutations return (ok, status, data) so a single failed call is reported rather
    than crashing a run mid-measurement."""

    def __init__(self, host: str, user: str, password: str, port: int = 9443,
                 verify_tls: bool = False, timeout: int = 30) -> None:
        self.base = f"https://{host}:{port}"
        self.timeout = timeout
        self._auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        self._ctx = ssl.create_default_context()
        if not verify_tls:
            # Redis Enterprise clusters normally present a self-signed certificate.
            self._ctx.check_hostname = False
            self._ctx.verify_mode = ssl.CERT_NONE

    def _open_url(self, method: str, url: str, body: Any = None,
                  timeout: Optional[float] = None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Authorization": self._auth, "Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        return urllib.request.urlopen(
            req, timeout=self.timeout if timeout is None else timeout, context=self._ctx)

    def _open(self, method: str, path: str, body: Any = None, timeout: Optional[float] = None):
        return self._open_url(method, self.base + path, body, timeout=timeout)

    def get(self, path: str, timeout: Optional[float] = None) -> Any:
        try:
            with self._open("GET", path, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise SystemExit("REST auth failed (401). Check --user / --password.")
            raise SystemExit(f"REST GET {path} -> HTTP {exc.code}: {exc.read()[:300]!r}")
        except urllib.error.URLError as exc:
            raise SystemExit(f"REST GET {path} failed: {exc.reason} (check --host).")
        except (ValueError, OSError) as exc:
            raise SystemExit(f"REST GET {path} error: {exc}")

    def mutate(self, method: str, path: str, body: Any = None,
               timeout: Optional[float] = None,
               max_redirects: int = 3) -> tuple[bool, int, Any]:
        """POST/PUT/DELETE, following redirects while preserving method and body.

        Redis Enterprise serves reads from any node but redirects mutations to the
        cluster master with a 307. urllib only follows redirects for GET/HEAD -
        HTTPRedirectHandler.redirect_request returns None for other methods - so
        without this every mutation fails unless aimed at the current master, and
        a master change mid-run would break an in-flight matrix.
        """
        url = self.base + path
        cur_method, cur_body = method, body
        for hop in range(max_redirects + 1):
            try:
                with self._open_url(cur_method, url, cur_body, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8", "replace")
                    try:
                        data = json.loads(raw) if raw.strip() else {}
                    except ValueError:
                        data = raw
                    return True, getattr(resp, "status", 200), data
            except urllib.error.HTTPError as exc:
                loc = exc.headers.get("Location") if exc.headers else None
                if exc.code in (301, 302, 303, 307, 308) and loc:
                    if hop >= max_redirects:
                        return False, exc.code, (
                            f"too many redirects (>{max_redirects}) for "
                            f"{method} {path}; last Location: {loc}")
                    target = urllib.parse.urljoin(url, loc)
                    parts = urllib.parse.urlsplit(target)
                    new_base = f"{parts.scheme}://{parts.netloc}"
                    log(f"REST {cur_method} {url} -> HTTP {exc.code}, "
                        f"following redirect to {new_base}")
                    # Remember the master so later mutations go straight there.
                    if new_base != self.base:
                        self.base = new_base
                    if exc.code == 303:
                        # Per HTTP semantics a 303 becomes a GET without a body.
                        cur_method, cur_body = "GET", None
                    url = target
                    continue
                return False, exc.code, exc.read().decode("utf-8", "replace")[:500]
            except urllib.error.URLError as exc:
                return False, 0, str(exc.reason)
            except OSError as exc:
                return False, 0, str(exc)
        return False, 0, f"redirect handling fell through for {method} {path}"


# --------------------------------------------------------------------------- #
# Topology inspection
# --------------------------------------------------------------------------- #

def node_map(client: RestClient) -> dict[int, str]:
    """uid -> address, for reporting which node shards and the endpoint sit on."""
    return {int(n["uid"]): n.get("addr") or n.get("external_addr") or "?"
            for n in client.get("/v1/nodes")}


def db_topology(client: RestClient, uid: int) -> dict[str, Any]:
    """Current shard placement and endpoint binding for one database."""
    bdb = client.get(f"/v1/bdbs/{uid}")
    shards = [s for s in client.get("/v1/shards") if int(s.get("bdb_uid", -1)) == uid]

    masters = sorted(int(s["node_uid"]) for s in shards if s.get("role") == "master")
    replicas = sorted(int(s["node_uid"]) for s in shards if s.get("role") == "slave")

    # An endpoint may be advertised on several nodes under all-* proxy policies.
    endpoint_nodes: list[int] = []
    dns_name = None
    port = None
    for ep in bdb.get("endpoints") or []:
        dns_name = ep.get("dns_name") or dns_name
        port = ep.get("port") or port
        for a in ep.get("addr") or []:
            endpoint_nodes.append(a)

    return {
        "status": bdb.get("status"),
        "shards_count": bdb.get("shards_count"),
        "replication": bdb.get("replication"),
        "proxy_policy": bdb.get("proxy_policy"),
        "shards_placement": bdb.get("shards_placement"),
        "oss_sharding": bdb.get("oss_sharding", False),
        "master_nodes": masters,
        "replica_nodes": replicas,
        "endpoint_addrs": sorted(set(str(a) for a in endpoint_nodes)),
        "dns_name": dns_name,
        "port": port,
        "shard_detail": sorted(
            (int(s["uid"]), s.get("role"), int(s["node_uid"])) for s in shards),
    }


def describe(topo: dict[str, Any], nodes: dict[int, str]) -> str:
    def nn(uids):
        return ", ".join(f"node{u}({nodes.get(u, '?')})" for u in uids) or "none"
    return (f"status={topo['status']} shards={topo['shards_count']} "
            f"policy={topo['proxy_policy']} placement={topo['shards_placement']}\n"
            f"    masters : {nn(topo['master_nodes'])}\n"
            f"    replicas: {nn(topo['replica_nodes'])}\n"
            f"    endpoint: {topo['dns_name']}:{topo['port']} addrs={topo['endpoint_addrs']}")


# --------------------------------------------------------------------------- #
# Database lifecycle
# --------------------------------------------------------------------------- #

def create_db(client: RestClient, name: str, policy: str, memory_size: int,
              password: Optional[str]) -> int:
    """Create the source DB for one arm: 1 master + 1 replica, dense placement,
    proxy_policy set at create time so no disruptive policy change is needed later."""
    body: dict[str, Any] = {
        "name": name,
        "type": "redis",
        "memory_size": memory_size,
        "shards_count": 1,
        "replication": True,
        "shards_placement": "dense",
        "proxy_policy": policy,
    }
    if password:
        body["authentication_redis_pass"] = password

    ok, status, data = client.mutate("POST", "/v1/bdbs", body)
    if not ok:
        raise SystemExit(f"DB create failed: HTTP {status}: {data}")
    uid = int(data["uid"])
    log(f"created bdb uid={uid} name={name} proxy_policy={policy}")
    return uid


def wait_status(client: RestClient, uid: int, want: str = "active",
                timeout: float = 300) -> str:
    deadline = time.monotonic() + timeout
    last = "?"
    while time.monotonic() < deadline:
        last = str(client.get(f"/v1/bdbs/{uid}").get("status"))
        if last == want:
            return last
        if last in ("creation-failed", "delete-failed", "recovery-failed"):
            raise SystemExit(f"bdb {uid} entered terminal status {last}")
        time.sleep(2)
    raise SystemExit(f"bdb {uid} status {last!r}, wanted {want!r} within {timeout:.0f}s")


def delete_db(client: RestClient, uid: int, timeout: float = 300) -> None:
    ok, status, data = client.mutate("DELETE", f"/v1/bdbs/{uid}")
    if not ok:
        log(f"WARNING: delete bdb {uid} failed HTTP {status}: {data}")
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client.get(f"/v1/bdbs/{uid}")
        except SystemExit:
            log(f"deleted bdb uid={uid}")
            return
        time.sleep(2)
    log(f"WARNING: bdb {uid} still present after {timeout:.0f}s")


def trigger_reshard(client: RestClient, uid: int, target_shards: int) -> tuple[bool, Any]:
    """Enable sharding and scale to target_shards. Mirrors
    rs_test_helpers/cluster/database.py:reshard()."""
    body = {
        "sharding": True,
        "shards_count": target_shards,
        "shard_key_regex": DEFAULT_REGEX_RULES,
    }
    ok, status, data = client.mutate("PUT", f"/v1/bdbs/{uid}", body)
    return ok, (data if not ok else f"HTTP {status}")


def reshard_complete(client: RestClient, uid: int, target_shards: int) -> bool:
    """Completion signal differs by database type.

    Flex-shard / ASM databases (oss_sharding=True) are NOT done when shards_count
    reaches target — scale-out completes when CCS reports is_balanced=enabled. That
    field is CCS-only and unavailable over REST, so for flex databases we fall back
    to shard-object count plus an active status, and warn that this may be early.
    See rs_test_helpers/dmc/reshard_helper.py:619.
    """
    bdb = client.get(f"/v1/bdbs/{uid}")
    if str(bdb.get("status")) != "active":
        return False
    shards = [s for s in client.get("/v1/shards") if int(s.get("bdb_uid", -1)) == uid]
    masters = [s for s in shards if s.get("role") == "master"]
    return len(masters) >= target_shards


# --------------------------------------------------------------------------- #
# Arm execution
# --------------------------------------------------------------------------- #

def wait_for_event(events_csv: str, kind: str, timeout: float) -> bool:
    """Poll the harness event log for a named event (e.g. warmup_complete)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with open(events_csv, "r", encoding="utf-8", errors="replace") as fh:
                if any(kind in line for line in fh):
                    return True
        except FileNotFoundError:
            pass
        time.sleep(0.25)
    return False


def run_arm(client: RestClient, args: argparse.Namespace, policy: str) -> dict[str, Any]:
    is_control = policy == CONTROL_ARM
    db_policy = "single" if is_control else policy
    tag = policy.replace("-", "_")

    out_dir = os.path.join(args.outdir, tag)
    os.makedirs(out_dir, exist_ok=True)
    markers = os.path.join(out_dir, "markers.txt")
    topo_csv = os.path.join(out_dir, "topology.csv")
    open(markers, "w").close()

    nodes = node_map(client)
    db_name = f"{args.db_prefix}-{tag}"
    uid = create_db(client, db_name, db_policy, args.memory_size, args.db_password)

    result: dict[str, Any] = {"arm": policy, "bdb_uid": uid, "db_name": db_name}
    probe = None

    try:
        wait_status(client, uid, "active", timeout=args.create_timeout)
        pre = db_topology(client, uid)
        log(f"pre-reshard topology:\n    {describe(pre, nodes)}")
        result["pre"] = pre

        if pre["oss_sharding"]:
            log("WARNING: oss_sharding=True (flex/ASM). Scale-out completion cannot be "
                "read over REST (is_balanced is CCS-only); completion may be detected early.")

        if len(pre["master_nodes"]) != 1:
            raise SystemExit(f"expected exactly 1 master, got {pre['master_nodes']}")

        endpoint_host = pre["dns_name"]
        endpoint_port = pre["port"]
        if not endpoint_host or not endpoint_port:
            raise SystemExit(f"could not determine endpoint from bdb {uid}")

        # Clients connect via the endpoint FQDN so that DNS re-resolution after an
        # endpoint migration is part of the measured outage.
        endpoint = f"{endpoint_host}:{endpoint_port}"
        log(f"client endpoint: {endpoint}")

        probe_cmd = [
            args.probe_exe,
            "--endpoint", endpoint,
            "--tag", tag,
            "--outdir", out_dir,
            "--markers", markers,
            "--duration-sec", str(args.max_duration),
            "--warmup-sec", str(args.warmup),
            "--load-connections", str(args.load_connections),
            "--load-rate", str(args.load_rate),
            "--corr-workers", str(args.corr_workers),
            "--sync-timeout", str(args.sync_timeout),
            "--connect-timeout", str(args.connect_timeout),
            "--config-check-sec", str(args.config_check_sec),
            "--keepalive-sec", str(args.keepalive_sec),
        ]
        if args.db_password:
            probe_cmd += ["--password", args.db_password]
        if args.tls:
            probe_cmd.append("--tls")

        log(f"starting harness: {' '.join(probe_cmd[:6])} ...")
        probe_log = open(os.path.join(out_dir, "probe_stdout.log"), "w", encoding="utf-8")
        probe = subprocess.Popen(probe_cmd, stdout=probe_log, stderr=subprocess.STDOUT)

        events_csv = os.path.join(out_dir, "events.csv")
        if not wait_for_event(events_csv, "warmup_complete", args.warmup + 120):
            raise SystemExit("harness never reported warmup_complete")
        log("warmup complete; baseline established")

        topo_fh = open(topo_csv, "w", encoding="utf-8", newline="")
        topo_fh.write("utc,elapsed_s,status,shards_count,master_nodes,replica_nodes,endpoint_addrs\n")

        def sample(t0: float) -> dict[str, Any]:
            t = db_topology(client, uid)
            topo_fh.write(
                f"{utc_now()},{time.monotonic() - t0:.3f},{t['status']},{t['shards_count']},"
                f"\"{t['master_nodes']}\",\"{t['replica_nodes']}\",\"{t['endpoint_addrs']}\"\n")
            topo_fh.flush()
            return t

        def mark(text: str) -> None:
            with open(markers, "a", encoding="utf-8") as fh:
                fh.write(text + "\n")

        t0 = time.monotonic()

        if is_control:
            log(f"CONTROL arm: no reshard, holding load for {args.control_hold}s")
            mark("control_hold_start")
            end = time.monotonic() + args.control_hold
            while time.monotonic() < end:
                sample(t0)
                time.sleep(args.poll_interval)
            mark("control_hold_end")
            result["reshard"] = None
        else:
            mark(f"reshard_trigger shards=1->{args.target_shards}")
            log(f"TRIGGERING reshard 1 -> {args.target_shards}")
            ok, info = trigger_reshard(client, uid, args.target_shards)
            result["reshard"] = {"ok": ok, "info": info, "utc": utc_now()}
            if not ok:
                mark("reshard_trigger_failed")
                raise SystemExit(f"reshard trigger failed: {info}")

            # Poll topology; mark every observed transition so the client-side
            # timeline can be split into migration / endpoint-rebind / reconfig.
            prev_sig = None
            done_at = None
            deadline = time.monotonic() + args.reshard_timeout
            while time.monotonic() < deadline:
                t = sample(t0)
                sig = (t["status"], t["shards_count"],
                       tuple(t["master_nodes"]), tuple(t["endpoint_addrs"]))
                if sig != prev_sig:
                    mark(f"topology_change status={t['status']} shards={t['shards_count']} "
                         f"masters={t['master_nodes']} endpoint={t['endpoint_addrs']}")
                    prev_sig = sig
                if reshard_complete(client, uid, args.target_shards):
                    done_at = time.monotonic() - t0
                    mark(f"reshard_complete elapsed_s={done_at:.3f}")
                    log(f"reshard complete after {done_at:.1f}s")
                    break
                time.sleep(args.poll_interval)

            if done_at is None:
                mark("reshard_timeout")
                log(f"WARNING: reshard did not complete within {args.reshard_timeout}s")
            result["reshard_elapsed_s"] = done_at

            log(f"holding load for {args.tail}s to capture the recovery tail")
            tail_end = time.monotonic() + args.tail
            while time.monotonic() < tail_end:
                sample(t0)
                time.sleep(args.poll_interval)

            post = db_topology(client, uid)
            log(f"post-reshard topology:\n    {describe(post, nodes)}")
            result["post"] = post

        topo_fh.close()

        # Clean stop so the harness drains and reconciles; killing it would lose
        # the phantom/lost write counts, which are the point of the exercise.
        log("signalling harness to stop")
        mark("STOP")
        try:
            probe.wait(timeout=args.reconcile_budget + 120)
        except subprocess.TimeoutExpired:
            log("WARNING: harness did not exit; terminating")
            probe.terminate()
        probe = None
        probe_log.close()

        rec_path = os.path.join(out_dir, "reconcile.json")
        if os.path.exists(rec_path):
            with open(rec_path, encoding="utf-8") as fh:
                result["reconcile"] = json.load(fh).get("totals")
            log(f"reconcile totals: {result['reconcile']}")

    finally:
        if probe is not None and probe.poll() is None:
            log("cleaning up harness process")
            probe.terminate()
        if not args.keep_db:
            delete_db(client, uid)
        else:
            log(f"--keep-db set; leaving bdb {uid} in place")

    with open(os.path.join(out_dir, "arm_result.json"), "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2)
    return result


# --------------------------------------------------------------------------- #
# Subcommands
# --------------------------------------------------------------------------- #

def cmd_check(client: RestClient, args: argparse.Namespace) -> int:
    cluster = client.get("/v1/cluster")
    log(f"cluster name: {cluster.get('name')}")
    nodes = client.get("/v1/nodes")
    log(f"nodes: {len(nodes)}")
    for n in sorted(nodes, key=lambda x: int(x["uid"])):
        log(f"  node{n['uid']}: addr={n.get('addr')} status={n.get('status')} "
            f"shards={n.get('shard_count')} cores={n.get('cores')} "
            f"total_memory={n.get('total_memory')}")
    bdbs = client.get("/v1/bdbs")
    log(f"existing databases: {len(bdbs)}")
    for b in bdbs:
        log(f"  bdb{b['uid']}: name={b.get('name')} shards={b.get('shards_count')} "
            f"policy={b.get('proxy_policy')} status={b.get('status')}")
    return 0


def cmd_dryrun(client: RestClient, args: argparse.Namespace) -> int:
    nodes = node_map(client)
    name = f"{args.db_prefix}-dryrun"
    uid = create_db(client, name, "single", args.memory_size, args.db_password)
    try:
        wait_status(client, uid, "active", timeout=args.create_timeout)
        topo = db_topology(client, uid)
        log(f"dry-run topology:\n    {describe(topo, nodes)}")
        if len(topo["master_nodes"]) == 1:
            log(f"OK: single master on node{topo['master_nodes'][0]}")
        if topo["oss_sharding"]:
            log("NOTE: oss_sharding=True (flex/ASM) — completion detection is limited over REST")
    finally:
        if not args.keep_db:
            delete_db(client, uid)
    return 0


def cmd_arm(client: RestClient, args: argparse.Namespace) -> int:
    run_arm(client, args, args.policy)
    return 0


def cmd_matrix(client: RestClient, args: argparse.Namespace) -> int:
    arms = [CONTROL_ARM] + POLICIES
    results = []
    for arm in arms:
        log("=" * 72)
        log(f"ARM: {arm}")
        log("=" * 72)
        try:
            results.append(run_arm(client, args, arm))
        except SystemExit as exc:
            log(f"ARM {arm} FAILED: {exc}")
            results.append({"arm": arm, "error": str(exc)})
        if arm != arms[-1]:
            log(f"settling {args.between_arms}s before next arm")
            time.sleep(args.between_arms)

    path = os.path.join(args.outdir, "matrix_results.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2)
    log(f"matrix complete -> {path}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["check", "dryrun", "arm", "matrix"])
    p.add_argument("--policy", choices=POLICIES + [CONTROL_ARM],
                   help="which arm to run (for 'arm')")

    p.add_argument("--host", default=os.environ.get("RL_REST_HOST"),
                   help="cluster management address (or env RL_REST_HOST)")
    p.add_argument("--port", type=int,
                   default=int(os.environ.get("RL_REST_PORT", "9443")))
    p.add_argument("--user", default=os.environ.get("RL_REST_USER"))
    p.add_argument("--password", default=os.environ.get("RL_REST_PASSWORD"))
    p.add_argument("--verify-tls", action="store_true")

    p.add_argument("--outdir", default="results")
    p.add_argument("--probe-exe",
                   default=os.path.join("ReshardProbe", "bin", "Release", "net8.0",
                                        "ReshardProbe.exe"))

    p.add_argument("--db-prefix", default="reshardtest")
    p.add_argument("--db-password", default=os.environ.get("RL_DB_PASSWORD"),
                   help="password set on the created DB and used by clients")
    p.add_argument("--memory-size", type=int, default=1_073_741_824, help="bytes")
    p.add_argument("--target-shards", type=int, default=2)
    p.add_argument("--tls", action="store_true", help="clients connect over TLS")
    p.add_argument("--keep-db", action="store_true",
                   help="do not delete the DB afterwards (debugging)")

    p.add_argument("--warmup", type=int, default=60)
    p.add_argument("--tail", type=int, default=120)
    p.add_argument("--control-hold", type=int, default=180)
    p.add_argument("--max-duration", type=int, default=1800,
                   help="harness safety cap; normally ended by the STOP marker")
    p.add_argument("--reshard-timeout", type=int, default=900)
    p.add_argument("--poll-interval", type=float, default=0.5)
    p.add_argument("--between-arms", type=int, default=30)
    p.add_argument("--create-timeout", type=int, default=300)
    p.add_argument("--reconcile-budget", type=int, default=120)

    p.add_argument("--load-connections", type=int, default=4)
    p.add_argument("--load-rate", type=float, default=200)
    p.add_argument("--corr-workers", type=int, default=4)
    p.add_argument("--sync-timeout", type=int, default=5000)
    p.add_argument("--connect-timeout", type=int, default=5000)
    p.add_argument("--config-check-sec", type=int, default=60)
    p.add_argument("--keepalive-sec", type=int, default=60)

    args = p.parse_args()

    missing = [f for f, v in (("--host", args.host), ("--user", args.user),
                              ("--password", args.password)) if not v]
    if missing:
        p.error(f"missing required: {', '.join(missing)}")
    if args.command == "arm" and not args.policy:
        p.error("--policy is required for 'arm'")

    os.makedirs(args.outdir, exist_ok=True)
    client = RestClient(args.host, args.user, args.password, args.port, args.verify_tls)

    return {
        "check": cmd_check,
        "dryrun": cmd_dryrun,
        "arm": cmd_arm,
        "matrix": cmd_matrix,
    }[args.command](client, args)


if __name__ == "__main__":
    sys.exit(main())

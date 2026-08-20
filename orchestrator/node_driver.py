#!/usr/bin/env python3
"""
Orchestrator for the proxy-policy resharding test, run ON a Redis Enterprise node.

Drives the real .NET client harness (StackExchange.Redis / NRedisStack) - this
script only handles cluster orchestration over REST and never touches the data
path, so the measured client behaviour is genuinely the .NET client's.

Per arm: create a fresh DB (1 master + 1 replica, dense placement, proxy_policy
set at create time), run the harness, reshard 1 -> N, record the server-side
topology timeline, then delete the DB. Shard count cannot be reduced on a normal
database, hence create/delete per arm.

Stdlib only. Analysis is deliberately left out: collect the results directory and
analyse it off-box.
"""

import argparse
import base64
import json
import os
import re
import shlex
import socket
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.error
import urllib.request

# rs_test_helpers/infra/database/common.py - hashing policy applied when sharding
# is enabled. Identical across arms, or slot remapping is conflated with the
# proxy_policy variable under test.
DEFAULT_REGEX_RULES = [{"regex": ".*\\{(?<tag>.*)\\}.*"}, {"regex": "(?<tag>.*)"}]

POLICIES = ["single", "all-master-shards", "all-nodes"]
CONTROL_ARM = "control"


def log(msg):
    sys.stdout.write("[%s] %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg))
    sys.stdout.flush()


class Rest(object):
    def __init__(self, host, port, user, password, timeout=30, verify_tls=False):
        self.base = "https://%s:%d" % (host, port)
        self.timeout = timeout
        self.auth = "Basic " + base64.b64encode(("%s:%s" % (user, password)).encode()).decode()
        self.ctx = ssl.create_default_context()
        if not verify_tls:
            # Redis Enterprise presents a self-signed certificate by default.
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE

    def _open_url(self, method, url, body=None):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": self.auth, "Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        return urllib.request.urlopen(req, timeout=self.timeout, context=self.ctx)

    def _open(self, method, path, body=None):
        return self._open_url(method, self.base + path, body)

    def get(self, path):
        try:
            with self._open("GET", path) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise SystemExit("REST auth failed (401): check --user/--password")
            if exc.code == 404:
                raise KeyError(path)
            raise SystemExit("REST GET %s -> HTTP %d: %s" % (path, exc.code, exc.read()[:300]))
        except urllib.error.URLError as exc:
            raise SystemExit("REST GET %s failed: %s" % (path, exc.reason))

    def mutate(self, method, path, body=None, max_redirects=3):
        """POST/PUT/DELETE, following redirects while preserving method and body.

        Redis Enterprise serves reads from any node but redirects mutations to the
        cluster master with a 307. urllib follows redirects for GET/HEAD only -
        HTTPRedirectHandler.redirect_request returns None for other methods - so
        without this the request fails on every non-master node, and a master
        change mid-run would break an in-flight matrix.
        """
        url = self.base + path
        cur_method, cur_body = method, body
        for hop in range(max_redirects + 1):
            try:
                with self._open_url(cur_method, url, cur_body) as r:
                    raw = r.read().decode("utf-8", "replace")
                    try:
                        return True, r.status, (json.loads(raw) if raw.strip() else {})
                    except ValueError:
                        return True, r.status, raw
            except urllib.error.HTTPError as exc:
                loc = exc.headers.get("Location") if exc.headers else None
                if exc.code in (301, 302, 303, 307, 308) and loc:
                    if hop >= max_redirects:
                        return False, exc.code, (
                            "too many redirects (>%d) for %s %s; last Location: %s"
                            % (max_redirects, method, path, loc))
                    target = urllib.parse.urljoin(url, loc)
                    parts = urllib.parse.urlsplit(target)
                    new_base = "%s://%s" % (parts.scheme, parts.netloc)
                    log("REST %s %s -> HTTP %d, following redirect to %s" % (
                        cur_method, url, exc.code, new_base))
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
        return False, 0, "redirect handling fell through for %s %s" % (method, path)


ROLE_ROW_RE = re.compile(
    r"^\*?node:(\d+)\s+(\S+)\s+(\d+\.\d+\.\d+\.\d+)", re.MULTILINE)


def find_api_master(rest):
    """Address of the node that accepts REST mutations, i.e. the cluster master.

    Redis Enterprise serves reads from any node but 307s mutations to the master.
    Resolving it up front makes the intent explicit in the logs and avoids relying
    on redirect handling. Redirect following stays as a backstop for a master
    change mid-run.

    Returns (address, how) or (None, reason).
    """
    # Preferred: the API itself, which also works when run off-box.
    try:
        for n in rest.get("/v1/nodes"):
            for key in ("role", "node_role", "cluster_role"):
                if str(n.get(key, "")).lower() == "master":
                    addr = n.get("addr") or n.get("external_addr")
                    if addr:
                        return addr, "REST /v1/nodes %s field" % key
    # Rest.get raises SystemExit, which is NOT an Exception subclass, so catching
    # only Exception here would abort the run instead of falling back to rladmin.
    except (Exception, SystemExit) as exc:
        log("master discovery via REST failed (%s); trying rladmin" % exc)

    # Fallback: rladmin, available because we run on a node. Use the ROLE column,
    # NOT the leading '*' - that marks the local node, not the master.
    ok, out = _run("rladmin status nodes", timeout=60)
    if ok:
        for uid, role, addr in ROLE_ROW_RE.findall(out):
            if role.lower() == "master":
                return addr, "rladmin status nodes (node%s)" % uid
        return None, "rladmin ran but no node had ROLE=master"

    return None, "neither REST nor rladmin could identify the master"


def node_map(rest):
    return dict((int(n["uid"]), n.get("addr") or n.get("external_addr") or "?")
                for n in rest.get("/v1/nodes"))


def local_addresses():
    addrs = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            addrs.add(info[4][0])
    except Exception:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # no packets sent; just resolves the route
        addrs.add(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    return addrs


def local_node_uid(rest):
    mine = local_addresses()
    for uid, addr in node_map(rest).items():
        if addr in mine:
            return uid
    return None


def db_topology(rest, uid):
    bdb = rest.get("/v1/bdbs/%d" % uid)
    shards = [s for s in rest.get("/v1/shards") if int(s.get("bdb_uid", -1)) == uid]
    ep_addrs, dns_name, port = [], None, None
    for ep in bdb.get("endpoints") or []:
        dns_name = ep.get("dns_name") or dns_name
        port = ep.get("port") or port
        for a in ep.get("addr") or []:
            ep_addrs.append(str(a))
    return {
        "status": bdb.get("status"),
        "shards_count": bdb.get("shards_count"),
        "proxy_policy": bdb.get("proxy_policy"),
        "shards_placement": bdb.get("shards_placement"),
        "replication": bdb.get("replication"),
        "oss_sharding": bdb.get("oss_sharding", False),
        "master_nodes": sorted(int(s["node_uid"]) for s in shards if s.get("role") == "master"),
        "replica_nodes": sorted(int(s["node_uid"]) for s in shards if s.get("role") == "slave"),
        "endpoint_addrs": sorted(set(ep_addrs)),
        "dns_name": dns_name,
        "port": port,
    }


def describe(topo, nodes):
    def nn(uids):
        return ", ".join("node%d(%s)" % (u, nodes.get(u, "?")) for u in uids) or "none"
    return ("status=%s shards=%s policy=%s placement=%s\n"
            "      masters : %s\n      replicas: %s\n      endpoint: %s:%s addrs=%s" % (
                topo["status"], topo["shards_count"], topo["proxy_policy"],
                topo["shards_placement"], nn(topo["master_nodes"]),
                nn(topo["replica_nodes"]), topo["dns_name"], topo["port"],
                topo["endpoint_addrs"]))


def create_db(rest, name, policy, memory_size, db_password):
    body = {"name": name, "type": "redis", "memory_size": memory_size,
            "shards_count": 1, "replication": True, "shards_placement": "dense",
            "proxy_policy": policy}
    if db_password:
        body["authentication_redis_pass"] = db_password
    ok, status, data = rest.mutate("POST", "/v1/bdbs", body)
    if not ok:
        raise SystemExit("DB create failed HTTP %s: %s" % (status, data))
    return int(data["uid"])


def wait_status(rest, uid, want="active", timeout=300):
    deadline = time.monotonic() + timeout
    last = "?"
    while time.monotonic() < deadline:
        last = str(rest.get("/v1/bdbs/%d" % uid).get("status"))
        if last == want:
            return
        if last.endswith("-failed"):
            raise SystemExit("bdb %d terminal status %s" % (uid, last))
        time.sleep(2)
    raise SystemExit("bdb %d status %s, wanted %s in %ds" % (uid, last, want, timeout))


def delete_db(rest, uid, timeout=300):
    ok, status, data = rest.mutate("DELETE", "/v1/bdbs/%d" % uid)
    if not ok:
        log("WARNING: delete bdb %d failed HTTP %s: %s" % (uid, status, data))
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            rest.get("/v1/bdbs/%d" % uid)
        except KeyError:
            log("deleted bdb %d" % uid)
            return
        time.sleep(2)
    log("WARNING: bdb %d still present after %ds" % (uid, timeout))


# --------------------------------------------------------------------------- #
# Node-local commands. Available because we run on a cluster node, and they
# expose things the REST API cannot.
# --------------------------------------------------------------------------- #

CLIENT_PROP_RE = re.compile(r"(\w+(?:-\w+)?)=(\S*)")


def _run(cmd, timeout=30):
    """Run a node command, retrying via sudo -i if it is not on PATH.
    Returns (ok, output)."""
    for attempt in (cmd, "sudo -i " + cmd):
        try:
            p = subprocess.Popen(attempt, shell=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            out, _ = p.communicate(timeout=timeout)
            text = out.decode("utf-8", "replace")
            if p.returncode == 0:
                return True, text
            last = text
        except subprocess.TimeoutExpired:
            p.kill()
            last = "timeout after %ss" % timeout
        except Exception as exc:
            last = str(exc)
    return False, last


def ccs_is_balanced(uid):
    """Flex-shard (ASM) scale-out completion signal. CCS-only: not exposed over
    REST. See rs_test_helpers/dmc/reshard_helper.py - 'enabled' means the
    scale-out has finished, 'disabled' means it is still rebalancing."""
    ok, out = _run("ccs-cli hget bdb:%d is_balanced" % uid)
    if not ok:
        return None
    val = out.strip().splitlines()[-1].strip() if out.strip() else ""
    return val if val in ("enabled", "disabled") else None


def capture_rladmin_status(path):
    ok, out = _run("rladmin status extra all", timeout=120)
    try:
        with open(path, "w") as fh:
            fh.write(out if ok else "rladmin status failed:\n" + out)
    except IOError:
        pass
    return ok


def proxy_conn_counts(uid, name_filter="reshardprobe"):
    """Our client connections per proxy, from every proxy serving the database.

    This is the clearest evidence for the proxy_policy question: under 'single'
    all our connections sit on one proxy address and must move wholesale when the
    endpoint re-binds; under all-master-shards / all-nodes they can be spread and
    partially survive. Keyed by laddr (the proxy-side local address).
    """
    ok, out = _run("bdb-cli %d --all-proxies client list" % uid, timeout=60)
    if not ok:
        return None
    counts = {}
    for line in out.splitlines():
        props = dict(CLIENT_PROP_RE.findall(line))
        if not props:
            continue
        if name_filter and name_filter not in props.get("name", ""):
            continue
        laddr = props.get("laddr", "?").split(":")[0]
        counts[laddr] = counts.get(laddr, 0) + 1
    return counts


def reshard_done(rest, uid, target, oss_sharding=False):
    """Completion detection.

    For a flex-shard/ASM database the shard count reaching the target does NOT
    mean the scale-out finished - is_balanced does. Using the wrong signal stops
    the measurement early and under-reports impact.
    """
    bdb = rest.get("/v1/bdbs/%d" % uid)
    if str(bdb.get("status")) != "active":
        return False
    shards = [s for s in rest.get("/v1/shards") if int(s.get("bdb_uid", -1)) == uid]
    masters = len([s for s in shards if s.get("role") == "master"])
    if masters < target:
        return False
    if oss_sharding:
        bal = ccs_is_balanced(uid)
        if bal is not None:
            return bal == "enabled"
        # Could not read CCS; fall back to shard count but the caller has warned.
    return True


def wait_for_event(path, kind, timeout, proc):
    """The harness flushes events.csv per line, so it can be tailed live."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with open(path) as fh:
                if any(kind in line for line in fh):
                    return True
        except IOError:
            pass
        time.sleep(0.25)
    return False


def run_arm(rest, args, policy):
    is_control = (policy == CONTROL_ARM)
    db_policy = "single" if is_control else policy
    tag = policy.replace("-", "_")
    outdir = os.path.join(args.outdir, tag)
    if not os.path.isdir(outdir):
        os.makedirs(outdir)
    markers = os.path.join(outdir, "markers.txt")
    open(markers, "w").close()
    topo_path = os.path.join(outdir, "topology.csv")

    nodes = node_map(rest)
    me = local_node_uid(rest)
    db_name = "%s-%s" % (args.db_prefix, tag)

    log("creating db %s (proxy_policy=%s)" % (db_name, db_policy))
    uid = create_db(rest, db_name, db_policy, args.memory_size, args.db_password)
    result = {"arm": policy, "bdb_uid": uid, "db_name": db_name, "local_node_uid": me}
    proc = None
    topo_fh = None
    pconn_fh = None

    try:
        wait_status(rest, uid, "active", args.create_timeout)
        pre = db_topology(rest, uid)
        log("pre-reshard topology:\n      %s" % describe(pre, nodes))
        result["pre"] = pre

        if len(pre["master_nodes"]) != 1:
            raise SystemExit("expected exactly 1 master, got %s" % pre["master_nodes"])
        if pre["oss_sharding"]:
            log("WARNING: oss_sharding=True (flex/ASM). Scale-out completion is "
                "is_balanced in CCS and not visible over REST, so completion may "
                "be detected early and impact under-reported.")

        # The load generator must not compete for CPU with the shard and proxy
        # being measured.
        if me is not None:
            if me in pre["master_nodes"] or nodes.get(me) in pre["endpoint_addrs"]:
                msg = "node%d hosts the test DB master and/or endpoint" % me
                if not args.allow_colocated:
                    raise SystemExit("REFUSING: %s. Run on a different node or pass "
                                     "--allow-colocated." % msg)
                log("WARNING: %s (proceeding due to --allow-colocated)" % msg)
            else:
                log("client node = node%d, not hosting the test DB - good" % me)
        else:
            log("WARNING: could not identify this node; skipping co-location check")

        host, port = pre["dns_name"], pre["port"]
        if not host or not port:
            raise SystemExit("could not determine endpoint for bdb %d" % uid)
        try:
            resolved = sorted(set(i[4][0] for i in socket.getaddrinfo(host, int(port))))
            log("endpoint %s:%s resolves to %s" % (host, port, resolved))
        except Exception as exc:
            raise SystemExit("endpoint %s does not resolve on this node: %s" % (host, exc))

        cmd = [args.dotnet, args.probe_dll,
               "--endpoint", "%s:%s" % (host, port),
               "--tag", tag, "--outdir", outdir, "--markers", markers,
               "--duration-sec", str(args.max_duration),
               "--warmup-sec", str(args.warmup),
               "--load-connections", str(args.load_connections),
               "--load-rate", str(args.load_rate),
               "--corr-workers", str(args.corr_workers),
               "--sync-timeout", str(args.sync_timeout),
               "--connect-timeout", str(args.connect_timeout),
               "--config-check-sec", str(args.config_check_sec),
               "--keepalive-sec", str(args.keepalive_sec),
               "--reconcile-budget-sec", str(args.reconcile_budget)]
        if args.db_password:
            cmd += ["--password", args.db_password]

        log("starting .NET harness: %s" % " ".join(shlex.quote(c) for c in cmd[:6]))
        stdout = open(os.path.join(outdir, "harness_stdout.log"), "w")
        env = dict(os.environ)
        env["DOTNET_ROOT"] = os.path.dirname(args.dotnet)
        proc = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT, env=env)

        events = os.path.join(outdir, "events.csv")
        if not wait_for_event(events, "warmup_complete", args.warmup + 180, proc):
            tail = ""
            try:
                with open(os.path.join(outdir, "harness_stdout.log")) as fh:
                    tail = fh.read()[-2000:]
            except IOError:
                pass
            raise SystemExit("harness never reported warmup_complete. Output:\n%s" % tail)
        log("warmup complete; baseline established")

        capture_rladmin_status(os.path.join(outdir, "rladmin_status_pre.txt"))

        topo_fh = open(topo_path, "w")
        topo_fh.write("utc,elapsed_s,status,shards_count,master_nodes,replica_nodes,endpoint_addrs\n")
        pconn_fh = open(os.path.join(outdir, "proxy_conns.csv"), "w")
        pconn_fh.write("utc,elapsed_s,proxy_addr,our_connections\n")
        t0 = time.monotonic()
        pconn_state = {"next": 0.0}

        def sample():
            now = time.monotonic() - t0
            try:
                t = db_topology(rest, uid)
            except Exception as exc:
                log("topology poll error: %s" % exc)
                t = None
            if t is not None:
                topo_fh.write('%s,%.3f,%s,%s,"%s","%s","%s"\n' % (
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    now, t["status"], t["shards_count"],
                    t["master_nodes"], t["replica_nodes"], t["endpoint_addrs"]))
                topo_fh.flush()

            # bdb-cli forks a process, so sample it on a slower cadence than the
            # REST poll to keep our own overhead off the measurement.
            if not args.no_proxy_conns and now >= pconn_state["next"]:
                pconn_state["next"] = now + args.proxy_conn_interval
                counts = proxy_conn_counts(uid)
                if counts is not None:
                    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    if not counts:
                        pconn_fh.write("%s,%.3f,none,0\n" % (stamp, now))
                    for addr, c in sorted(counts.items()):
                        pconn_fh.write("%s,%.3f,%s,%d\n" % (stamp, now, addr, c))
                    pconn_fh.flush()
            return t

        def mark(text):
            with open(markers, "a") as fh:
                fh.write(text + "\n")

        if is_control:
            log("CONTROL arm: no reshard, holding %ds" % args.control_hold)
            mark("control_hold_start")
            end = time.monotonic() + args.control_hold
            while time.monotonic() < end:
                sample()
                time.sleep(args.poll_interval)
            mark("control_hold_end")
        else:
            mark("reshard_trigger 1->%d" % args.target_shards)
            log("TRIGGERING reshard 1 -> %d" % args.target_shards)
            ok, status, data = rest.mutate("PUT", "/v1/bdbs/%d" % uid, {
                "sharding": True, "shards_count": args.target_shards,
                "shard_key_regex": DEFAULT_REGEX_RULES})
            result["reshard_http"] = status
            if not ok:
                mark("reshard_trigger_failed")
                raise SystemExit("reshard trigger failed HTTP %s: %s" % (status, data))

            prev, done_at = None, None
            deadline = time.monotonic() + args.reshard_timeout
            while time.monotonic() < deadline:
                t = sample()
                if t is not None:
                    sig = (t["status"], t["shards_count"], tuple(t["master_nodes"]),
                           tuple(t["endpoint_addrs"]))
                    if sig != prev:
                        mark("topology status=%s shards=%s masters=%s endpoint=%s" % (
                            t["status"], t["shards_count"], t["master_nodes"],
                            t["endpoint_addrs"]))
                        prev = sig
                try:
                    if reshard_done(rest, uid, args.target_shards,
                                    oss_sharding=pre["oss_sharding"]):
                        done_at = time.monotonic() - t0
                        mark("reshard_complete elapsed_s=%.3f" % done_at)
                        log("reshard complete after %.1fs" % done_at)
                        break
                except Exception as exc:
                    log("completion check error: %s" % exc)
                time.sleep(args.poll_interval)

            if done_at is None:
                mark("reshard_timeout")
                log("WARNING: reshard did not complete within %ds" % args.reshard_timeout)
            result["reshard_elapsed_s"] = done_at

            log("holding %ds for the recovery tail" % args.tail)
            end = time.monotonic() + args.tail
            while time.monotonic() < end:
                sample()
                time.sleep(args.poll_interval)

            post = db_topology(rest, uid)
            log("post-reshard topology:\n      %s" % describe(post, nodes))
            result["post"] = post
            capture_rladmin_status(os.path.join(outdir, "rladmin_status_post.txt"))

        # Clean stop so the harness drains and reconciles; killing it would lose
        # the phantom/lost write counts, which are the point of the exercise.
        log("signalling harness to stop")
        mark("STOP")
        try:
            proc.wait(timeout=args.reconcile_budget + 180)
        except subprocess.TimeoutExpired:
            log("WARNING: harness did not exit in time; terminating")
            proc.terminate()
        proc = None

        rec = os.path.join(outdir, "reconcile.json")
        if os.path.exists(rec):
            with open(rec) as fh:
                result["reconcile"] = json.load(fh).get("totals")
            log("reconcile totals: %s" % json.dumps(result["reconcile"]))
        else:
            log("WARNING: no reconcile.json produced")

    finally:
        for fh in (topo_fh, pconn_fh):
            if fh is not None:
                try:
                    fh.close()
                except Exception:
                    pass
        if proc is not None and proc.poll() is None:
            log("cleaning up harness process")
            proc.terminate()
        if not args.keep_db:
            delete_db(rest, uid)
        else:
            log("--keep-db set; leaving bdb %d" % uid)

    with open(os.path.join(outdir, "arm_result.json"), "w") as fh:
        json.dump(result, fh, indent=2)
    return result


def cmd_check(rest, args):
    log("cluster: %s" % rest.get("/v1/cluster").get("name"))
    me = local_node_uid(rest)
    log("this host looks like node %s (local addrs: %s)" % (
        me, ", ".join(sorted(local_addresses()))))
    for n in sorted(rest.get("/v1/nodes"), key=lambda x: int(x["uid"])):
        log("  node%-3s addr=%-15s status=%-10s shards=%-4s cores=%-3s" % (
            n["uid"], n.get("addr"), n.get("status"), n.get("shard_count"), n.get("cores")))
    bdbs = rest.get("/v1/bdbs")
    log("databases: %d" % len(bdbs))
    for b in bdbs:
        log("  bdb%-4s name=%-24s shards=%-4s policy=%-18s status=%s" % (
            b["uid"], b.get("name"), b.get("shards_count"), b.get("proxy_policy"),
            b.get("status")))
    return 0


def cmd_dryrun(rest, args):
    nodes = node_map(rest)
    me = local_node_uid(rest)
    uid = create_db(rest, "%s-dryrun" % args.db_prefix, "single",
                    args.memory_size, args.db_password)
    try:
        wait_status(rest, uid, "active", args.create_timeout)
        topo = db_topology(rest, uid)
        log("topology:\n      %s" % describe(topo, nodes))
        if topo["oss_sharding"]:
            log("NOTE: oss_sharding=True (flex/ASM); REST cannot observe is_balanced")
        if me is not None and (me in topo["master_nodes"] or
                               nodes.get(me) in topo["endpoint_addrs"]):
            log("WARNING: this node hosts the DB master/endpoint. Use another node "
                "for the real run, or pass --allow-colocated.")
        else:
            log("this node is not hosting the DB - good for the real run")
        try:
            resolved = sorted(set(i[4][0] for i in socket.getaddrinfo(
                topo["dns_name"], int(topo["port"]))))
            log("endpoint %s:%s resolves to %s" % (topo["dns_name"], topo["port"], resolved))
        except Exception as exc:
            log("WARNING: endpoint does not resolve here: %s" % exc)
    finally:
        if not args.keep_db:
            delete_db(rest, uid)
    return 0


def cmd_arm(rest, args):
    run_arm(rest, args, args.policy)
    return 0


def cmd_matrix(rest, args):
    arms = [CONTROL_ARM] + POLICIES
    results = []
    for i, arm in enumerate(arms):
        log("=" * 70)
        log("ARM %d/%d: %s" % (i + 1, len(arms), arm))
        log("=" * 70)
        try:
            results.append(run_arm(rest, args, arm))
        except SystemExit as exc:
            log("ARM %s FAILED: %s" % (arm, exc))
            results.append({"arm": arm, "error": str(exc)})
        except Exception as exc:
            log("ARM %s ERROR: %s: %s" % (arm, type(exc).__name__, exc))
            results.append({"arm": arm, "error": "%s: %s" % (type(exc).__name__, exc)})
        if i < len(arms) - 1:
            log("settling %ds before next arm" % args.between_arms)
            time.sleep(args.between_arms)
    with open(os.path.join(args.outdir, "matrix_results.json"), "w") as fh:
        json.dump(results, fh, indent=2)
    log("matrix complete. Collect with: tar czf results.tgz %s" % args.outdir)
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["check", "dryrun", "arm", "matrix"])
    p.add_argument("--policy", choices=POLICIES + [CONTROL_ARM])

    p.add_argument("--rest-host", default="localhost")
    p.add_argument("--rest-port", type=int, default=9443)
    p.add_argument("--no-master-discovery", action="store_true",
                   help="do not look up the cluster master first; rely only on "
                        "following 307 redirects")
    p.add_argument("--user", default=os.environ.get("RL_REST_USER"))
    p.add_argument("--password", default=os.environ.get("RL_REST_PASSWORD"))

    p.add_argument("--outdir", default="results")
    p.add_argument("--dotnet", default=os.path.expanduser("~/.dotnet/dotnet"))
    p.add_argument("--probe-dll", required=True)

    p.add_argument("--db-prefix", default="reshardtest")
    p.add_argument("--db-password", default=os.environ.get("RL_DB_PASSWORD", ""))
    p.add_argument("--memory-size", type=int, default=1073741824)
    p.add_argument("--target-shards", type=int, default=2)
    p.add_argument("--keep-db", action="store_true")
    p.add_argument("--allow-colocated", action="store_true")

    p.add_argument("--warmup", type=int, default=60)
    p.add_argument("--tail", type=int, default=120)
    p.add_argument("--control-hold", type=int, default=180)
    p.add_argument("--max-duration", type=int, default=1800)
    p.add_argument("--reshard-timeout", type=int, default=900)
    p.add_argument("--create-timeout", type=int, default=300)
    p.add_argument("--reconcile-budget", type=int, default=120)
    p.add_argument("--between-arms", type=int, default=30)
    p.add_argument("--poll-interval", type=float, default=0.5)
    p.add_argument("--proxy-conn-interval", type=float, default=2.0,
                   help="seconds between 'bdb-cli --all-proxies client list' samples")
    p.add_argument("--no-proxy-conns", action="store_true",
                   help="skip per-proxy connection sampling (loses the clearest "
                        "evidence of endpoint/proxy migration)")

    p.add_argument("--load-connections", type=int, default=4)
    p.add_argument("--load-rate", type=float, default=200)
    p.add_argument("--corr-workers", type=int, default=4)
    p.add_argument("--sync-timeout", type=int, default=5000)
    p.add_argument("--connect-timeout", type=int, default=5000)
    p.add_argument("--config-check-sec", type=int, default=60)
    p.add_argument("--keepalive-sec", type=int, default=60)

    args = p.parse_args()
    for name, val in (("--user", args.user), ("--password", args.password)):
        if not val:
            p.error("missing required: %s" % name)
    if args.command == "arm" and not args.policy:
        p.error("--policy is required for 'arm'")
    if not os.path.isdir(args.outdir):
        os.makedirs(args.outdir)

    rest = Rest(args.rest_host, args.rest_port, args.user, args.password)

    # Point mutations at the cluster master up front. Harmless for read-only
    # commands, and it makes the target explicit rather than implicit in a redirect.
    if not args.no_master_discovery:
        addr, how = find_api_master(rest)
        if addr:
            new_base = "https://%s:%d" % (addr, args.rest_port)
            if new_base != rest.base:
                log("API master is %s (via %s); directing REST there (was %s)" % (
                    addr, how, rest.base))
                rest.base = new_base
            else:
                log("API master is %s (via %s); already targeting it" % (addr, how))
        else:
            log("could not determine the API master (%s); relying on 307 "
                "redirect following" % how)

    return {"check": cmd_check, "dryrun": cmd_dryrun, "arm": cmd_arm,
            "matrix": cmd_matrix}[args.command](rest, args)


if __name__ == "__main__":
    sys.exit(main())

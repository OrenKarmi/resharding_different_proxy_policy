#!/usr/bin/env python3
"""
reshard_test.py - measure client impact of resharding under different proxy policies.

Single file, Python 3 standard library only. Designed to be pasted onto a Redis
Enterprise node and run there.

WHAT IT DOES
  For each arm it creates a fresh database (1 master + 1 replica, dense shard
  placement, proxy_policy set at create time), drives load against the database
  endpoint, reshards 1 -> N shards, records exactly what the client experiences,
  then deletes the database. Shard count cannot be reduced on a normal database,
  hence create/delete per arm.

  Arms: control (no reshard), single, all-master-shards, all-nodes.

WHAT IT MEASURES
  - outage windows, from a fixed-cadence PING probe
  - every operation classified as ok / fail_definite (never sent) /
    fail_ambiguous (may have been applied) / fail_server
  - phantom writes: INCRs the server applied but never acknowledged. These are the
    dangerous ones, because a naive application retry double-applies them.
  - lost writes: acknowledged INCRs missing from the server. Must always be 0.
  - throughput hole and latency percentiles before / during / after
  - reconnect count and time, plus the server-side topology timeline so the
    slot-migration phase can be separated from the endpoint re-bind

IMPORTANT SCOPE LIMIT
  This uses its own RESP client, NOT StackExchange.Redis / NRedisStack. It
  measures what the database and proxy do to a connected client. It does NOT
  reproduce a specific .NET client's reconnect policy, multiplexer backlog, or
  topology-refresh timing. Treat the numbers as the server-side ground truth.

DEPENDENCIES
  python3 (3.6+). Nothing else. No pip installs, no internet access needed.

USAGE (on a cluster node)
  # 1. sanity check: REST reachable, node inventory, where this node sits
  python3 reshard_test.py check --user "$RL_USER" --password "$RL_PASS"

  # 2. dry run: create a DB, show shard/endpoint placement, delete it. No load.
  python3 reshard_test.py dryrun --user "$RL_USER" --password "$RL_PASS"

  # 3. full matrix (control + 3 policies). ~10 min per arm.
  python3 reshard_test.py matrix --user "$RL_USER" --password "$RL_PASS" \
      --db-password 'testpass' --outdir ./results

  # or a single arm
  python3 reshard_test.py arm --policy single --user "$RL_USER" \
      --password "$RL_PASS" --db-password 'testpass'

  # 4. re-print the report from an existing results dir
  python3 reshard_test.py analyze --outdir ./results

  Then: tar czf results.tgz results/   and copy that back.

RUN THIS ON A NODE THAT DOES NOT HOST THE TEST DATABASE. The script detects
which node it is on and warns if it is co-located with the master shards or the
endpoint, because the load generator would then compete for CPU with the very
shard and proxy being measured. Override with --allow-colocated.
"""

import argparse
import base64
import json
import os
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request

# Matches rs_test_helpers/infra/database/common.py - the hashing policy applied
# when sharding is enabled. Must be identical across arms, or slot remapping gets
# conflated with the proxy_policy variable under test.
DEFAULT_REGEX_RULES = [{"regex": ".*\\{(?<tag>.*)\\}.*"}, {"regex": "(?<tag>.*)"}]

POLICIES = ["single", "all-master-shards", "all-nodes"]
CONTROL_ARM = "control"

_T0 = [time.monotonic()]
_T0_WALL = [time.time()]


def anchor_clock():
    _T0[0] = time.monotonic()
    _T0_WALL[0] = time.time()


def ms():
    """Milliseconds since the clock was anchored."""
    return (time.monotonic() - _T0[0]) * 1000.0


def utc_of(msec):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(_T0_WALL[0] + msec / 1000.0))


def now_utc():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log(msg):
    sys.stdout.write("[%s] %s\n" % (now_utc(), msg))
    sys.stdout.flush()


# --------------------------------------------------------------------------- #
# Recording
# --------------------------------------------------------------------------- #

def csv_escape(s):
    if s is None:
        return ""
    s = str(s).replace("\r", " ").replace("\n", " ")
    if "," in s or '"' in s:
        return '"' + s.replace('"', '""') + '"'
    return s


class Recorder(object):
    """Thread-safe CSV sinks. events.csv is flushed per line so progress is
    visible while a long arm is running."""

    def __init__(self, outdir):
        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        self.lock = threading.Lock()
        self.ops = open(os.path.join(outdir, "ops.csv"), "w")
        self.ops.write("t_issue_ms,t_done_ms,latency_ms,role,worker,op,outcome,error\n")
        self.probe = open(os.path.join(outdir, "probe.csv"), "w")
        self.probe.write("t_issue_ms,t_done_ms,latency_ms,outcome,error\n")
        self.events = open(os.path.join(outdir, "events.csv"), "w")
        self.events.write("t_ms,utc,source,kind,detail\n")
        self.topo = open(os.path.join(outdir, "topology.csv"), "w")
        self.topo.write("t_ms,utc,status,shards_count,master_nodes,replica_nodes,endpoint_addrs\n")

    def op(self, t0, t1, role, worker, opname, outcome, err=None):
        line = "%.3f,%.3f,%.3f,%s,%d,%s,%s,%s\n" % (
            t0, t1, t1 - t0, role, worker, opname, outcome, csv_escape(err))
        with self.lock:
            self.ops.write(line)

    def ping(self, t0, t1, outcome, err=None):
        line = "%.3f,%.3f,%.3f,%s,%s\n" % (t0, t1, t1 - t0, outcome, csv_escape(err))
        with self.lock:
            self.probe.write(line)

    def event(self, source, kind, detail=None):
        t = ms()
        line = "%.3f,%s,%s,%s,%s\n" % (t, utc_of(t), source, kind, csv_escape(detail))
        with self.lock:
            self.events.write(line)
            self.events.flush()
        log("  [%8.3fs] %-10s %-26s %s" % (t / 1000.0, source, kind, detail or ""))

    def topology(self, t, topo):
        line = "%.3f,%s,%s,%s,%s,%s,%s\n" % (
            t, utc_of(t), topo.get("status"), topo.get("shards_count"),
            csv_escape(topo.get("master_nodes")), csv_escape(topo.get("replica_nodes")),
            csv_escape(topo.get("endpoint_addrs")))
        with self.lock:
            self.topo.write(line)
            self.topo.flush()

    def close(self):
        with self.lock:
            for f in (self.ops, self.probe, self.events, self.topo):
                try:
                    f.flush()
                    f.close()
                except Exception:
                    pass


# --------------------------------------------------------------------------- #
# Minimal RESP client
# --------------------------------------------------------------------------- #

OK = "ok"
FAIL_DEFINITE = "fail_definite"     # provably never sent; safe to retry
FAIL_AMBIGUOUS = "fail_ambiguous"   # may have been applied; NOT safe to retry
FAIL_SERVER = "fail_server"         # server replied with an error


class RespError(Exception):
    """Server returned an error reply. The command's fate is known."""


class Disconnected(Exception):
    """Connection is unusable. sent=True means the command may have been applied."""

    def __init__(self, msg, sent):
        Exception.__init__(self, msg)
        self.sent = sent


class Resp(object):
    def __init__(self, host, port, password=None, op_timeout=5.0,
                 connect_timeout=5.0, name=None):
        self.host = host
        self.port = port
        self.password = password
        self.op_timeout = op_timeout
        self.connect_timeout = connect_timeout
        self.name = name
        self.sock = None
        self.buf = b""
        self.connects = 0
        self.connect_ms_total = 0.0

    # -- connection ------------------------------------------------------- #

    def connect(self):
        self.close()
        t0 = ms()
        # Resolve every time: after an endpoint migration the FQDN points at a
        # different node, and that re-resolution delay is part of the outage.
        s = socket.create_connection((self.host, self.port), self.connect_timeout)
        s.settimeout(self.op_timeout)
        try:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass
        self.sock = s
        self.buf = b""
        try:
            if self.password:
                self.call("AUTH", self.password)
            if self.name:
                self.call("CLIENT", "SETNAME", self.name)
        except Exception:
            self.close()
            raise
        self.connects += 1
        self.connect_ms_total += ms() - t0

    def close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
        self.buf = b""

    @property
    def connected(self):
        return self.sock is not None

    # -- protocol --------------------------------------------------------- #

    def call(self, *args):
        """Send a command and read its reply. Raises RespError or Disconnected."""
        if self.sock is None:
            raise Disconnected("not connected", sent=False)
        out = [b"*" + str(len(args)).encode()]
        for a in args:
            if not isinstance(a, bytes):
                a = str(a).encode()
            out.append(b"$" + str(len(a)).encode())
            out.append(a)
        payload = b"\r\n".join(out) + b"\r\n"
        try:
            self.sock.sendall(payload)
        except Exception as exc:
            self.close()
            # Nothing (or only part) went out and we got an error on write; the
            # server may still have received a prefix, so treat as ambiguous only
            # if it was not an outright connection refusal.
            raise Disconnected("send failed: %s" % exc, sent=True)
        return self._read_reply()

    def _fill(self):
        try:
            chunk = self.sock.recv(65536)
        except socket.timeout:
            self.close()
            raise Disconnected("read timeout", sent=True)
        except Exception as exc:
            self.close()
            raise Disconnected("read error: %s" % exc, sent=True)
        if not chunk:
            self.close()
            raise Disconnected("peer closed", sent=True)
        self.buf += chunk

    def _read_line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n):
        while len(self.buf) < n + 2:
            self._fill()
        data = self.buf[:n]
        self.buf = self.buf[n + 2:]
        return data

    def _read_reply(self):
        line = self._read_line()
        if not line:
            raise Disconnected("empty reply", sent=True)
        tag, rest = line[:1], line[1:]
        if tag == b"+":
            return rest.decode("utf-8", "replace")
        if tag == b"-":
            raise RespError(rest.decode("utf-8", "replace"))
        if tag == b":":
            return int(rest)
        if tag == b"$":
            n = int(rest)
            if n == -1:
                return None
            return self._read_exact(n)
        if tag == b"*":
            n = int(rest)
            if n == -1:
                return None
            return [self._read_reply() for _ in range(n)]
        raise Disconnected("bad protocol byte %r" % tag, sent=True)


def classify(exc):
    """What can the application actually conclude about this operation?"""
    if isinstance(exc, RespError):
        return FAIL_SERVER
    if isinstance(exc, Disconnected):
        return FAIL_AMBIGUOUS if exc.sent else FAIL_DEFINITE
    if isinstance(exc, (socket.timeout,)):
        return FAIL_AMBIGUOUS
    if isinstance(exc, (ConnectionRefusedError, socket.gaierror)):
        # Never reached the server: DNS failed or the port refused outright.
        return FAIL_DEFINITE
    if isinstance(exc, OSError):
        return FAIL_AMBIGUOUS
    return FAIL_AMBIGUOUS


# --------------------------------------------------------------------------- #
# Workers
# --------------------------------------------------------------------------- #

class Worker(threading.Thread):
    """Common connection management: an operation failure drops the connection and
    the worker reconnects, recording how long that took."""

    def __init__(self, rec, stop_evt, host, port, password, role, idx,
                 op_timeout, connect_timeout):
        threading.Thread.__init__(self)
        self.daemon = True
        self.rec = rec
        self.stop_evt = stop_evt
        self.role = role
        self.idx = idx
        self.conn = Resp(host, port, password, op_timeout, connect_timeout,
                         name="reshardprobe-%s-%d" % (role, idx))
        self.reconnect_attempts = 0
        self.reconnect_failures = 0

    def ensure(self):
        """Return True if connected. Never raises."""
        if self.conn.connected:
            return True
        self.reconnect_attempts += 1
        try:
            self.conn.connect()
            if self.reconnect_attempts > 1:
                self.rec.event(self.role + str(self.idx), "reconnected",
                               "attempt=%d" % self.reconnect_attempts)
            return True
        except Exception as exc:
            self.reconnect_failures += 1
            return False


class ProbeWorker(Worker):
    """Fixed-cadence PING. Defines the outage window: the gap between consecutive
    successful pings. Kept on its own connection so load stalls cannot distort it."""

    def __init__(self, rec, stop_evt, host, port, password, interval_ms,
                 op_timeout, connect_timeout):
        Worker.__init__(self, rec, stop_evt, host, port, password, "probe", 0,
                        op_timeout, connect_timeout)
        self.interval = interval_ms / 1000.0

    def run(self):
        nxt = time.monotonic()
        while not self.stop_evt.is_set():
            nxt += self.interval
            t0 = ms()
            if not self.ensure():
                self.rec.ping(t0, ms(), FAIL_DEFINITE, "connect failed")
            else:
                try:
                    self.conn.call("PING")
                    self.rec.ping(t0, ms(), OK)
                except Exception as exc:
                    self.rec.ping(t0, ms(), classify(exc), str(exc))
            sleep_for = nxt - time.monotonic()
            if sleep_for > 0:
                self.stop_evt.wait(sleep_for)
            else:
                # Behind schedule (an op took longer than the interval); resync
                # rather than firing a burst of catch-up pings.
                nxt = time.monotonic()


class LoadWorker(Worker):
    """Rate-limited SET/GET. Provides the throughput and latency signal."""

    def __init__(self, rec, stop_evt, host, port, password, idx, rate,
                 prefix, keyspace, value_bytes, op_timeout, connect_timeout):
        Worker.__init__(self, rec, stop_evt, host, port, password, "load", idx,
                        op_timeout, connect_timeout)
        self.interval = 1.0 / rate if rate > 0 else 0.01
        self.prefix = prefix
        self.keyspace = keyspace
        self.value = b"x" * value_bytes
        self.seq = 0

    def run(self):
        nxt = time.monotonic()
        while not self.stop_evt.is_set():
            nxt += self.interval
            self.seq += 1
            key = "%s:k:%d" % (self.prefix, self.seq % self.keyspace)
            is_write = (self.seq % 2) == 0
            opname = "SET" if is_write else "GET"
            t0 = ms()
            if not self.ensure():
                self.rec.op(t0, ms(), "load", self.idx, opname, FAIL_DEFINITE,
                            "connect failed")
            else:
                try:
                    if is_write:
                        self.conn.call("SET", key, self.value)
                    else:
                        self.conn.call("GET", key)
                    self.rec.op(t0, ms(), "load", self.idx, opname, OK)
                except Exception as exc:
                    self.rec.op(t0, ms(), "load", self.idx, opname,
                                classify(exc), str(exc))
            sleep_for = nxt - time.monotonic()
            if sleep_for > 0:
                self.stop_evt.wait(sleep_for)
            else:
                nxt = time.monotonic()


class CorrWorker(Worker):
    """Sequential INCR on a dedicated key. One operation in flight at a time so
    that attempted vs acknowledged is unambiguous and the server-side counter can
    be compared exactly against what the client believes."""

    def __init__(self, rec, stop_evt, host, port, password, idx, interval_ms,
                 key, op_timeout, connect_timeout):
        Worker.__init__(self, rec, stop_evt, host, port, password, "correctness",
                        idx, op_timeout, connect_timeout)
        self.interval = interval_ms / 1000.0
        self.key = key
        self.attempted = 0
        self.acked = 0
        self.n_definite = 0
        self.n_ambiguous = 0
        self.n_server = 0
        self.max_acked = 0

    def run(self):
        nxt = time.monotonic()
        while not self.stop_evt.is_set():
            nxt += self.interval
            t0 = ms()
            if not self.ensure():
                # Could not even open a socket: the INCR was definitely not sent.
                self.attempted += 1
                self.n_definite += 1
                self.rec.op(t0, ms(), "correctness", self.idx, "INCR",
                            FAIL_DEFINITE, "connect failed")
            else:
                self.attempted += 1
                try:
                    v = self.conn.call("INCR", self.key)
                    self.acked += 1
                    if isinstance(v, int) and v > self.max_acked:
                        self.max_acked = v
                    self.rec.op(t0, ms(), "correctness", self.idx, "INCR", OK)
                except Exception as exc:
                    outcome = classify(exc)
                    if outcome == FAIL_DEFINITE:
                        self.n_definite += 1
                    elif outcome == FAIL_SERVER:
                        self.n_server += 1
                    else:
                        self.n_ambiguous += 1
                    self.rec.op(t0, ms(), "correctness", self.idx, "INCR",
                                outcome, str(exc))
            sleep_for = nxt - time.monotonic()
            if sleep_for > 0:
                self.stop_evt.wait(sleep_for)
            else:
                nxt = time.monotonic()


# --------------------------------------------------------------------------- #
# REST
# --------------------------------------------------------------------------- #

class Rest(object):
    def __init__(self, host, port, user, password, timeout=30, verify_tls=False):
        self.base = "https://%s:%d" % (host, port)
        self.timeout = timeout
        self.auth = "Basic " + base64.b64encode(
            ("%s:%s" % (user, password)).encode()).decode()
        self.ctx = ssl.create_default_context()
        if not verify_tls:
            # Redis Enterprise presents a self-signed certificate by default.
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE

    def _open(self, method, path, body=None, timeout=None):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": self.auth, "Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers=headers)
        return urllib.request.urlopen(
            req, timeout=self.timeout if timeout is None else timeout, context=self.ctx)

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
            raise SystemExit("REST GET %s failed: %s (check --rest-host)" % (path, exc.reason))

    def mutate(self, method, path, body=None):
        try:
            with self._open(method, path, body) as r:
                raw = r.read().decode("utf-8", "replace")
                try:
                    return True, r.status, (json.loads(raw) if raw.strip() else {})
                except ValueError:
                    return True, r.status, raw
        except urllib.error.HTTPError as exc:
            return False, exc.code, exc.read().decode("utf-8", "replace")[:500]
        except urllib.error.URLError as exc:
            return False, 0, str(exc.reason)
        except OSError as exc:
            return False, 0, str(exc)


def node_map(rest):
    out = {}
    for n in rest.get("/v1/nodes"):
        out[int(n["uid"])] = n.get("addr") or n.get("external_addr") or "?"
    return out


def local_addresses():
    """Best-effort set of this host's IPs, to work out which node we are on."""
    addrs = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            addrs.add(info[4][0])
    except Exception:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # no packets sent; just picks the route
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
    masters = sorted(int(s["node_uid"]) for s in shards if s.get("role") == "master")
    replicas = sorted(int(s["node_uid"]) for s in shards if s.get("role") == "slave")
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
        "master_nodes": masters,
        "replica_nodes": replicas,
        "endpoint_addrs": sorted(set(ep_addrs)),
        "dns_name": dns_name,
        "port": port,
    }


def describe(topo, nodes):
    def nn(uids):
        return ", ".join("node%d(%s)" % (u, nodes.get(u, "?")) for u in uids) or "none"
    return ("status=%s shards=%s policy=%s placement=%s\n"
            "      masters : %s\n"
            "      replicas: %s\n"
            "      endpoint: %s:%s addrs=%s" % (
                topo["status"], topo["shards_count"], topo["proxy_policy"],
                topo["shards_placement"], nn(topo["master_nodes"]),
                nn(topo["replica_nodes"]), topo["dns_name"], topo["port"],
                topo["endpoint_addrs"]))


def create_db(rest, name, policy, memory_size, db_password, port=None):
    body = {
        "name": name,
        "type": "redis",
        "memory_size": memory_size,
        "shards_count": 1,
        "replication": True,
        "shards_placement": "dense",
        "proxy_policy": policy,
    }
    if db_password:
        body["authentication_redis_pass"] = db_password
    if port:
        body["port"] = port
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
            raise SystemExit("bdb %d entered terminal status %s" % (uid, last))
        time.sleep(2)
    raise SystemExit("bdb %d status %s, wanted %s within %ds" % (uid, last, want, timeout))


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


def trigger_reshard(rest, uid, target):
    body = {"sharding": True, "shards_count": target,
            "shard_key_regex": DEFAULT_REGEX_RULES}
    return rest.mutate("PUT", "/v1/bdbs/%d" % uid, body)


def reshard_done(rest, uid, target):
    bdb = rest.get("/v1/bdbs/%d" % uid)
    if str(bdb.get("status")) != "active":
        return False
    shards = [s for s in rest.get("/v1/shards") if int(s.get("bdb_uid", -1)) == uid]
    return len([s for s in shards if s.get("role") == "master"]) >= target


# --------------------------------------------------------------------------- #
# Analysis
# --------------------------------------------------------------------------- #

def pct(sorted_vals, p):
    if not sorted_vals:
        return None
    k = int(round((p / 100.0) * (len(sorted_vals) - 1)))
    return sorted_vals[max(0, min(k, len(sorted_vals) - 1))]


def read_csv(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path) as fh:
        header = fh.readline().strip().split(",")
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            # Only the trailing error field can contain commas; split with a cap.
            parts = line.split(",", len(header) - 1)
            rows.append(dict(zip(header, parts)))
    return rows


def outage_windows(probe_rows, min_gap_ms):
    oks = sorted(float(r["t_done_ms"]) for r in probe_rows if r["outcome"] == OK)
    gaps = []
    for i in range(1, len(oks)):
        gap = oks[i] - oks[i - 1]
        if gap >= min_gap_ms:
            gaps.append({"start_ms": oks[i - 1], "end_ms": oks[i], "duration_ms": gap})
    return gaps, oks


def analyze_arm(outdir, min_gap_ms=250):
    probe = read_csv(os.path.join(outdir, "probe.csv"))
    ops = read_csv(os.path.join(outdir, "ops.csv"))
    events = read_csv(os.path.join(outdir, "events.csv"))

    gaps, oks = outage_windows(probe, min_gap_ms)

    counts = {}
    for r in ops:
        key = (r["role"], r["op"], r["outcome"])
        counts[key] = counts.get(key, 0) + 1
    probe_counts = {}
    for r in probe:
        probe_counts[r["outcome"]] = probe_counts.get(r["outcome"], 0) + 1

    trigger = None
    complete = None
    for e in events:
        if e["kind"] == "reshard_trigger" and trigger is None:
            trigger = float(e["t_ms"])
        if e["kind"] == "reshard_complete" and complete is None:
            complete = float(e["t_ms"])

    def lat_stats(rows):
        vals = sorted(float(r["latency_ms"]) for r in rows if r["outcome"] == OK)
        return {
            "n": len(vals),
            "p50": pct(vals, 50), "p99": pct(vals, 99),
            "p999": pct(vals, 99.9), "max": vals[-1] if vals else None,
        }

    load = [r for r in ops if r["role"] == "load"]
    if trigger is not None:
        pre = [r for r in load if float(r["t_issue_ms"]) < trigger]
        during_end = complete if complete is not None else float("inf")
        during = [r for r in load if trigger <= float(r["t_issue_ms"]) < during_end]
        post = [r for r in load if float(r["t_issue_ms"]) >= during_end]
    else:
        pre, during, post = load, [], []

    # Throughput hole: compare per-second successful op rate during the event to
    # the pre-trigger baseline.
    def rate(rows, span_ms):
        n = len([r for r in rows if r["outcome"] == OK])
        return (n / (span_ms / 1000.0)) if span_ms > 0 else None

    baseline_rate = rate(pre, trigger) if trigger else None
    lost_ops = None
    if trigger is not None and complete is not None and baseline_rate:
        span = complete - trigger
        expected = baseline_rate * (span / 1000.0)
        actual = len([r for r in during if r["outcome"] == OK])
        lost_ops = max(0, int(round(expected - actual)))

    rec_path = os.path.join(outdir, "reconcile.json")
    reconcile = None
    if os.path.exists(rec_path):
        with open(rec_path) as fh:
            reconcile = json.load(fh).get("totals")

    return {
        "arm": os.path.basename(outdir.rstrip("/\\")),
        "reshard_trigger_ms": trigger,
        "reshard_complete_ms": complete,
        "reshard_duration_ms": (complete - trigger) if (trigger and complete) else None,
        "outages": {
            "count": len(gaps),
            "longest_ms": max([g["duration_ms"] for g in gaps]) if gaps else 0.0,
            "total_ms": sum(g["duration_ms"] for g in gaps),
            "windows": gaps[:20],
        },
        "probe_outcomes": probe_counts,
        "op_outcomes": dict(("%s/%s/%s" % k, v) for k, v in sorted(counts.items())),
        "latency_ms": {"pre": lat_stats(pre), "during": lat_stats(during),
                       "post": lat_stats(post)},
        "baseline_ops_per_sec": baseline_rate,
        "estimated_lost_ops": lost_ops,
        "reconcile": reconcile,
    }


def print_report(summaries):
    w = sys.stdout.write
    w("\n" + "=" * 78 + "\n")
    w("RESHARD CLIENT-IMPACT SUMMARY\n")
    w("=" * 78 + "\n\n")
    w("%-18s %10s %10s %9s %9s %9s\n" % (
        "arm", "outage_ms", "reshard_s", "ambig", "phantom", "lost"))
    w("-" * 78 + "\n")
    for s in summaries:
        rc = s.get("reconcile") or {}
        amb = 0
        for k, v in (s.get("op_outcomes") or {}).items():
            if k.endswith(FAIL_AMBIGUOUS):
                amb += v
        rs = s.get("reshard_duration_ms")
        w("%-18s %10.0f %10s %9d %9s %9s\n" % (
            s["arm"], s["outages"]["longest_ms"],
            ("%.1f" % (rs / 1000.0)) if rs else "-",
            amb,
            rc.get("phantom_writes", "-"), rc.get("lost_writes", "-")))
    w("\nColumns: outage_ms = longest window with no successful PING.\n")
    w("         ambig = ops that may or may not have been applied.\n")
    w("         phantom = writes the server applied but never acknowledged.\n")
    w("         lost = acknowledged writes missing from the server (must be 0).\n\n")
    for s in summaries:
        w("-" * 78 + "\n%s\n" % s["arm"])
        lp = s["latency_ms"]
        for phase in ("pre", "during", "post"):
            st = lp[phase]
            if st["n"]:
                w("  latency %-6s n=%-7d p50=%-8.2f p99=%-9.2f max=%.2f\n" % (
                    phase, st["n"], st["p50"], st["p99"], st["max"]))
        if s.get("baseline_ops_per_sec"):
            w("  baseline %.1f ops/s, estimated lost ops during reshard: %s\n" % (
                s["baseline_ops_per_sec"], s.get("estimated_lost_ops")))
        if s["outages"]["count"]:
            w("  outage windows (first few):\n")
            for g in s["outages"]["windows"][:5]:
                w("    %.0f ms  from t=%.0f to t=%.0f\n" % (
                    g["duration_ms"], g["start_ms"], g["end_ms"]))
        w("\n")
    sys.stdout.flush()


# --------------------------------------------------------------------------- #
# Arm runner
# --------------------------------------------------------------------------- #

def run_arm(rest, args, policy):
    is_control = (policy == CONTROL_ARM)
    db_policy = "single" if is_control else policy
    tag = policy.replace("-", "_")
    outdir = os.path.join(args.outdir, tag)
    if not os.path.isdir(outdir):
        os.makedirs(outdir)

    nodes = node_map(rest)
    me = local_node_uid(rest)
    db_name = "%s-%s" % (args.db_prefix, tag)

    log("creating db %s (proxy_policy=%s)" % (db_name, db_policy))
    uid = create_db(rest, db_name, db_policy, args.memory_size, args.db_password)
    rec = None
    stop_evt = threading.Event()
    workers = []
    result = {"arm": policy, "bdb_uid": uid, "db_name": db_name,
              "local_node_uid": me, "utc_start": now_utc()}

    try:
        wait_status(rest, uid, "active", args.create_timeout)
        pre = db_topology(rest, uid)
        log("pre-reshard topology:\n      %s" % describe(pre, nodes))
        result["pre"] = pre

        if len(pre["master_nodes"]) != 1:
            raise SystemExit("expected exactly 1 master, got %s" % pre["master_nodes"])

        if pre["oss_sharding"]:
            log("WARNING: oss_sharding=True (flex/ASM). Scale-out completion is "
                "is_balanced in CCS, not visible over REST; completion may be "
                "detected early and impact under-reported.")

        if me is not None:
            colocated = (me in pre["master_nodes"] or
                         nodes.get(me) in pre["endpoint_addrs"])
            if colocated:
                msg = ("this node (node%d) hosts the test DB's master and/or "
                       "endpoint; the load generator will compete with the shard "
                       "and proxy being measured" % me)
                if not args.allow_colocated:
                    raise SystemExit("REFUSING: %s. Re-run on another node, or "
                                     "pass --allow-colocated." % msg)
                log("WARNING: %s" % msg)
            else:
                log("client node = node%d (not hosting the test DB) - good" % me)
        else:
            log("WARNING: could not determine which node this is; cannot check "
                "co-location with the test DB")

        host = pre["dns_name"]
        port = pre["port"]
        if not host or not port:
            raise SystemExit("could not determine endpoint for bdb %d" % uid)
        try:
            resolved = sorted(set(i[4][0] for i in socket.getaddrinfo(host, int(port))))
        except Exception as exc:
            raise SystemExit("endpoint %s does not resolve from this node: %s" % (host, exc))
        log("endpoint %s:%s resolves to %s" % (host, port, resolved))

        prefix = "rstest:%s" % tag
        keys = ["%s:seq:%d" % (prefix, i) for i in range(args.corr_workers)]

        # Clear counters so reconciliation starts from a known state.
        try:
            c = Resp(host, int(port), args.db_password, args.op_timeout,
                     args.connect_timeout, name="reshardprobe-setup")
            c.connect()
            for k in keys:
                c.call("DEL", k)
            c.close()
        except Exception as exc:
            raise SystemExit("could not prepare counters on %s: %s" % (host, exc))

        anchor_clock()
        rec = Recorder(outdir)
        rec.event("run", "start", "arm=%s policy=%s bdb=%d endpoint=%s:%s" % (
            policy, db_policy, uid, host, port))

        workers.append(ProbeWorker(rec, stop_evt, host, int(port), args.db_password,
                                   args.probe_interval_ms, args.op_timeout,
                                   args.connect_timeout))
        for i in range(args.load_connections):
            workers.append(LoadWorker(rec, stop_evt, host, int(port), args.db_password,
                                      i, args.load_rate, prefix, args.keyspace,
                                      args.value_bytes, args.op_timeout,
                                      args.connect_timeout))
        corr = []
        for i in range(args.corr_workers):
            cw = CorrWorker(rec, stop_evt, host, int(port), args.db_password, i,
                            args.corr_interval_ms, keys[i], args.op_timeout,
                            args.connect_timeout)
            corr.append(cw)
            workers.append(cw)
        for t in workers:
            t.start()

        log("warmup %ds (establishing baseline)" % args.warmup)
        time.sleep(args.warmup)
        rec.event("run", "warmup_complete", "baseline established")

        def sample():
            t = ms()
            try:
                topo = db_topology(rest, uid)
            except Exception as exc:
                rec.event("topology", "poll_error", str(exc))
                return None
            rec.topology(t, topo)
            return topo

        if is_control:
            rec.event("run", "control_hold_start", "%ds, no reshard" % args.control_hold)
            end = time.monotonic() + args.control_hold
            while time.monotonic() < end:
                sample()
                time.sleep(args.poll_interval)
            rec.event("run", "control_hold_end", None)
        else:
            rec.event("run", "reshard_trigger", "1 -> %d shards" % args.target_shards)
            ok, status, data = trigger_reshard(rest, uid, args.target_shards)
            if not ok:
                rec.event("run", "reshard_trigger_failed", "HTTP %s: %s" % (status, data))
                raise SystemExit("reshard trigger failed HTTP %s: %s" % (status, data))
            result["reshard_http"] = status

            prev = None
            done_ms = None
            deadline = time.monotonic() + args.reshard_timeout
            while time.monotonic() < deadline:
                topo = sample()
                if topo is not None:
                    sig = (topo["status"], topo["shards_count"],
                           tuple(topo["master_nodes"]), tuple(topo["endpoint_addrs"]))
                    if sig != prev:
                        rec.event("topology", "change",
                                  "status=%s shards=%s masters=%s endpoint=%s" % (
                                      topo["status"], topo["shards_count"],
                                      topo["master_nodes"], topo["endpoint_addrs"]))
                        prev = sig
                try:
                    if reshard_done(rest, uid, args.target_shards):
                        done_ms = ms()
                        rec.event("run", "reshard_complete", "elapsed_ms=%.0f" % done_ms)
                        break
                except Exception as exc:
                    rec.event("run", "completion_check_error", str(exc))
                time.sleep(args.poll_interval)

            if done_ms is None:
                rec.event("run", "reshard_timeout",
                          "did not complete within %ds" % args.reshard_timeout)

            rec.event("run", "tail_start", "%ds recovery tail" % args.tail)
            end = time.monotonic() + args.tail
            while time.monotonic() < end:
                sample()
                time.sleep(args.poll_interval)

            post = db_topology(rest, uid)
            log("post-reshard topology:\n      %s" % describe(post, nodes))
            result["post"] = post

        rec.event("run", "stopping", "draining workers")
        stop_evt.set()
        for t in workers:
            t.join(timeout=max(10.0, args.op_timeout * 2))
        for t in workers:
            try:
                t.conn.close()
            except Exception:
                pass

        # Reconciliation: the point of the exercise. Retry hard.
        totals = {"attempted": 0, "acked": 0, "fail_definite": 0,
                  "fail_ambiguous": 0, "fail_server": 0}
        per_worker = []
        server_vals = {}
        deadline = time.monotonic() + args.reconcile_budget
        got = False
        while time.monotonic() < deadline and not got:
            try:
                c = Resp(host, int(port), args.db_password, 10.0, 10.0,
                         name="reshardprobe-reconcile")
                c.connect()
                for i, k in enumerate(keys):
                    v = c.call("GET", k)
                    server_vals[i] = int(v) if v is not None else 0
                c.close()
                got = True
            except Exception as exc:
                rec.event("run", "reconcile_retry", str(exc))
                time.sleep(3)
        if not got:
            rec.event("run", "reconcile_failed",
                      "server counters unavailable within %ds" % args.reconcile_budget)

        for cw in corr:
            sv = server_vals.get(cw.idx)
            applied = sv if sv is not None else None
            entry = {
                "worker": cw.idx, "key": cw.key,
                "attempted": cw.attempted, "acked": cw.acked,
                "fail_definite": cw.n_definite,
                "fail_ambiguous": cw.n_ambiguous,
                "fail_server": cw.n_server,
                "max_acked_value": cw.max_acked,
                "server_value": sv,
                "phantom_writes": (max(0, applied - cw.acked) if applied is not None else None),
                "lost_writes": (max(0, cw.acked - applied) if applied is not None else None),
                "reconnect_attempts": cw.reconnect_attempts,
            }
            per_worker.append(entry)
            totals["attempted"] += cw.attempted
            totals["acked"] += cw.acked
            totals["fail_definite"] += cw.n_definite
            totals["fail_ambiguous"] += cw.n_ambiguous
            totals["fail_server"] += cw.n_server
        if got:
            totals["applied_this_run"] = sum(server_vals.values())
            totals["phantom_writes"] = sum(e["phantom_writes"] for e in per_worker)
            totals["lost_writes"] = sum(e["lost_writes"] for e in per_worker)

        totals["reconnects_total"] = sum(w.conn.connects for w in workers)
        totals["reconnect_failures_total"] = sum(w.reconnect_failures for w in workers)

        with open(os.path.join(outdir, "reconcile.json"), "w") as fh:
            json.dump({"arm": policy, "per_worker": per_worker, "totals": totals,
                       "note": "phantom_writes = applied by server but never acked "
                               "(unsafe to blindly retry). lost_writes must be 0."},
                      fh, indent=2)
        rec.event("run", "reconcile_written", json.dumps(totals))
        result["reconcile"] = totals

    finally:
        stop_evt.set()
        if rec is not None:
            rec.event("run", "done", outdir)
            rec.close()
        if not args.keep_db:
            delete_db(rest, uid)
        else:
            log("--keep-db set; leaving bdb %d" % uid)

    result["utc_end"] = now_utc()
    with open(os.path.join(outdir, "arm_result.json"), "w") as fh:
        json.dump(result, fh, indent=2)
    return result


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #

def cmd_check(rest, args):
    cluster = rest.get("/v1/cluster")
    log("cluster: %s" % cluster.get("name"))
    me = local_node_uid(rest)
    log("this host appears to be node %s (local addrs: %s)" % (
        me, ", ".join(sorted(local_addresses()))))
    for n in sorted(rest.get("/v1/nodes"), key=lambda x: int(x["uid"])):
        log("  node%-3s addr=%-15s status=%-10s shards=%-4s cores=%-3s mem=%s" % (
            n["uid"], n.get("addr"), n.get("status"), n.get("shard_count"),
            n.get("cores"), n.get("total_memory")))
    bdbs = rest.get("/v1/bdbs")
    log("databases: %d" % len(bdbs))
    for b in bdbs:
        log("  bdb%-4s name=%-24s shards=%-4s policy=%-18s status=%s" % (
            b["uid"], b.get("name"), b.get("shards_count"),
            b.get("proxy_policy"), b.get("status")))
    return 0


def cmd_dryrun(rest, args):
    nodes = node_map(rest)
    me = local_node_uid(rest)
    name = "%s-dryrun" % args.db_prefix
    log("creating %s ..." % name)
    uid = create_db(rest, name, "single", args.memory_size, args.db_password)
    try:
        wait_status(rest, uid, "active", args.create_timeout)
        topo = db_topology(rest, uid)
        log("topology:\n      %s" % describe(topo, nodes))
        if topo["oss_sharding"]:
            log("NOTE: oss_sharding=True (flex/ASM) - REST cannot see is_balanced, "
                "so reshard completion detection is limited on this cluster")
        if me is not None and (me in topo["master_nodes"] or
                               nodes.get(me) in topo["endpoint_addrs"]):
            log("WARNING: this node hosts the DB master/endpoint. For the real run, "
                "use a different node or pass --allow-colocated.")
        host, port = topo["dns_name"], topo["port"]
        try:
            resolved = sorted(set(i[4][0] for i in socket.getaddrinfo(host, int(port))))
            log("endpoint %s:%s resolves to %s" % (host, port, resolved))
        except Exception as exc:
            log("WARNING: endpoint %s does not resolve here: %s" % (host, exc))
        # Prove we can actually talk to it.
        try:
            c = Resp(host, int(port), args.db_password, 5.0, 5.0, name="reshardprobe-dryrun")
            c.connect()
            c.call("SET", "rstest:dryrun:probe", "1")
            v = c.call("GET", "rstest:dryrun:probe")
            c.call("DEL", "rstest:dryrun:probe")
            c.close()
            log("data-path check OK (SET/GET round-trip returned %r)" % v)
        except Exception as exc:
            log("WARNING: data-path check FAILED: %s" % exc)
    finally:
        if not args.keep_db:
            delete_db(rest, uid)
    return 0


def cmd_arm(rest, args):
    s = run_arm(rest, args, args.policy)
    tag = args.policy.replace("-", "_")
    summary = analyze_arm(os.path.join(args.outdir, tag), args.min_gap_ms)
    with open(os.path.join(args.outdir, tag, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print_report([summary])
    return 0


def cmd_matrix(rest, args):
    arms = [CONTROL_ARM] + POLICIES
    summaries = []
    for i, arm in enumerate(arms):
        log("=" * 70)
        log("ARM %d/%d: %s" % (i + 1, len(arms), arm))
        log("=" * 70)
        try:
            run_arm(rest, args, arm)
        except SystemExit as exc:
            log("ARM %s FAILED: %s" % (arm, exc))
        except Exception as exc:
            log("ARM %s ERROR: %s: %s" % (arm, type(exc).__name__, exc))
        tag = arm.replace("-", "_")
        d = os.path.join(args.outdir, tag)
        if os.path.isdir(d):
            s = analyze_arm(d, args.min_gap_ms)
            with open(os.path.join(d, "summary.json"), "w") as fh:
                json.dump(s, fh, indent=2)
            summaries.append(s)
        if i < len(arms) - 1:
            log("settling %ds before next arm" % args.between_arms)
            time.sleep(args.between_arms)
    with open(os.path.join(args.outdir, "matrix_summary.json"), "w") as fh:
        json.dump(summaries, fh, indent=2)
    print_report(summaries)
    log("done. Collect with:  tar czf results.tgz %s" % args.outdir)
    return 0


def cmd_analyze(rest, args):
    summaries = []
    for tag in [CONTROL_ARM] + [p.replace("-", "_") for p in POLICIES]:
        d = os.path.join(args.outdir, tag.replace("-", "_"))
        if os.path.isdir(d):
            summaries.append(analyze_arm(d, args.min_gap_ms))
    if not summaries:
        log("no arm directories found under %s" % args.outdir)
        return 1
    print_report(summaries)
    return 0


def main():
    p = argparse.ArgumentParser(
        description="Measure client impact of resharding under proxy policies.")
    p.add_argument("command", choices=["check", "dryrun", "arm", "matrix", "analyze"])
    p.add_argument("--policy", choices=POLICIES + [CONTROL_ARM])

    p.add_argument("--rest-host", default="localhost")
    p.add_argument("--rest-port", type=int, default=9443)
    p.add_argument("--user", default=os.environ.get("RL_REST_USER"))
    p.add_argument("--password", default=os.environ.get("RL_REST_PASSWORD"))

    p.add_argument("--outdir", default="results")
    p.add_argument("--db-prefix", default="reshardtest")
    p.add_argument("--db-password", default=os.environ.get("RL_DB_PASSWORD", ""))
    p.add_argument("--memory-size", type=int, default=1073741824)
    p.add_argument("--target-shards", type=int, default=2)
    p.add_argument("--keep-db", action="store_true")
    p.add_argument("--allow-colocated", action="store_true",
                   help="run even if this node hosts the test DB (not recommended)")

    p.add_argument("--warmup", type=int, default=60)
    p.add_argument("--tail", type=int, default=120)
    p.add_argument("--control-hold", type=int, default=180)
    p.add_argument("--reshard-timeout", type=int, default=900)
    p.add_argument("--create-timeout", type=int, default=300)
    p.add_argument("--reconcile-budget", type=int, default=120)
    p.add_argument("--between-arms", type=int, default=30)
    p.add_argument("--poll-interval", type=float, default=0.5)

    p.add_argument("--probe-interval-ms", type=float, default=50)
    p.add_argument("--load-connections", type=int, default=4)
    p.add_argument("--load-rate", type=float, default=50)
    p.add_argument("--corr-workers", type=int, default=4)
    p.add_argument("--corr-interval-ms", type=float, default=50)
    p.add_argument("--keyspace", type=int, default=10000)
    p.add_argument("--value-bytes", type=int, default=100)
    p.add_argument("--op-timeout", type=float, default=5.0,
                   help="per-operation timeout, seconds (analogous to syncTimeout)")
    p.add_argument("--connect-timeout", type=float, default=5.0)
    p.add_argument("--min-gap-ms", type=float, default=250,
                   help="minimum gap between successful pings counted as an outage")

    args = p.parse_args()

    if args.command in ("check", "dryrun", "arm", "matrix"):
        missing = [n for n, v in (("--user", args.user), ("--password", args.password))
                   if not v]
        if missing:
            p.error("missing required: %s" % ", ".join(missing))
    if args.command == "arm" and not args.policy:
        p.error("--policy is required for 'arm'")

    if not os.path.isdir(args.outdir):
        os.makedirs(args.outdir)

    rest = None
    if args.command != "analyze":
        rest = Rest(args.rest_host, args.rest_port, args.user, args.password)

    return {"check": cmd_check, "dryrun": cmd_dryrun, "arm": cmd_arm,
            "matrix": cmd_matrix, "analyze": cmd_analyze}[args.command](rest, args)


if __name__ == "__main__":
    sys.exit(main())

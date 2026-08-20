#!/usr/bin/env python3
"""
Analyse a results directory produced by the matrix.

Usage:
    python tools/analyze.py <results-dir>

Splits each arm into pre / during / post windows using the reshard markers the
driver wrote into the harness event log, then reports availability, correctness,
latency and throughput per window, plus which proxy actually served the client.

The control arm is the noise floor: subtract it before attributing anything to
resharding.
"""

import csv
import json
import os
import sys

ARMS = ["control", "single", "all_master_shards", "all_nodes"]


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        return list(csv.DictReader(fh))


def pct(vals, p):
    if not vals:
        return None
    k = int(round((p / 100.0) * (len(vals) - 1)))
    return vals[max(0, min(k, len(vals) - 1))]


def fmt(v, nd=2):
    return "-" if v is None else ("%.*f" % (nd, v))


def windows(events):
    """(trigger_ms, complete_ms, warmup_ms, is_reshard).

    is_reshard distinguishes a real reshard from the control arm's hold, because
    only a reshard arm needs a baseline before the event to be meaningful.
    """
    warmup = trigger = complete = None
    is_reshard = False
    for e in events:
        kind, detail, t = e["kind"], e.get("detail") or "", float(e["t_ms"])
        if kind == "warmup_complete" and warmup is None:
            warmup = t
        if kind == "marker":
            if detail.startswith("reshard_trigger") and trigger is None:
                trigger, is_reshard = t, True
            elif detail.startswith("reshard_complete") and complete is None:
                complete = t
            elif detail.startswith("control_hold_start") and trigger is None:
                trigger = t
            elif detail.startswith("control_hold_end") and complete is None:
                complete = t
    return trigger, complete, warmup, is_reshard


def subscription_drops(events):
    """Pair ConnectionFailed with the next ConnectionRestored per multiplexer."""
    pending, spans = {}, []
    for e in events:
        src, kind, detail = e["source"], e["kind"], e.get("detail") or ""
        t = float(e["t_ms"])
        ctype = "Subscription" if "type=Subscription" in detail else (
                "Interactive" if "type=Interactive" in detail else "?")
        if kind == "ConnectionFailed":
            pending.setdefault((src, ctype), []).append(t)
        elif kind == "ConnectionRestored":
            q = pending.get((src, ctype))
            if q:
                spans.append((src, ctype, q.pop(0), t))
    return spans


def analyse_arm(d):
    events = read_csv(os.path.join(d, "events.csv"))
    ops = read_csv(os.path.join(d, "ops.csv"))
    probe = read_csv(os.path.join(d, "probe.csv"))
    trigger, complete, warmup, is_reshard = windows(events)

    arm = {"name": os.path.basename(d), "trigger": trigger, "complete": complete,
           "warmup": warmup, "is_reshard": is_reshard}
    # A reshard that fired before warmup finished leaves no baseline to compare
    # against, so its windows are meaningless and the arm must be re-run. The
    # control arm has no reshard, so the whole post-warmup run is its baseline.
    arm["valid"] = (not is_reshard) or (
        trigger is not None and warmup is not None and trigger > warmup)
    try:
        arm["reconcile"] = json.load(
            open(os.path.join(d, "reconcile.json"), encoding="utf-8"))["totals"]
    except Exception:
        arm["reconcile"] = {}

    # Which proxy served us. Under all-nodes this decides whether a "no impact"
    # result means anything: a client served by the proxy on its own node would
    # not notice a reshard elsewhere.
    proxies = {}
    for r in read_csv(os.path.join(d, "proxy_conns.csv")):
        a = r.get("proxy_addr")
        if a and a != "none":
            proxies[a] = proxies.get(a, 0) + int(r.get("our_connections") or 0)
    arm["proxies"] = proxies

    # Load-generator latency and throughput per window. skipped_backpressure is a
    # harness-side drop, not a server outcome, so it is counted separately.
    def bucket(t):
        if trigger is None:
            return "pre"
        if t < trigger:
            return "pre"
        if complete is None or t <= complete:
            return "during"
        return "post"

    stats = {w: {"ok": 0, "fail": 0, "skipped": 0, "lat": []}
             for w in ("pre", "during", "post")}
    for r in ops:
        if r["role"] != "load":
            continue
        t = float(r["t_issue_ms"])
        if warmup is not None and t < warmup:
            continue  # exclude warmup from the baseline
        w = stats[bucket(t)]
        if r["op"] == "skipped_backpressure":
            w["skipped"] += 1
        elif r["outcome"] == "ok":
            w["ok"] += 1
            w["lat"].append(float(r["latency_ms"]))
        else:
            w["fail"] += 1

    spans = {"pre": (warmup or 0, trigger), "during": (trigger, complete),
             "post": (complete, None)}
    end = max([float(r["t_done_ms"]) for r in ops], default=0.0)
    for w, s in stats.items():
        s["lat"].sort()
        lo, hi = spans[w]
        hi = hi if hi is not None else end
        # Ops before warmup_complete are excluded, so the window must not start
        # earlier than that or the rate is understated.
        if lo is not None and warmup is not None:
            lo = max(lo, warmup)
        dur = ((hi - lo) / 1000.0) if (lo is not None and hi is not None
                                       and hi > lo) else None
        s["dur_s"] = dur
        s["rate"] = (s["ok"] / dur) if dur and dur > 0 else None
        s["p50"], s["p99"], s["p999"] = (pct(s["lat"], 50), pct(s["lat"], 99),
                                         pct(s["lat"], 99.9))
        s["max"] = s["lat"][-1] if s["lat"] else None
    arm["load"] = stats

    # Availability: the largest gap between consecutive successful pings.
    oks = sorted(float(r["t_done_ms"]) for r in probe if r["outcome"] == "ok")
    gap, gap_at = 0.0, None
    for i in range(1, len(oks)):
        g = oks[i] - oks[i - 1]
        if g > gap:
            gap, gap_at = g, oks[i]
    arm["probe"] = {"n": len(probe), "ok": len(oks),
                    "fail": len(probe) - len(oks),
                    "max_gap_ms": gap, "max_gap_at_ms": gap_at}

    arm["conn"] = subscription_drops(events)
    return arm


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    root = sys.argv[1]
    arms = [analyse_arm(os.path.join(root, a)) for a in ARMS
            if os.path.isdir(os.path.join(root, a))]
    if not arms:
        print("no arm directories under %s" % root)
        return 1

    w = sys.stdout.write

    w("\n" + "=" * 96 + "\n")
    w("CORRECTNESS AND AVAILABILITY\n")
    w("=" * 96 + "\n")
    w("%-18s %9s %9s %9s %9s %9s %11s %10s\n" % (
        "arm", "attempted", "acked", "ambig", "phantom", "lost",
        "probe_fail", "max_gap_ms"))
    w("-" * 96 + "\n")
    for a in arms:
        r = a["reconcile"]
        if not a["valid"]:
            w("%-18s  ** INVALID: event at t=%.1fs but baseline only ready at "
              "t=%.1fs -- re-run **\n" % (
                  a["name"], (a["trigger"] or 0) / 1000.0,
                  (a["warmup"] or 0) / 1000.0))
        w("%-18s %9s %9s %9s %9s %9s %11d %10.1f\n" % (
            a["name"], r.get("attempted", "-"), r.get("acked", "-"),
            r.get("fail_ambiguous", "-"), r.get("phantom_writes", "-"),
            r.get("lost_writes", "-"), a["probe"]["fail"], a["probe"]["max_gap_ms"]))

    w("\n" + "=" * 96 + "\n")
    w("LOAD LATENCY AND THROUGHPUT BY WINDOW (SET/GET)\n")
    w("=" * 96 + "\n")
    w("%-18s %-7s %8s %7s %8s %8s %9s %9s %8s\n" % (
        "arm", "window", "dur_s", "ok", "ops/s", "p50_ms", "p99_ms", "p99.9_ms", "max_ms"))
    w("-" * 96 + "\n")
    for a in arms:
        for win in ("pre", "during", "post"):
            s = a["load"][win]
            if not s["ok"] and not s["fail"]:
                continue
            w("%-18s %-7s %8s %7d %8s %8s %9s %9s %8s\n" % (
                a["name"], win, fmt(s["dur_s"], 1), s["ok"], fmt(s["rate"], 1),
                fmt(s["p50"]), fmt(s["p99"]), fmt(s["p999"]), fmt(s["max"])))
        w("\n")

    w("=" * 96 + "\n")
    w("CONNECTION DROPS (by SE.Redis connection type)\n")
    w("=" * 96 + "\n")
    for a in arms:
        inter = [s for s in a["conn"] if s[1] == "Interactive"]
        subs = [s for s in a["conn"] if s[1] == "Subscription"]
        w("%-18s Interactive=%d  Subscription=%d" % (a["name"], len(inter), len(subs)))
        if subs:
            rec = [(e - b) for _, _, b, e in subs]
            w("  recovery ms: min=%.0f med=%.0f max=%.0f" % (
                min(rec), sorted(rec)[len(rec) // 2], max(rec)))
        w("\n")
        for src, ctype, b, e in subs:
            rel = ""
            if a["trigger"] is not None:
                rel = "  (t_trigger%+.1fs)" % ((b - a["trigger"]) / 1000.0)
            w("    %-6s %-13s down %6.0f ms at t=%.1fs%s\n" % (
                src, ctype, e - b, b / 1000.0, rel))

    w("\n" + "=" * 96 + "\n")
    w("WHICH PROXY SERVED THE CLIENT\n")
    w("=" * 96 + "\n")
    for a in arms:
        tot = sum(a["proxies"].values()) or 1
        parts = ", ".join("%s (%.0f%%)" % (k, 100.0 * v / tot)
                          for k, v in sorted(a["proxies"].items(),
                                             key=lambda kv: -kv[1]))
        w("  %-18s %s\n" % (a["name"], parts or "no samples"))
    w("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

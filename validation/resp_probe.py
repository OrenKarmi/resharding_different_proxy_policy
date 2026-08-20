"""Minimal RESP probe: is the Redis endpoint stable from Windows, independent of
StackExchange.Redis? Tests sequential pipelining on one socket, then a burst of
concurrent short-lived connections (which is what a multiplexer reconnect storm
looks like)."""
import socket
import sys
import threading
import time


def one_conn_many_pings(host, port, n=50, timeout=5):
    try:
        s = socket.create_connection((host, port), timeout=timeout)
    except Exception as exc:
        return f"connect failed: {exc}"
    ok = bad = 0
    try:
        for _ in range(n):
            s.sendall(b"PING\r\n")
            data = s.recv(64)
            if data == b"+PONG\r\n":
                ok += 1
            else:
                bad += 1
                if bad <= 2:
                    print(f"    unexpected reply: {data!r}")
    except Exception as exc:
        return f"ok={ok} bad={bad} then error: {exc}"
    finally:
        s.close()
    return f"ok={ok} bad={bad}"


def burst(host, port, count=50, timeout=5):
    results = {"ok": 0, "fail": 0}
    lock = threading.Lock()
    errs = []

    def work():
        try:
            s = socket.create_connection((host, port), timeout=timeout)
            s.sendall(b"PING\r\n")
            d = s.recv(64)
            s.close()
            with lock:
                if d == b"+PONG\r\n":
                    results["ok"] += 1
                else:
                    results["fail"] += 1
                    errs.append(repr(d))
        except Exception as exc:
            with lock:
                results["fail"] += 1
                errs.append(str(exc))

    ts = [threading.Thread(target=work) for _ in range(count)]
    t0 = time.time()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    el = time.time() - t0
    msg = f"ok={results['ok']} fail={results['fail']} in {el:.2f}s"
    if errs:
        msg += f" | sample errors: {errs[:3]}"
    return msg


if __name__ == "__main__":
    port = 6379
    for host in sys.argv[1:]:
        print(f"=== {host}:{port} ===")
        print(f"  50 pings on 1 socket : {one_conn_many_pings(host, port)}")
        print(f"  50 concurrent conns  : {burst(host, port)}")

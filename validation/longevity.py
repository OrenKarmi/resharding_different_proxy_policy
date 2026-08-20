"""Does a long-lived connection survive? The harness needs connections that stay
open for minutes; the earlier burst test only proved short-lived connections work.
Reports the elapsed time at which the peer closes the socket, if it does."""
import socket
import sys
import time


def longevity(host, port=6379, seconds=25, interval=0.2):
    t0 = time.time()
    try:
        s = socket.create_connection((host, port), timeout=5)
        s.settimeout(5)
    except Exception as exc:
        return f"connect failed: {exc}"
    n = 0
    try:
        while time.time() - t0 < seconds:
            s.sendall(b"PING\r\n")
            d = s.recv(64)
            if not d:
                return f"PEER CLOSED after {time.time()-t0:.2f}s ({n} pings)"
            if d != b"+PONG\r\n":
                return f"unexpected {d!r} after {time.time()-t0:.2f}s ({n} pings)"
            n += 1
            time.sleep(interval)
    except Exception as exc:
        return f"ERROR after {time.time()-t0:.2f}s ({n} pings): {type(exc).__name__}: {exc}"
    finally:
        s.close()
    return f"survived {seconds}s, {n} pings OK"


if __name__ == "__main__":
    for host in sys.argv[1:]:
        print(f"{host:20s} -> {longevity(host)}", flush=True)

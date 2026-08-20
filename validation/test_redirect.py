"""
Verify Rest.mutate follows a 307 the way Redis Enterprise issues it: same method,
same body, and the master's origin remembered for later calls.

Getting this wrong is dangerous rather than merely broken - a redirect handled as
a GET would silently turn "create database" into a no-op that looks successful.

No cluster needed. Two local HTTP servers: one always 307s to the other, which
records what it actually received.
"""
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                os.pardir, "orchestrator"))
import node_driver  # noqa: E402

RECEIVED = []


def make_handler(mode, target=None):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _handle(self):
            length = int(self.headers.get("Content-Length") or 0)
            _body = self.rfile.read(length) if length else b""
            if mode == "redirect":
                self.send_response(307)
                self.send_header("Location", target + self.path)
                self.end_headers()
                return
            if mode == "loop":
                self.send_response(307)
                self.send_header("Location", target + self.path)
                self.end_headers()
                return
            RECEIVED.append({"method": self.command, "path": self.path,
                             "body": _body.decode("utf-8", "replace"),
                             "auth": self.headers.get("Authorization")})
            payload = json.dumps({"uid": 42}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        do_GET = do_POST = do_PUT = do_DELETE = _handle

    return H


def serve(handler):
    srv = HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, srv.server_address[1]


def main():
    failures = []

    final_srv, final_port = serve(make_handler("final"))
    final_origin = "http://127.0.0.1:%d" % final_port
    redir_srv, redir_port = serve(make_handler("redirect", final_origin))

    rest = node_driver.Rest("127.0.0.1", redir_port, "u", "p")
    rest.base = "http://127.0.0.1:%d" % redir_port  # plain HTTP for the test

    body = {"name": "reshardtest-x", "shards_count": 1}
    ok, status, data = rest.mutate("POST", "/v1/bdbs", body)

    if not (ok and status == 200):
        failures.append("mutate did not succeed through the redirect: ok=%s status=%s data=%r"
                        % (ok, status, data))
    if not RECEIVED:
        failures.append("final server received nothing")
    else:
        got = RECEIVED[-1]
        if got["method"] != "POST":
            failures.append("method not preserved across 307: got %s" % got["method"])
        if got["path"] != "/v1/bdbs":
            failures.append("path not preserved: got %s" % got["path"])
        try:
            if json.loads(got["body"]) != body:
                failures.append("body not preserved: got %s" % got["body"])
        except ValueError:
            failures.append("body was not valid JSON: %r" % got["body"])
        if not (got["auth"] or "").startswith("Basic "):
            failures.append("Authorization header lost across redirect")
    if data != {"uid": 42}:
        failures.append("response not parsed: %r" % data)
    if rest.base != final_origin:
        failures.append("base not updated to the redirect target: %s" % rest.base)

    # A second mutation must now go straight to the master, with no further redirect.
    before = len(RECEIVED)
    ok2, status2, _ = rest.mutate("DELETE", "/v1/bdbs/42")
    if not (ok2 and status2 == 200 and len(RECEIVED) == before + 1):
        failures.append("second mutation did not go direct: ok=%s status=%s" % (ok2, status2))
    elif RECEIVED[-1]["method"] != "DELETE":
        failures.append("second mutation method wrong: %s" % RECEIVED[-1]["method"])

    # Redirect loop must terminate rather than recurse forever.
    loop_holder = {}
    loop_srv, loop_port = serve(make_handler("loop", "PLACEHOLDER"))
    loop_srv.shutdown()
    # Point a fresh client at a server that redirects to itself.
    self_origin = [None]

    class SelfLoop(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _handle(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                self.rfile.read(length)
            self.send_response(307)
            self.send_header("Location", self_origin[0] + self.path)
            self.end_headers()

        do_GET = do_POST = do_PUT = do_DELETE = _handle

    ls, lport = serve(SelfLoop)
    self_origin[0] = "http://127.0.0.1:%d" % lport
    rest2 = node_driver.Rest("127.0.0.1", lport, "u", "p")
    rest2.base = self_origin[0]
    ok3, status3, msg3 = rest2.mutate("POST", "/v1/bdbs", {"a": 1})
    if ok3:
        failures.append("redirect loop was not stopped")
    elif "too many redirects" not in str(msg3):
        failures.append("loop stopped but with unexpected error: %r" % msg3)

    for srv in (final_srv, redir_srv, ls):
        srv.shutdown()

    if failures:
        print("FAIL")
        for f in failures:
            print("  - %s" % f)
        return 1
    print("PASS: 307 preserves method+body+auth, base is cached, loops are bounded")
    return 0


if __name__ == "__main__":
    sys.exit(main())

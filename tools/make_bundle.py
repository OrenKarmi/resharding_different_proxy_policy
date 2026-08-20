#!/usr/bin/env python3
"""
Assemble ../reshard_bundle.sh: one pasteable, self-extracting shell script that
carries the C# harness sources and the node orchestrator inside it.

Generated from the real sources rather than hand-copied, so editing anything under
harness/ or orchestrator/ and re-running this is the only way the bundle should
ever change. Heredocs are quoted ('EOF') so no shell expansion touches payloads.

Usage:
    python tools/make_bundle.py
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
HARNESS = os.path.join(ROOT, "harness")
ORCH = os.path.join(ROOT, "orchestrator")

# (path inside $WORKDIR on the node, source file here)
PAYLOADS = [
    ("ReshardProbe/ReshardProbe.csproj", os.path.join(HARNESS, "ReshardProbe.csproj")),
    ("ReshardProbe/Infra.cs", os.path.join(HARNESS, "Infra.cs")),
    ("ReshardProbe/Roles.cs", os.path.join(HARNESS, "Roles.cs")),
    ("ReshardProbe/Program.cs", os.path.join(HARNESS, "Program.cs")),
    ("node_driver.py", os.path.join(ORCH, "node_driver.py")),
]

PROLOGUE = r'''#!/bin/bash
# =============================================================================
# reshard_bundle.sh - proxy-policy resharding client-impact test
#
# Self-contained. Paste onto a Redis Enterprise node, then:
#
#   bash reshard_bundle.sh setup
#   bash reshard_bundle.sh check    --user "$RL_USER" --password "$RL_PASS"
#   bash reshard_bundle.sh dryrun   --user "$RL_USER" --password "$RL_PASS"
#   bash reshard_bundle.sh matrix   --user "$RL_USER" --password "$RL_PASS" \
#                                   --db-password 'testpass'
#   bash reshard_bundle.sh collect
#
# WHAT IT DOES
#   Builds and runs a real StackExchange.Redis / NRedisStack client (pinned to
#   SE.Redis 2.8.x, what production .NET apps run), drives load against a
#   database endpoint, reshards 1 -> 2 shards, and measures what the client
#   experiences: outage duration, dropped vs ambiguous commands, phantom writes,
#   throughput hole, latency percentiles, reconnects.
#
#   Arms: control (no reshard), single, all-master-shards, all-nodes.
#   Each arm creates a fresh DB (1 master + 1 replica, dense placement,
#   proxy_policy set at create time) and deletes it afterwards, because shard
#   count cannot be reduced on a normal database.
#
# REQUIREMENTS
#   - run on a cluster node that does NOT host the test database (the script
#     detects this and refuses otherwise; override with --allow-colocated)
#   - python3 (present on Redis Enterprise nodes)
#   - outbound HTTPS during 'setup' only, to dot.net and nuget.org
#   - REST credentials for the cluster
#
#   If the node has no outbound HTTPS, skip 'setup': copy the prebuilt
#   self-contained binary (prebuilt/ReshardProbe-linux-x64) to the node instead
#   and run node_driver.py directly against it.
#
#   'setup' installs the .NET SDK under $HOME/.dotnet. Nothing is installed
#   system-wide; remove with:  rm -rf $HOME/.dotnet $HOME/reshard_probe
# =============================================================================
set -u

WORKDIR="${RESHARD_WORKDIR:-$HOME/reshard_probe}"
DOTNET_DIR="$HOME/.dotnet"
DOTNET="$DOTNET_DIR/dotnet"
DLL="$WORKDIR/ReshardProbe/bin/Release/net8.0/ReshardProbe.dll"
RESULTS="${RESHARD_RESULTS:-$WORKDIR/results}"

die() { echo "ERROR: $*" >&2; exit 1; }
info() { echo "[bundle] $*"; }

# Digest of the C# sources, so we can tell whether the built harness matches
# them. mtime is useless here: extract() rewrites the files on every run.
sources_digest() {
  cat "$WORKDIR"/ReshardProbe/*.csproj "$WORKDIR"/ReshardProbe/*.cs 2>/dev/null |
    { md5sum 2>/dev/null || cksum; } | awk '{print $1}'
}

extract() {
  info "extracting sources to $WORKDIR"
  mkdir -p "$WORKDIR/ReshardProbe" || die "cannot create $WORKDIR"
  _write_payloads
  # Payloads are emitted with Unix line endings, but be defensive in case the
  # copy/paste round-trip introduced CRLF.
  for f in "$WORKDIR"/ReshardProbe/*.cs "$WORKDIR"/ReshardProbe/*.csproj "$WORKDIR"/node_driver.py; do
    [ -f "$f" ] && sed -i 's/\r$//' "$f"
  done
}

cmd_setup() {
  extract

  if [ ! -x "$DOTNET" ]; then
    info "installing .NET SDK 8 into $DOTNET_DIR (user-local, no root)"
    command -v curl >/dev/null 2>&1 || command -v wget >/dev/null 2>&1 \
      || die "need curl or wget to fetch the .NET installer"
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL https://dot.net/v1/dotnet-install.sh -o "$WORKDIR/dotnet-install.sh" \
        || die "could not download dotnet-install.sh (no outbound HTTPS to dot.net?)"
    else
      wget -qO "$WORKDIR/dotnet-install.sh" https://dot.net/v1/dotnet-install.sh \
        || die "could not download dotnet-install.sh (no outbound HTTPS to dot.net?)"
    fi
    bash "$WORKDIR/dotnet-install.sh" --channel 8.0 --install-dir "$DOTNET_DIR" --no-path \
      || die ".NET SDK install failed"
  else
    info ".NET SDK already present at $DOTNET_DIR"
  fi

  export DOTNET_ROOT="$DOTNET_DIR"
  export PATH="$DOTNET_DIR:$PATH"
  export DOTNET_CLI_TELEMETRY_OPTOUT=1
  export DOTNET_NOLOGO=1

  "$DOTNET" --list-sdks || die "dotnet not runnable"

  info "building harness (restores StackExchange.Redis 2.8.x + NRedisStack from nuget.org)"
  ( cd "$WORKDIR/ReshardProbe" && "$DOTNET" build -c Release --nologo ) \
    || die "build failed (no outbound HTTPS to nuget.org?)"

  [ -f "$DLL" ] || die "expected $DLL after build"
  sources_digest > "$WORKDIR/.build_digest"
  info "OK. Harness built at $DLL"
  info "Next: bash $0 check --user <u> --password <p>"
}

need_built() {
  [ -x "$DOTNET" ] || die "run 'bash $0 setup' first (.NET not installed)"
  [ -f "$DLL" ]    || die "run 'bash $0 setup' first (harness not built)"
  [ -f "$WORKDIR/node_driver.py" ] || die "run 'bash $0 setup' first (driver missing)"
}

run_driver() {
  need_built
  local sub="$1"; shift
  export DOTNET_ROOT="$DOTNET_DIR"

  # Re-extract on every run. Otherwise a newer bundle still executes the
  # node_driver.py left behind by a previous 'setup', silently running stale
  # code - which is exactly how an already-fixed bug appeared to persist.
  extract

  # Only 'setup' compiles the C# sources, so warn if what is on disk no longer
  # matches what the harness was built from.
  if [ -f "$WORKDIR/.build_digest" ]; then
    if [ "$(sources_digest)" != "$(cat "$WORKDIR/.build_digest")" ]; then
      echo "[bundle] WARNING: C# sources differ from the built harness." >&2
      echo "[bundle]          Run: bash $0 setup   to rebuild before measuring." >&2
    fi
  fi

  python3 "$WORKDIR/node_driver.py" "$sub" --dotnet "$DOTNET" --probe-dll "$DLL" --outdir "$RESULTS" "$@"
}

cmd_collect() {
  local tgz="$HOME/reshard_results_$(date -u +%Y%m%dT%H%M%SZ).tgz"
  [ -d "$RESULTS" ] || die "no results directory at $RESULTS"
  tar czf "$tgz" -C "$(dirname "$RESULTS")" "$(basename "$RESULTS")" \
    || die "could not create $tgz"
  echo
  echo "Results archived: $tgz"
  echo "Copy that back for analysis."
}

usage() {
  sed -n '2,45p' "$0"
  echo
  echo "Subcommands: setup | check | dryrun | arm --policy <p> | matrix | collect"
  exit 1
}

[ $# -ge 1 ] || usage
SUB="$1"; shift || true

case "$SUB" in
  setup)   cmd_setup ;;
  collect) cmd_collect ;;
  check|dryrun|arm|matrix) run_driver "$SUB" "$@" ;;
  -h|--help|help) usage ;;
  *) die "unknown subcommand '$SUB' (try: setup, check, dryrun, arm, matrix, collect)" ;;
esac
'''


def emit_payload_writer(payloads):
    """Shell function writing each embedded file via a quoted heredoc."""
    lines = ["_write_payloads() {"]
    for rel, path in payloads:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            content = fh.read().replace("\r\n", "\n").replace("\r", "\n")
        marker = "RESHARD_EOF_%s" % rel.replace("/", "_").replace(".", "_").upper()
        if marker in content:
            raise SystemExit("heredoc marker collision in %s" % rel)
        lines.append('  cat > "$WORKDIR/%s" <<\'%s\'' % (rel, marker))
        lines.append(content.rstrip("\n"))
        lines.append(marker)
    lines.append("}")
    return "\n".join(lines) + "\n"


def main():
    for rel, path in PAYLOADS:
        if not os.path.exists(path):
            raise SystemExit("missing payload source: %s" % path)

    writer = emit_payload_writer(PAYLOADS)

    anchor = 'die() { echo "ERROR: $*" >&2; exit 1; }'
    if anchor not in PROLOGUE:
        raise SystemExit("prologue anchor not found")
    out = PROLOGUE.replace(anchor, anchor + "\n\n" + writer, 1)

    dest = os.path.join(ROOT, "reshard_bundle.sh")
    with open(dest, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(out)

    print("wrote %s" % dest)
    print("  %d bytes, %d lines" % (os.path.getsize(dest), out.count("\n") + 1))
    for rel, path in PAYLOADS:
        print("  embedded %-34s %6d bytes" % (rel, os.path.getsize(path)))


if __name__ == "__main__":
    sys.exit(main())

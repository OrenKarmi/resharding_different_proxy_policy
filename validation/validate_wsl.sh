#!/bin/bash
# End-to-end validation of the harness against a real Redis over Linux loopback.
#
# Injects CLIENT PAUSE for longer than the client's syncTimeout. Commands sent
# during the pause time out client-side but are still executed by the server when
# the pause lifts, which is precisely the "applied but never acknowledged" case.
# Expected: fail_ambiguous > 0, phantom_writes > 0, lost_writes == 0.
set -e

export PATH="$HOME/.dotnet:$PATH"
# The apphost resolves the runtime via DOTNET_ROOT, not PATH; without this the
# binary exits with "You must install .NET to run this application".
export DOTNET_ROOT="$HOME/.dotnet"
DEST="$HOME/ReshardProbe"
OUT="$HOME/out_validate"
EXE="$DEST/bin/Release/net8.0/ReshardProbe"
MARKERS="$OUT/markers.txt"

PAUSE_MS=8000
SYNC_TIMEOUT=3000

rm -rf "$OUT"; mkdir -p "$OUT"; : > "$MARKERS"

redis-cli ping >/dev/null || { echo "redis not reachable"; exit 1; }
redis-cli del rstest:val:seq:0 rstest:val:seq:1 rstest:val:seq:2 rstest:val:seq:3 >/dev/null

echo "--- starting harness ---"
"$EXE" --endpoint 127.0.0.1:6379 --tag val --outdir "$OUT" --markers "$MARKERS" \
  --duration-sec 300 --warmup-sec 5 \
  --load-connections 2 --load-rate 100 \
  --corr-workers 4 --corr-interval-ms 50 \
  --probe-interval-ms 10 \
  --sync-timeout "$SYNC_TIMEOUT" --connect-timeout 3000 \
  --reconcile-budget-sec 30 > "$OUT/stdout.log" 2>&1 &
PID=$!

# Wait for the baseline to be established before injecting the fault. Must fail
# loudly rather than fall through, or we would inject into a dead harness and
# then misread the empty results.
WARMED=0
for _ in $(seq 1 120); do
  if grep -q warmup_complete "$OUT/events.csv" 2>/dev/null; then WARMED=1; break; fi
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "HARNESS DIED during warmup; stdout:"; cat "$OUT/stdout.log"; exit 1
  fi
  sleep 0.5
done
[ "$WARMED" = "1" ] || { echo "warmup_complete never appeared; stdout:"; cat "$OUT/stdout.log"; exit 1; }
echo "--- warmup complete; baseline running ---"
sleep 3

echo "--- injecting CLIENT PAUSE ${PAUSE_MS}ms (syncTimeout=${SYNC_TIMEOUT}ms) ---"
echo "pause_start ${PAUSE_MS}ms" >> "$MARKERS"
redis-cli client pause "$PAUSE_MS"
sleep $(( (PAUSE_MS / 1000) + 4 ))
echo "pause_end" >> "$MARKERS"
echo "--- pause lifted; holding for recovery tail ---"
sleep 8

echo "STOP" >> "$MARKERS"
wait $PID || true

echo
echo "=== server-side truth ==="
for i in 0 1 2 3; do
  printf "  rstest:val:seq:%s = %s\n" "$i" "$(redis-cli get rstest:val:seq:$i)"
done

echo
echo "=== reconcile.json ==="
cat "$OUT/reconcile.json"

echo
echo "=== ops outcome counts ==="
awk -F, 'NR>1 {print $4","$6","$7}' "$OUT/ops.csv" | sort | uniq -c | sort -rn | head -20

echo
echo "=== probe outcome counts ==="
awk -F, 'NR>1 {print $4}' "$OUT/probe.csv" | sort | uniq -c | sort -rn

echo
echo "=== probe: successful-ping gaps > 250ms (outage windows) ==="
awk -F, 'NR>1 && $4=="ok" {print $2}' "$OUT/probe.csv" | sort -n | awk '
  NR==1 { prev=$1; next }
  { gap = $1 - prev; if (gap > 250) printf "  gap %.0f ms ending at t=%.0f ms\n", gap, $1; prev=$1 }
  END { }'

echo
echo "VALIDATE_DONE"

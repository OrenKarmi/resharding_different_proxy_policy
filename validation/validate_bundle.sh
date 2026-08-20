#!/bin/bash
# Prove the harness built from the pasted bundle measures correctly, by injecting
# a CLIENT PAUSE longer than the client's syncTimeout. Commands sent during the
# pause time out client-side but the server still applies them when it lifts:
# the exact "applied but never acknowledged" case.
# Expected: an outage gap ~= the pause, fail_ambiguous > 0, phantom == ambiguous,
# lost == 0.
set -u

WORK=/tmp/rb_test
export DOTNET_ROOT=$HOME/.dotnet
DOTNET=$HOME/.dotnet/dotnet
DLL=$WORK/ReshardProbe/bin/Release/net8.0/ReshardProbe.dll
OUT=/tmp/out_bundle_val
MARKERS=$OUT/markers.txt
PAUSE_MS=8000
SYNC_TIMEOUT=3000

[ -f "$DLL" ] || { echo "missing $DLL - run bundle setup first"; exit 1; }
rm -rf "$OUT"; mkdir -p "$OUT"; : > "$MARKERS"
redis-cli ping >/dev/null || { echo "redis not reachable"; exit 1; }
redis-cli del rstest:bv:seq:0 rstest:bv:seq:1 rstest:bv:seq:2 rstest:bv:seq:3 >/dev/null

"$DOTNET" "$DLL" --endpoint 127.0.0.1:6379 --tag bv --outdir "$OUT" --markers "$MARKERS" \
  --duration-sec 300 --warmup-sec 5 \
  --load-connections 2 --load-rate 100 \
  --corr-workers 4 --corr-interval-ms 50 --probe-interval-ms 10 \
  --sync-timeout "$SYNC_TIMEOUT" --connect-timeout 3000 \
  --reconcile-budget-sec 30 > "$OUT/stdout.log" 2>&1 &
PID=$!

WARMED=0
for _ in $(seq 1 120); do
  if grep -q warmup_complete "$OUT/events.csv" 2>/dev/null; then WARMED=1; break; fi
  kill -0 $PID 2>/dev/null || { echo "harness died:"; cat "$OUT/stdout.log"; exit 1; }
  sleep 0.5
done
[ "$WARMED" = 1 ] || { echo "no warmup_complete"; cat "$OUT/stdout.log"; exit 1; }
echo "--- baseline running; injecting CLIENT PAUSE ${PAUSE_MS}ms ---"
sleep 3
echo "pause_start" >> "$MARKERS"
redis-cli client pause "$PAUSE_MS" >/dev/null
sleep $(( PAUSE_MS/1000 + 4 ))
echo "pause_end" >> "$MARKERS"
sleep 8
echo "STOP" >> "$MARKERS"
wait $PID 2>/dev/null

echo
echo "=== reconcile totals ==="
python3 -c "import json;t=json.load(open('$OUT/reconcile.json'))['totals'];print(json.dumps(t,indent=2))"
echo "=== server truth ==="
redis-cli mget rstest:bv:seq:0 rstest:bv:seq:1 rstest:bv:seq:2 rstest:bv:seq:3
echo "=== op outcomes ==="
awk -F, 'NR>1 {print $4","$6","$7}' "$OUT/ops.csv" | sort | uniq -c | sort -rn | head
echo "=== probe outcomes ==="
awk -F, 'NR>1 {print $4}' "$OUT/probe.csv" | sort | uniq -c
echo "=== outage gaps > 250ms between successful pings ==="
awk -F, 'NR>1 && $4=="ok" {print $2}' "$OUT/probe.csv" | sort -n | awk '
  NR==1 {prev=$1; next}
  {g=$1-prev; if (g>250) printf "  %.0f ms  (t=%.0f -> %.0f)\n", g, prev, $1; prev=$1}'
echo BUNDLE_VALIDATION_DONE

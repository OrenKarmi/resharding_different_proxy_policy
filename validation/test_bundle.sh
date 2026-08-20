#!/bin/bash
# Verify the self-contained linux-x64 artifact runs with no .NET installed:
# unset DOTNET_ROOT / drop ~/.dotnet from PATH so we cannot accidentally rely on it.
set -e
SRC="$1"
unset DOTNET_ROOT
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

cp "$SRC/ReshardProbe" /tmp/ReshardProbe
chmod +x /tmp/ReshardProbe
OUT=/tmp/out_bundle
rm -rf "$OUT"; mkdir -p "$OUT"

redis-cli del rstest:bundle:seq:0 rstest:bundle:seq:1 >/dev/null

echo "--- running self-contained binary (no dotnet on PATH) ---"
/tmp/ReshardProbe --endpoint 127.0.0.1:6379 --tag bundle --outdir "$OUT" \
  --duration-sec 6 --warmup-sec 2 --load-connections 1 --load-rate 50 \
  --corr-workers 2 --corr-interval-ms 20 --probe-interval-ms 10 \
  --reconcile-budget-sec 20 | tail -5

echo
echo "=== reconcile totals ==="
python3 -c "import json;print(json.load(open('$OUT/reconcile.json'))['totals'])"
echo "=== server truth ==="
redis-cli mget rstest:bundle:seq:0 rstest:bundle:seq:1
echo "=== probe outcomes ==="
awk -F, 'NR>1 {print $4}' "$OUT/probe.csv" | sort | uniq -c
echo BUNDLE_OK

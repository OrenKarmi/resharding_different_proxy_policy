#!/bin/bash
OUT="$HOME/out_validate"
echo "=== files ==="
ls -la "$OUT" 2>&1
echo
echo "=== stdout.log head ==="
head -c 4000 "$OUT/stdout.log" 2>&1
echo
echo "=== events.csv (first 25 lines, trimmed) ==="
cut -c1-220 "$OUT/events.csv" 2>&1 | head -25
echo
echo "=== ops.csv line count ==="
wc -l "$OUT/ops.csv" 2>&1
echo "=== probe.csv line count ==="
wc -l "$OUT/probe.csv" 2>&1

#!/bin/bash
# Sample the server's stacks while under load, to see what the writer waits on.
# Usage: sample_load.sh <rate> <conn> <duration> <outdir>
set -u
RATE=${1:-4000}
CONN=${2:-1024}
DUR=${3:-3m}
OUT=${4:-/tmp/breezy-sample}
BODY=/tmp/breezy-knee-body.jsonl
DB="$OUT/db.sqlite"
mkdir -p "$OUT"; rm -rf "$OUT"; mkdir -p "$OUT"
rm -f "$DB" "$DB-wal" "$DB-shm"

./target/release/examples/seed_node "$DB" 20 >/dev/null
BREEZY_SYNCHRONOUS=FULL BREEZY_FULLFSYNC=ON BREEZY_CACHE_SIZE=-2097152 \
  DB="$DB" PORT=3000 ./target/release/breezydb >"$OUT/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do curl -sf -o /dev/null http://127.0.0.1:3000/noop && break; sleep 0.1; done

oha -z "$DUR" -c "$CONN" -q "$RATE" --latency-correction --no-tui -u s \
  -m POST -T application/json -Z "$BODY" http://127.0.0.1:3000/tx >"$OUT/oha.txt" 2>&1 &
OHA=$!
sleep 45
sample "$SRV" 10 -file "$OUT/sample.txt" >/dev/null 2>&1
ps -o pid,%cpu,time,command -p "$SRV" >"$OUT/ps.txt" 2>/dev/null
wait "$OHA"
awk -F'\t' '/Requests\/sec|Success rate/{print} /50.00% in|90.00% in|99.00% in/{print}' "$OUT/oha.txt"
kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null
echo "sample in $OUT/sample.txt"
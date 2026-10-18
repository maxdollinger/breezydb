#!/bin/bash
# Instrumented run to locate p99 stalls.
#
# Usage: stall.sh <offered-rate> <duration> <outdir>
#   e.g.  scripts/stall.sh 4000 4m /tmp/breezy-stall
#
# While oha hammers /tx, this harness also:
#   - probes /noop + a 1-fact /tx every ~250ms (timestamped) -> probe.csv
#     (if /noop stays fast while /tx stalls, the HTTP loop is fine and the
#      storage-writer is the one stuck)
#   - samples DB size, WAL size, server CPU every 1s           -> sizes.csv
#   - when the probe /tx latency exceeds 1s, grabs a 3s stack
#     sample of the server (throttled to 1 per 15s)             -> sample-*.txt
#
# Requires: run from repo root; /tmp/breezy-knee-body.jsonl exists.

set -u
RATE=${1:-4000}
DUR=${2:-4m}
OUT=${3:-/tmp/breezy-stall}
CONN=${4:-128}
BODY=/tmp/breezy-knee-body.jsonl
DB="$OUT/db.sqlite"
PROBE_TX='{"ops":[{"op":"entity","as":"$e"},{"op":"add","e":"$e","a":2,"v":"probe"}]}'

mkdir -p "$OUT"
rm -f "$OUT"/probe.csv "$OUT"/sizes.csv "$OUT"/markers.txt "$OUT"/oha.txt "$OUT"/sample-*.txt
rm -f "$DB" "$DB-wal" "$DB-shm"

now() { perl -MTime::HiRes=time -e 'print time'; echo; }

./target/release/examples/seed_node "$DB" 20 >/dev/null
BREEZY_SYNCHRONOUS=FULL BREEZY_FULLFSYNC=ON BREEZY_CACHE_SIZE=-2097152 \
  DB="$DB" PORT=3000 ./target/release/breezydb >"$OUT/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 100); do
  curl -sf -o /dev/null http://127.0.0.1:3000/noop && break
  sleep 0.1
done

echo "$(now) server_up" >>"$OUT/markers.txt"
echo "time,noop_s,tx_s" >>"$OUT/probe.csv"
echo "time,db_bytes,wal_bytes,cpu_pct,cputime" >>"$OUT/sizes.csv"

# --- probe loop -------------------------------------------------------------
(
  last_sample=0
  while kill -0 "$SRV" 2>/dev/null; do
    t=$(now)
    noop=$(curl -s -o /dev/null -w '%{time_total}' -m 60 http://127.0.0.1:3000/noop 2>/dev/null || echo -1)
    tx=$(curl -s -o /dev/null -w '%{time_total}' -m 60 -X POST \
         -H 'content-type: application/json' -d "$PROBE_TX" \
         http://127.0.0.1:3000/tx 2>/dev/null || echo -1)
    echo "$t,$noop,$tx" >>"$OUT/probe.csv"
    # spike -> stack sample (throttled)
    if awk -v x="$tx" 'BEGIN { exit !(x > 1.0) }'; then
      n=$(printf '%.0f' "$(now)")
      if [ "$(( n - last_sample ))" -ge 15 ]; then
        last_sample=$n
        sample "$SRV" 3 -file "$OUT/sample-$n.txt" >/dev/null 2>&1
      fi
    fi
    sleep 0.25
  done
) &
PROBE=$!

# --- size monitor -----------------------------------------------------------
(
  while kill -0 "$SRV" 2>/dev/null; do
    echo "$(now),$(stat -f%z "$DB" 2>/dev/null || echo 0),$(stat -f%z "$DB-wal" 2>/dev/null || echo 0),$(ps -o %cpu= -p "$SRV" 2>/dev/null | tr -d ' ')" >>"$OUT/sizes.csv"
    sleep 1
  done
) &
MON=$!

echo "$(now) load_start" >>"$OUT/markers.txt"
oha -z "$DUR" -c "$CONN" -q "$RATE" --latency-correction --no-tui -u s \
  -m POST -T application/json -Z "$BODY" http://127.0.0.1:3000/tx \
  | tee "$OUT/oha.txt" \
  | awk -F'\t' '/Requests\/sec|Success rate/{print} /50.00% in|90.00% in|99.00% in/{print}'
echo "$(now) load_end" >>"$OUT/markers.txt"

kill "$PROBE" "$MON" "$SRV" 2>/dev/null
wait "$PROBE" "$MON" "$SRV" 2>/dev/null
echo "artifacts in $OUT (probe.csv, sizes.csv, markers.txt, sample-*.txt)"

#!/usr/bin/env bash
#
# Bottleneck-localization / tuning harness. Seeds once, starts one server, then
# runs a sequence of oha measurements against it and prints a compact table.
#
# Usage:
#   scripts/tune.sh [closed|open] [qps] [fanouts]
#   scripts/tune.sh closed
#   scripts/tune.sh open 12000 1,2,5,11,20
#
# `closed` = no -q (max throughput), `open` = fixed offered qps + latency
# correction. Server env (BREEZY_*) is inherited from the caller.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-closed}"
QPS="${2:-12000}"
FANOUTS="${3:-1,2,5,11,20}"
DUR="${DUR:-6s}"
CONN="${CONN:-1024}"
PORT="${PORT:-3000}"
N_ATTRS="${N_ATTRS:-20}"
DB="${DB:-/tmp/breezy-tune.db}"
BDIR="${BDIR:-/tmp/breezy-tune-bodies}"
LINES="${LINES:-50}"

mkdir -p "$BDIR"
cargo build --release --bin breezydb --example seed_node >/dev/null

rm -f "$DB" "$DB-wal" "$DB-shm"
./target/release/examples/seed_node "$DB" "$N_ATTRS" >/dev/null

DB="$DB" PORT="$PORT" ./target/release/breezydb >"$BDIR/server.log" 2>&1 &
SPID=$!
trap 'kill "$SPID" 2>/dev/null || true' EXIT
ready=0
for _ in $(seq 1 100); do
  if curl -sf -o /dev/null "http://127.0.0.1:${PORT}/noop"; then ready=1; break; fi
  sleep 0.1
done
[ "$ready" -eq 1 ] || { echo "server failed to start; see $BDIR/server.log" >&2; exit 1; }

echo "mode=$MODE qps=$QPS dur=$DUR conn=$CONN fanouts=$FANOUTS"
printf '%-10s %12s %10s %10s %10s\n' "fanout" "req/s" "p50(ms)" "p99(ms)" "succ%"

for f in ${FANOUTS//,/ }; do
  body="$BDIR/f$f.jsonl"
  awk -v k="$f" -v nattrs="$N_ATTRS" -v lines="$LINES" '
  BEGIN { srand();
    for (l = 0; l < lines; l++) {
      for (i = 1; i <= nattrs; i++) pool[i] = i + 1;
      for (i = nattrs; i > 1; i--) { j = 1 + int(rand() * i); t = pool[i]; pool[i] = pool[j]; pool[j] = t; }
      body = "{\"ops\":[{\"op\":\"entity\",\"as\":\"$e\"}";
      for (i = 1; i <= k; i++)
        body = body ",{\"op\":\"add\",\"e\":\"$e\",\"a\":" pool[i] ",\"v\":\"v" int(rand() * 1000000) "\"}";
      print body "]}";
    }
  }' > "$body"

  if [ "$MODE" = closed ]; then
    out=$(oha -z "$DUR" -c "$CONN" --no-tui -m POST -T application/json -Z "$body" "http://127.0.0.1:${PORT}/tx")
  else
    out=$(oha -z "$DUR" -c "$CONN" -q "$QPS" --latency-correction --no-tui -m POST -T application/json -Z "$body" "http://127.0.0.1:${PORT}/tx")
  fi
  rps=$(printf '%s\n' "$out" | awk -F'\t' '/Requests\/sec/{print $2}')
  p50=$(printf '%s\n' "$out" | awk '/50.00% in/{print $(NF-1)}')
  p99=$(printf '%s\n' "$out" | awk '/99.00% in/{print $(NF-1)}')
  succ=$(printf '%s\n' "$out" | awk -F'\t' '/Success rate/{print $2}' | tr -d '%')
  printf '%-10s %12s %10s %10s %10s\n' "$f" "$rps" "$p50" "$p99" "$succ"
done

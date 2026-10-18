#!/usr/bin/env bash
#
# Sustained-load decay test: N consecutive open-loop oha blocks against ONE
# growing DB (no reseed between blocks), so throughput vs file size is visible.
# Server env (BREEZY_*) is inherited from the caller.
#
# Usage: scripts/sustained.sh [qps]
#   BREEZY_CACHE_SIZE=-2097152 BREEZY_PAGE_SIZE=65536 scripts/sustained.sh 9000
set -euo pipefail
cd "$(dirname "$0")/.."

QPS="${1:-9000}"
BLOCKS="${BLOCKS:-6}"
BLOCK_DUR="${BLOCK_DUR:-9s}"
CONN="${CONN:-1024}"
PORT="${PORT:-3000}"
N_ATTRS="${N_ATTRS:-20}"
DB="${DB:-/tmp/breezy-sustained.db}"
BODY="${BODY:-/tmp/breezy-knee-body.jsonl}"

if [ ! -f "$BODY" ]; then
  awk -v lines=2000 -v nattrs="$N_ATTRS" '
  BEGIN { srand();
    for (l = 0; l < lines; l++) {
      k = 2 + int(rand() * 19);
      for (i = 1; i <= nattrs; i++) pool[i] = i + 1;
      for (i = nattrs; i > 1; i--) { j = 1 + int(rand() * i); t = pool[i]; pool[i] = pool[j]; pool[j] = t; }
      body = "{\"ops\":[{\"op\":\"entity\",\"as\":\"$e\"}";
      for (i = 1; i <= k; i++)
        body = body ",{\"op\":\"add\",\"e\":\"$e\",\"a\":" pool[i] ",\"v\":\"v" int(rand() * 1000000) "\"}";
      print body "]}";
    }
  }' > "$BODY"
fi

rm -f "$DB" "$DB-wal" "$DB-shm"
./target/release/examples/seed_node "$DB" "$N_ATTRS" >/dev/null
DB="$DB" PORT="$PORT" ./target/release/breezydb >/tmp/breezy-sustained.log 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true' EXIT
ready=0
for _ in $(seq 1 100); do
  if curl -sf -o /dev/null "http://127.0.0.1:${PORT}/noop"; then ready=1; break; fi
  sleep 0.1
done
[ "$ready" -eq 1 ] || { echo "server failed; see /tmp/breezy-sustained.log" >&2; exit 1; }

echo "cache=${BREEZY_CACHE_SIZE:-default} page=${BREEZY_PAGE_SIZE:-default} qps=$QPS block=$BLOCK_DUR"
printf '%-6s %10s %9s %9s %9s %9s\n' block achieved p50 p99 dbMB walMB
for i in $(seq 1 "$BLOCKS"); do
  out=$(oha -z "$BLOCK_DUR" -c "$CONN" -q "$QPS" --latency-correction --no-tui -u s \
        -m POST -T application/json -Z "$BODY" "http://127.0.0.1:${PORT}/tx")
  rps=$(printf '%s\n' "$out" | awk -F'\t' '/Requests\/sec/{print $2}')
  p50=$(printf '%s\n' "$out" | awk '/50.00% in/{print $(NF-1)}')
  p99=$(printf '%s\n' "$out" | awk '/99.00% in/{print $(NF-1)}')
  db=$(( $(stat -f%z "$DB") / 1048576 ))
  wal=$(( $(stat -f%z "$DB-wal" 2>/dev/null || echo 0) / 1048576 ))
  printf '%-6s %10s %9s %9s %9s %9s\n' "$i" "$rps" "$p50" "$p99" "$db" "$wal"
  sleep 2
done
kill "$pid" 2>/dev/null || true
wait "$pid" 2>/dev/null || true
trap - EXIT
rm -f "$DB" "$DB-wal" "$DB-shm"

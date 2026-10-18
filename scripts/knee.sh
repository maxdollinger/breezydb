#!/usr/bin/env bash
#
# Open-loop knee finder. For each offered rate: fresh DB, seed, start server,
# run oha with -q + --latency-correction (true open loop), report achieved
# throughput and the latency percentiles. Each point gets its own DB so later
# points are not measured on a larger file.
#
# Usage: scripts/knee.sh <qps> [<qps> ...]
#   scripts/knee.sh 10000 11000 12000
set -euo pipefail
cd "$(dirname "$0")/.."

[ "$#" -ge 1 ] || { echo "usage: scripts/knee.sh <qps> [<qps> ...]" >&2; exit 1; }
DUR="${DUR:-10s}"
CONN="${CONN:-128}"
PORT="${PORT:-3000}"
N_ATTRS="${N_ATTRS:-20}"
DB="${DB:-/tmp/breezy-knee.db}"
BODY="${BODY:-/tmp/breezy-knee-body.jsonl}"
LINES="${LINES:-2000}"

# Mixed workload: one fresh entity + a random 2..20 distinct facts per request.
if [ ! -f "$BODY" ] || [ "${REGEN:-0}" = 1 ]; then
  awk -v lines="$LINES" -v nattrs="$N_ATTRS" '
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

printf '%-9s %11s %9s %9s %9s\n' "offered" "achieved" "p50" "p99" "succ%"
for q in "$@"; do
  rm -f "$DB" "$DB-wal" "$DB-shm"
  ./target/release/examples/seed_node "$DB" "$N_ATTRS" >/dev/null
  DB="$DB" PORT="$PORT" ./target/release/breezydb >/tmp/breezy-knee-server.log 2>&1 &
  pid=$!
  ready=0
  for _ in $(seq 1 100); do
    if curl -sf -o /dev/null "http://127.0.0.1:${PORT}/noop"; then ready=1; break; fi
    sleep 0.1
  done
  [ "$ready" -eq 1 ] || { echo "server failed; see /tmp/breezy-knee-server.log" >&2; exit 1; }

  out=$(oha -z "$DUR" -c "$CONN" -q "$q" --latency-correction --no-tui -u s \
        -m POST -T application/json -Z "$BODY" "http://127.0.0.1:${PORT}/tx")
  rps=$(printf '%s\n' "$out" | awk -F'\t' '/Requests\/sec/{print $2}')
  p50=$(printf '%s\n' "$out" | awk '/50.00% in/{print $(NF-1)}')
  p99=$(printf '%s\n' "$out" | awk '/99.00% in/{print $(NF-1)}')
  succ=$(printf '%s\n' "$out" | awk -F'\t' '/Success rate/{print $2}' | tr -d '%')
  printf '%-9s %11s %9s %9s %9s\n' "$q" "$rps" "$p50" "$p99" "$succ"

  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  sleep 1
done
rm -f "$DB" "$DB-wal" "$DB-shm"

#!/usr/bin/env bash
#
# Open-loop write-capacity test for breezyDB.
#
# Stresses POST /tx with a *random* number of facts per request (2..20). Each
# request creates one fresh entity and adds K distinct facts to it. `oha` is the
# driver; `-Z <file>` makes it send a uniformly random *line* of the file as the
# body on every request, so every request gets a different fan-out.
#
# Open-loop means the offered rate is fixed and independent of responses:
# `-q <QPS>` sets the offered transactions/s and `--latency-correction` undoes
# coordinated omission, so a stalled server shows up in the tail instead of
# silently lowering the offered load.
#
# Usage:
#   scripts/write-load.sh [offered_qps] [duration]
#   scripts/write-load.sh 20000 10s
#
# Env knobs: PORT, DB, BODIES, BODY_LINES, MIN_FACTS, MAX_FACTS, N_ATTRS.

set -euo pipefail
cd "$(dirname "$0")/.."

QPS="${1:-20000}"          # offered transactions/s (open-loop rate)
DUR="${2:-10s}"            # oha duration, e.g. 10s / 1m
PORT="${PORT:-3000}"
DB="${DB:-/tmp/breezy-load.db}"
BODIES="${BODIES:-/tmp/breezy-load-bodies.jsonl}"
BODY_LINES="${BODY_LINES:-2000}"   # distinct random bodies; oha picks one per request
MIN_FACTS="${MIN_FACTS:-2}"
MAX_FACTS="${MAX_FACTS:-20}"
N_ATTRS="${N_ATTRS:-20}"           # node/1 .. node/N, seeded at ids 2..N+1

URL="http://127.0.0.1:${PORT}/tx"
SERVER_LOG="$(dirname "$BODIES")/breezy-load-server.log"

echo "==> build server + seed helper"
cargo build --release --bin breezydb --example seed_node

echo "==> seed $DB (concept node + $N_ATTRS attributes)"
rm -f "$DB" "$DB-wal" "$DB-shm"
./target/release/examples/seed_node "$DB" "$N_ATTRS"

echo "==> generate $BODY_LINES random bodies ($MIN_FACTS..$MAX_FACTS facts each)"
# For each line: pick K uniformly in [min,max], then K distinct attribute ids
# via Fisher-Yates over the pool 2..N_ATTRS+1. Distinct is required: `fact` has
# PK (e, a, tx_from) and one tx stamps a single tx_from, so a transaction may
# touch a given (entity, attr) at most once.
awk -v lines="$BODY_LINES" -v min="$MIN_FACTS" -v max="$MAX_FACTS" -v nattrs="$N_ATTRS" '
BEGIN {
  srand();
  for (l = 0; l < lines; l++) {
    k = min + int(rand() * (max - min + 1));
    for (i = 1; i <= nattrs; i++) pool[i] = i + 1;
    for (i = nattrs; i > 1; i--) {
      j = 1 + int(rand() * i);
      t = pool[i]; pool[i] = pool[j]; pool[j] = t;
    }
    body = "{\"ops\":[{\"op\":\"entity\",\"as\":\"$e\"}";
    for (i = 1; i <= k; i++)
      body = body ",{\"op\":\"add\",\"e\":\"$e\",\"a\":" pool[i] ",\"v\":\"v" int(rand() * 1000000) "\"}";
    print body "]}";
  }
}' > "$BODIES"

echo "==> start server (DB=$DB PORT=$PORT, log: $SERVER_LOG)"
DB="$DB" PORT="$PORT" ./target/release/breezydb >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null || true' EXIT

# Wait until the server accepts requests (it binds IPv4; use 127.0.0.1, not
# localhost, which resolves to ::1 on macOS).
ready=0
for _ in $(seq 1 100); do
  if curl -sf -o /dev/null "http://127.0.0.1:${PORT}/noop"; then ready=1; break; fi
  sleep 0.1
done
if [ "$ready" -ne 1 ]; then
  echo "server did not come up; see $SERVER_LOG" >&2
  exit 1
fi

echo "==> oha open-loop: $QPS offered tx/s for $DUR"
oha -z "$DUR" \
    -c 128 \
    -q "$QPS" \
    --latency-correction \
    --no-tui \
    -m POST -T application/json \
    -Z "$BODIES" \
    "$URL"

echo "==> done"

#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Kannel High-TPS Load Test Pipeline
# Usage: bash test/run_load_test.sh [MSG_COUNT]
# Default: 10000 messages
# ─────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MSG=${1:-10000}

BB="$ROOT/gw/.libs/bearerbox"
DRIVE="$ROOT/test/.libs/drive_smpp"
FAKE="$ROOT/test/.libs/fakesmsc"
CONF="$ROOT/test/drive_smpp_redis.conf"

LOG_DIR="/tmp/kannel_load"
mkdir -p "$LOG_DIR"

BB_LOG="$LOG_DIR/bearerbox.log"
DRIVE_LOG="$LOG_DIR/drive_smpp.log"
FAKE_LOG="$LOG_DIR/fakesmsc.log"

REDIS_DB=1
REDIS_TABLE="kannel_test_store"

# ── helpers ──────────────────────────────────────────────────
log()  { echo "[$(date +%H:%M:%S)] $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }

cleanup() {
    log "Stopping all processes..."
    kill "$BB_PID" "$DRIVE_PID" 2>/dev/null || true
    wait "$BB_PID" "$DRIVE_PID" 2>/dev/null || true
}
trap cleanup EXIT

# ── 1. Preflight ──────────────────────────────────────────────
log "=== Preflight ==="

redis-cli ping &>/dev/null || die "Redis not running — run: brew services start redis"
log "Redis OK"

ulimit -n 65536 2>/dev/null && log "fd limit → 65536" || log "WARN: could not raise fd limit"

# Kill any stale processes on our ports
lsof -ti tcp:2345  | xargs kill -9 2>/dev/null || true
lsof -ti tcp:13000 | xargs kill -9 2>/dev/null || true
lsof -ti tcp:13001 | xargs kill -9 2>/dev/null || true
sleep 0.5

# Flush Redis test DB
redis-cli -n "$REDIS_DB" FLUSHDB &>/dev/null
log "Redis DB $REDIS_DB flushed"

# ── 2. Start drive_smpp (fake SMSC) ──────────────────────────
log "=== Starting drive_smpp (-m $MSG) ==="
"$DRIVE" -m "$MSG" -v 0 > "$DRIVE_LOG" 2>&1 &
DRIVE_PID=$!
log "drive_smpp PID=$DRIVE_PID"
sleep 1  # let it open port 2345

# ── 3. Start bearerbox ────────────────────────────────────────
log "=== Starting bearerbox ==="
"$BB" -v 0 "$CONF" > "$BB_LOG" 2>&1 &
BB_PID=$!
log "bearerbox PID=$BB_PID"

# Wait for SMPP bind
log "Waiting for SMPP bind..."
for i in $(seq 1 20); do
    grep -q "SMPP.*bound\|smpp.*connected\|SMPP.*Connected" "$BB_LOG" 2>/dev/null && break
    sleep 0.5
done
grep -q "SMPP.*bound\|smpp.*connected\|SMPP.*Connected" "$BB_LOG" \
    || log "WARN: SMPP bind not confirmed yet, continuing anyway"
log "bearerbox ready"

# ── 4. Start fakesmsc (MO injector) ──────────────────────────
log "=== Starting fakesmsc (-m $MSG) ==="
START_TS=$(date +%s)
"$FAKE" -H 127.0.0.1 -p 2345 -i 0.0 -m "$MSG" > "$FAKE_LOG" 2>&1 &
FAKE_PID=$!
log "fakesmsc PID=$FAKE_PID"

# ── 5. Monitor progress ───────────────────────────────────────
log "=== Live progress (Ctrl-C to abort) ==="
PREV=0
while kill -0 "$FAKE_PID" 2>/dev/null; do
    STORED=$(redis-cli -n "$REDIS_DB" HLEN "$REDIS_TABLE" 2>/dev/null || echo 0)
    DELTA=$(( STORED - PREV ))
    printf "\r  Redis %-6s msgs stored  |  +%-5s/s   " "$STORED" "$DELTA"
    PREV=$STORED
    sleep 1
done
echo ""

wait "$FAKE_PID" 2>/dev/null || true
END_TS=$(date +%s)
ELAPSED=$(( END_TS - START_TS ))

# Give bearerbox 2s to flush remaining messages
sleep 2

# ── 6. Results ────────────────────────────────────────────────
log "=== Results ==="

FINAL=$(redis-cli -n "$REDIS_DB" HLEN "$REDIS_TABLE" 2>/dev/null || echo 0)
TPS=$(( ELAPSED > 0 ? FINAL / ELAPSED : FINAL ))

echo ""
echo "  Messages sent     : $MSG"
echo "  Messages in Redis : $FINAL"
echo "  Elapsed           : ${ELAPSED}s"
echo "  Avg TPS           : ${TPS} msg/s"
echo ""

# Sample one stored message
SAMPLE_KEY=$(redis-cli -n "$REDIS_DB" HKEYS "$REDIS_TABLE" 2>/dev/null | head -1)
if [[ -n "$SAMPLE_KEY" ]]; then
    echo "  Sample key        : $SAMPLE_KEY"
fi

# Error check
BB_ERRORS=$(grep -c "ERROR\|PANIC\|CRITICAL" "$BB_LOG" 2>/dev/null | awk '{s+=$1} END{print s}')
echo "  bearerbox errors  : $BB_ERRORS"
[[ "$BB_ERRORS" -gt 0 ]] && echo "  → check $BB_LOG"

echo ""
echo "  Logs:"
echo "    bearerbox : $BB_LOG"
echo "    drive_smpp: $DRIVE_LOG"
echo "    fakesmsc  : $FAKE_LOG"
echo ""

# ── 7. Pass/Fail ──────────────────────────────────────────────
if [[ "${FINAL}" -gt 0 && "${BB_ERRORS}" -eq 0 ]]; then
    log "=== PASSED ✅ ==="
else
    log "=== FAILED — stored=${FINAL} errors=${BB_ERRORS} ==="
    exit 1
fi

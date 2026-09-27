#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Kannel Full-Stack Test
# Starts: drive_smpp → bearerbox → smsbox → dlr_receiver
# Sends:  MSG_COUNT MTs via sendsms HTTP API
# Checks: Redis store, Redis DLR table, access log, DLR callbacks
#
# Usage:
#   bash test/run_full_stack_test.sh [MSG_COUNT]
#   Default: 100 messages (use 10000+ for load test)
# ─────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MSG=${1:-100}

BB="$ROOT/gw/.libs/bearerbox"
SMSBOX="$ROOT/gw/.libs/smsbox"
DRIVE="$ROOT/test/.libs/drive_smpp"
CONF="$ROOT/test/full_stack.conf"

LOG_DIR="/tmp/kannel_load"
mkdir -p "$LOG_DIR"

BB_LOG="$LOG_DIR/bearerbox.log"
SB_LOG="$LOG_DIR/smsbox.log"
DRIVE_LOG="$LOG_DIR/drive_smpp.log"
DLR_LOG="$LOG_DIR/dlr_receiver.log"
ACCESS_LOG="$LOG_DIR/access.log"

# ── helpers ──────────────────────────────────────────────────
log()  { echo "[$(date +%H:%M:%S)] $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }

BB_PID="" DRIVE_PID="" SB_PID="" DLR_PID=""

cleanup() {
    log "Stopping all processes..."
    for PID in "$BB_PID" "$SB_PID" "$DRIVE_PID" "$DLR_PID"; do
        [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

# ── 1. Preflight ──────────────────────────────────────────────
log "=== Preflight ==="

redis-cli ping &>/dev/null || die "Redis not running — run: brew services start redis"
log "Redis OK"

command -v python3 &>/dev/null || die "python3 not found"
python3 -c "import aiohttp, redis" 2>/dev/null \
    || die "Missing Python deps — run: pip install aiohttp redis"
log "Python deps OK"

ulimit -n 65536 2>/dev/null && log "fd limit → 65536" || log "WARN: could not raise fd limit"

# Kill stale processes
lsof -ti tcp:2345,13000,13001,13013,8080 | xargs kill -9 2>/dev/null || true
sleep 0.5

# Flush Redis DBs
redis-cli -n 1 FLUSHDB &>/dev/null && log "Redis DB 1 flushed (message store)"
redis-cli -n 2 FLUSHDB &>/dev/null && log "Redis DB 2 flushed (DLR table + stream)"

# Clear old logs
rm -f "$BB_LOG" "$SB_LOG" "$DRIVE_LOG" "$DLR_LOG" "$ACCESS_LOG"

# ── 2. Start DLR receiver ─────────────────────────────────────
log "=== Starting dlr_receiver (port 8080) ==="
python3 "$ROOT/test/dlr_receiver.py" > "$DLR_LOG" 2>&1 &
DLR_PID=$!
log "dlr_receiver PID=$DLR_PID"
sleep 1
kill -0 "$DLR_PID" 2>/dev/null || die "dlr_receiver failed to start — check $DLR_LOG"

# ── 3. Start drive_smpp (fake SMSC) ──────────────────────────
log "=== Starting drive_smpp (-m $MSG) ==="
"$DRIVE" -m "$MSG" -v 0 > "$DRIVE_LOG" 2>&1 &
DRIVE_PID=$!
log "drive_smpp PID=$DRIVE_PID"
sleep 1
kill -0 "$DRIVE_PID" 2>/dev/null || die "drive_smpp failed — check $DRIVE_LOG"

# ── 4. Start bearerbox ────────────────────────────────────────
log "=== Starting bearerbox ==="
"$BB" -v 0 "$CONF" > "$BB_LOG" 2>&1 &
BB_PID=$!
log "bearerbox PID=$BB_PID"

log "Waiting for SMPP bind..."
for i in $(seq 1 30); do
    grep -q "SMPP.*bound\|smpp.*connected\|SMPP.*Connected" "$BB_LOG" 2>/dev/null && break
    sleep 0.5
done
grep -q "SMPP.*bound\|smpp.*connected\|SMPP.*Connected" "$BB_LOG" \
    || log "WARN: SMPP bind not confirmed, continuing"
log "bearerbox ready"

# ── 5. Start smsbox ───────────────────────────────────────────
log "=== Starting smsbox ==="
"$SMSBOX" -v 0 "$CONF" > "$SB_LOG" 2>&1 &
SB_PID=$!
log "smsbox PID=$SB_PID"

log "Waiting for smsbox to connect..."
for i in $(seq 1 20); do
    grep -q "Connected\|smsbox.*ready\|Identified" "$SB_LOG" 2>/dev/null && break
    sleep 0.5
done
log "smsbox ready"

# ── 6. Send MTs via sendsms HTTP API ─────────────────────────
log "=== Sending $MSG MTs via sendsms ==="
DLR_URL="http%3A%2F%2F127.0.0.1%3A8080%2Fdlr%3Fid%3D%25I%26fid%3D%25F%26status%3D%25d%26smsc%3D%25i%26from%3D%25P%26to%3D%25p%26acct%3D%25o"

SENT=0
FAILED_SEND=0
START_TS=$(date +%s)

for i in $(seq 1 "$MSG"); do
    RESP=$(curl -sf --max-time 2 \
        "http://127.0.0.1:13013/cgi-bin/sendsms?username=foo&password=bar\
&to=35850000${i}&text=Test+${i}\
&dlr-url=${DLR_URL}&dlr-mask=31\
&account=test_campaign" 2>/dev/null || echo "FAIL")

    if echo "$RESP" | grep -q "^0:"; then
        SENT=$((SENT + 1))
    else
        FAILED_SEND=$((FAILED_SEND + 1))
    fi

    # progress every 10%
    if (( i % (MSG / 10 + 1) == 0 )); then
        printf "\r  Sent %-5s / %-5s  failed=%-3s" "$SENT" "$MSG" "$FAILED_SEND"
    fi
done
echo ""
END_TS=$(date +%s)
ELAPSED=$(( END_TS - START_TS ))
log "Sent $SENT / $MSG  failed=$FAILED_SEND  elapsed=${ELAPSED}s"

# ── 7. Wait for DLR callbacks ─────────────────────────────────
log "=== Waiting for DLR callbacks (up to 15s) ==="
for i in $(seq 1 15); do
    DLR_QUEUED=$(redis-cli -n 2 XLEN dlr_stream 2>/dev/null || echo 0)
    STORE_COUNT=$(redis-cli -n 1 HLEN kannel_test_store 2>/dev/null || echo 0)
    printf "\r  store=%-5s  dlr_stream=%-5s" "$STORE_COUNT" "$DLR_QUEUED"
    sleep 1
done
echo ""

# ── 8. Results ────────────────────────────────────────────────
log "=== Results ==="

STORE_FINAL=$(redis-cli -n 1 HLEN kannel_test_store 2>/dev/null || echo 0)
DLR_KEYS=$(redis-cli -n 2 KEYS "kannel_dlr:*" 2>/dev/null | wc -l | tr -d ' ')
DLR_STREAM=$(redis-cli -n 2 XLEN dlr_stream 2>/dev/null || echo 0)
SAMPLE_KEY=$(redis-cli -n 1 HKEYS kannel_test_store 2>/dev/null | head -1)

BB_ERRORS=$(grep -c "ERROR\|PANIC\|CRITICAL" "$BB_LOG" 2>/dev/null | awk '{s+=$1} END{print s+0}')
SB_ERRORS=$(grep -c "ERROR\|PANIC\|CRITICAL" "$SB_LOG" 2>/dev/null | awk '{s+=$1} END{print s+0}')
ACCESS_LINES=$(wc -l < "$ACCESS_LOG" 2>/dev/null || echo 0)

echo ""
printf "  %-28s %s\n" "MTs sent via sendsms:"    "$SENT / $MSG"
printf "  %-28s %s\n" "Messages in Redis store:" "$STORE_FINAL"
printf "  %-28s %s\n" "DLR entries in Redis:"    "$DLR_KEYS"
printf "  %-28s %s\n" "DLR callbacks queued:"    "$DLR_STREAM"
printf "  %-28s %s\n" "Access log lines:"        "$ACCESS_LINES"
printf "  %-28s %s\n" "bearerbox errors:"        "$BB_ERRORS"
printf "  %-28s %s\n" "smsbox errors:"           "$SB_ERRORS"
[[ -n "$SAMPLE_KEY" ]] && printf "  %-28s %s\n" "Sample store key:" "$SAMPLE_KEY"
echo ""

# ── 9. Access log sample ──────────────────────────────────────
if [[ -f "$ACCESS_LOG" && "$ACCESS_LINES" -gt 0 ]]; then
    log "=== Access log sample (last 3 lines) ==="
    tail -3 "$ACCESS_LOG"
    echo ""
fi

# ── 10. Redis DLR sample ──────────────────────────────────────
SAMPLE_DLR=$(redis-cli -n 2 KEYS "kannel_dlr:*" 2>/dev/null | head -1)
if [[ -n "$SAMPLE_DLR" ]]; then
    log "=== DLR Redis sample ==="
    echo "  Key: $SAMPLE_DLR"
    redis-cli -n 2 HGETALL "$SAMPLE_DLR" 2>/dev/null | paste - - | head -10
    echo ""
fi

echo "  Logs:"
echo "    bearerbox   : $BB_LOG"
echo "    smsbox      : $SB_LOG"
echo "    drive_smpp  : $DRIVE_LOG"
echo "    dlr_receiver: $DLR_LOG"
echo "    access log  : $ACCESS_LOG"
echo ""

# ── 11. Pass/Fail ─────────────────────────────────────────────
PASS=1
[[ "$SENT"        -eq 0 ]] && { log "FAIL: no MTs sent";              PASS=0; }
[[ "$BB_ERRORS"   -gt 0 ]] && { log "FAIL: bearerbox errors";         PASS=0; }
[[ "$SB_ERRORS"   -gt 0 ]] && { log "FAIL: smsbox errors";            PASS=0; }
[[ "$ACCESS_LINES" -eq 0 ]] && { log "FAIL: access log empty";        PASS=0; }

if [[ "$PASS" -eq 1 ]]; then
    log "=== PASSED ✅ ==="
else
    log "=== FAILED ❌ ==="
    exit 1
fi

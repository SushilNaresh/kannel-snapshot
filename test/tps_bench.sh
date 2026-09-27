#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Kannel End-to-End TPS Benchmark
# Usage: bash test/tps_bench.sh [MSG_COUNT] [CONCURRENCY]
# Defaults: 10000 messages, 50 parallel workers
# ─────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MSG=${1:-10000}
CONCURRENCY=${2:-50}

BB="$ROOT/gw/.libs/bearerbox"
SMSBOX="$ROOT/gw/.libs/smsbox"
DRIVE="$ROOT/test/.libs/drive_smpp"
CONF="$ROOT/test/full_stack.conf"

LOG_DIR="/tmp/kannel_bench"
mkdir -p "$LOG_DIR"

BB_LOG="$LOG_DIR/bearerbox.log"
DRIVE_LOG="$LOG_DIR/drive_smpp.log"
DLR_LOG="$LOG_DIR/dlr_receiver.log"
ACCESS_LOG="$LOG_DIR/access.log"
SEND_LOG="$LOG_DIR/send_results.log"
REPORT="$LOG_DIR/tps_report.txt"

NUM_SMSBOX=8
SMSBOX_BASE_PORT=13013
SMSBOX_PORTS=""
for _i in $(seq 1 $NUM_SMSBOX); do
    SMPORT=$(( SMSBOX_BASE_PORT + _i - 1 ))
    SMSBOX_PORTS="${SMSBOX_PORTS}${SMSBOX_PORTS:+,}${SMPORT}"
done

CONF_RUNTIME="$LOG_DIR/full_stack_bench.conf"
sed "s|/tmp/kannel_load|$LOG_DIR|g" "$CONF" > "$CONF_RUNTIME"

log() { echo "[$(date +%H:%M:%S)] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

BB_PID="" DRIVE_PID="" DLR_PID=""
SB_PIDS=()   # array of all smsbox PIDs

cleanup() {
    log "Stopping all processes..."
    for PID in "$BB_PID" "$DRIVE_PID" "$DLR_PID" "${SB_PIDS[@]:-}"; do
        [[ -n "$PID" ]] && kill "$PID" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT

# ── 1. Preflight ──────────────────────────────────────────────
log "=== Preflight ==="
redis-cli ping &>/dev/null || die "Redis not running"
command -v python3 &>/dev/null || die "python3 not found"
python3 -c "import aiohttp, redis" 2>/dev/null || die "pip install aiohttp redis"

ulimit -n 65536 2>/dev/null || log "WARN: could not raise fd limit"

KILL_PORTS="2345,13000,13001"
for _i in $(seq 1 $NUM_SMSBOX); do
    KILL_PORTS="${KILL_PORTS},$(( SMSBOX_BASE_PORT + _i - 1 ))"
done
lsof -ti tcp:${KILL_PORTS},8080 | xargs kill -9 2>/dev/null || true
sleep 0.5

redis-cli -n 1 FLUSHDB &>/dev/null
redis-cli -n 2 FLUSHDB &>/dev/null
rm -f "$BB_LOG" "$DRIVE_LOG" "$DLR_LOG" "$ACCESS_LOG" "$SEND_LOG"
for _i in $(seq 1 $NUM_SMSBOX); do rm -f "$LOG_DIR/smsbox${_i}.log"; done

log "MSG=$MSG  CONCURRENCY=$CONCURRENCY  SMSBOX_INSTANCES=$NUM_SMSBOX  PORTS=$SMSBOX_PORTS"

# ── 2. Start DLR receiver ─────────────────────────────────────
log "=== Starting dlr_receiver ==="
python3 "$ROOT/test/dlr_receiver.py" > "$DLR_LOG" 2>&1 &
DLR_PID=$!
sleep 1
kill -0 "$DLR_PID" 2>/dev/null || die "dlr_receiver failed — check $DLR_LOG"

# ── 3. Start drive_smpp ───────────────────────────────────────
log "=== Starting drive_smpp (-m $MSG) ==="
"$DRIVE" -m "$MSG" -v 0 > "$DRIVE_LOG" 2>&1 &
DRIVE_PID=$!
sleep 1
kill -0 "$DRIVE_PID" 2>/dev/null || die "drive_smpp failed"

# ── 4. Start bearerbox ────────────────────────────────────────
log "=== Starting bearerbox ==="
"$BB" -v 0 "$CONF_RUNTIME" > "$BB_LOG" 2>&1 &
BB_PID=$!

for i in $(seq 1 40); do
    grep -q "bind_transmitter_resp\|bind_receiver_resp\|bind_transceiver_resp" "$BB_LOG" 2>/dev/null && break
    sleep 0.5
done
grep -q "bind_transmitter_resp\|bind_receiver_resp\|bind_transceiver_resp" "$BB_LOG" \
    || log "WARN: SMPP bind not confirmed — check $BB_LOG"
log "bearerbox ready"

# ── 5. Start 8 smsbox instances ──────────────────────────────
log "=== Starting $NUM_SMSBOX smsbox instances ==="
for _i in $(seq 1 $NUM_SMSBOX); do
    _port=$(( SMSBOX_BASE_PORT + _i - 1 ))
    _id="smsbox${_i}"
    _log="$LOG_DIR/${_id}.log"
    # Generate a per-instance config: replace smsbox1/13013 placeholders
    _conf="$LOG_DIR/${_id}.conf"
    sed -e "s|smsbox-id = smsbox1|smsbox-id = ${_id}|" \
        -e "s|sendsms-port = 13013|sendsms-port = ${_port}|" \
        -e "s|smsbox1.log|${_id}.log|" \
        "$CONF_RUNTIME" > "$_conf"
    "$SMSBOX" -v 0 "$_conf" > "$_log" 2>&1 &
    _pid=$!
    SB_PIDS+=($_pid)
    log "  smsbox${_i} PID=${_pid} port=${_port}"
done
# Wait for all instances to identify to bearerbox
for _i in $(seq 1 $NUM_SMSBOX); do
    _log="$LOG_DIR/smsbox${_i}.log"
    for j in $(seq 1 40); do
        grep -q "Connected\|Identified" "$_log" 2>/dev/null && break
        sleep 0.3
    done
    grep -q "Connected\|Identified" "$_log" 2>/dev/null \
        || log "WARN: smsbox${_i} did not connect — check $_log"
done
log "$NUM_SMSBOX smsbox instances ready"

# ── 6. Send MTs via Python async sender ───────────────────────
log "=== Sending $MSG MTs with concurrency=$CONCURRENCY ==="

SEND_START=$(date +%s)
python3 "$ROOT/test/send_mts.py" "$MSG" "$CONCURRENCY" "$SMSBOX_PORTS" 2>"$SEND_LOG" || true
SEND_END=$(date +%s)
SEND_SEC=$(( SEND_END - SEND_START ))
[[ $SEND_SEC -eq 0 ]] && SEND_SEC=1

ACCEPTED=$(grep -o "accepted=[0-9]*"   "$SEND_LOG" | cut -d= -f2 || echo 0)
REJECTED=$(grep -o "rejected=[0-9]*"   "$SEND_LOG" | cut -d= -f2 || echo 0)
SEND_ERRORS=$(grep -o "errors=[0-9]*"  "$SEND_LOG" | cut -d= -f2 || echo 0)
SEND_TPS=$(grep -o "tps=[0-9]*"        "$SEND_LOG" | cut -d= -f2 || echo 0)
SEND_ELAPSED=$(grep -o "elapsed=[0-9.]*" "$SEND_LOG" | cut -d= -f2 || echo "$SEND_SEC")

log "Send phase done: accepted=$ACCEPTED rejected=$REJECTED errors=$SEND_ERRORS tps=${SEND_TPS}/s elapsed=${SEND_ELAPSED}s"

# ── 7. Monitor Redis store — track peak fill level ────────────
# The store is a crash-recovery buffer: bearerbox writes on submit,
# deletes on delivery ACK. Peak HLEN = max in-flight at any instant.
# We sample every 0.2s for 30s to catch the peak accurately.
log "=== Monitoring Redis store peak (30s) ==="

PEAK_STORE=0
MONITOR_START=$(date +%s)
SETTLED=0

for i in $(seq 1 150); do
    sleep 0.2
    NOW=$(redis-cli -n 1 HLEN kannel_test_store 2>/dev/null || echo 0)
    [[ $NOW -gt $PEAK_STORE ]] && PEAK_STORE=$NOW
    printf "\r  store=%-6s  peak=%-6s" "$NOW" "$PEAK_STORE"
    # Stop early once store has been zero for 3 consecutive checks after activity
    if [[ $PEAK_STORE -gt 0 && $NOW -eq 0 ]]; then
        SETTLED=$(( SETTLED + 1 ))
        [[ $SETTLED -ge 15 ]] && break
    else
        SETTLED=0
    fi
done
echo ""

MONITOR_END=$(date +%s)
MONITOR_ELAPSED=$(( MONITOR_END - MONITOR_START ))

# ── 8. Wait for DLR callbacks (up to 15s) ────────────────────
log "=== Waiting for DLR callbacks ==="
for i in $(seq 1 15); do
    DLR_STREAM=$(redis-cli -n 2 XLEN dlr_stream 2>/dev/null || echo 0)
    STORE_NOW=$(redis-cli -n 1 HLEN kannel_test_store 2>/dev/null || echo 0)
    printf "\r  store=%-6s  dlr_stream=%-6s" "$STORE_NOW" "$DLR_STREAM"
    sleep 1
done
echo ""

# ── 9. Final metrics ──────────────────────────────────────────
STORE_FINAL=$(redis-cli -n 1 HLEN kannel_test_store 2>/dev/null || echo 0)
DLR_KEYS=$(redis-cli -n 2 KEYS "kannel_dlr:*" 2>/dev/null | wc -l | tr -d ' ')
DLR_STREAM_FINAL=$(redis-cli -n 2 XLEN dlr_stream 2>/dev/null || echo 0)
ACCESS_LINES=$(wc -l < "$ACCESS_LOG" 2>/dev/null || echo 0)

BB_ERRORS=$(( $(grep "ERROR\|PANIC\|CRITICAL" "$BB_LOG" 2>/dev/null \
    | grep -vc "SIGINT\|aborting program\|shutting down\|no.*message_id" 2>/dev/null) + 0 )) || BB_ERRORS=0
# Aggregate errors across all smsbox logs
SB_ERRORS=0
for _i in $(seq 1 $NUM_SMSBOX); do
    _e=$(( $(grep "ERROR\|PANIC\|CRITICAL" "$LOG_DIR/smsbox${_i}.log" 2>/dev/null \
        | grep -vc "SIGINT\|aborting program\|shutting down" 2>/dev/null) + 0 )) || _e=0
    SB_ERRORS=$(( SB_ERRORS + _e ))
done

DLR_TPS_LINE=$(grep "recv_tps=" "$DLR_LOG" 2>/dev/null | tail -1 || echo "")
DLR_AVG_TPS=$(echo "$DLR_TPS_LINE" | grep -o "recv_tps=[0-9]*" | cut -d= -f2 2>/dev/null || echo "")
[[ -z "$DLR_AVG_TPS" ]] && DLR_AVG_TPS="N/A"

# ── 10. Write report ──────────────────────────────────────────
{
cat <<'HDR'
===============================================================
  KANNEL END-TO-END TPS BENCHMARK REPORT
HDR
echo "  $(date)"
cat <<'HDR'
===============================================================

  TEST PARAMETERS
  -------------------------------------------------------------
HDR
printf "  %-28s %s\n" "Messages requested:"    "$MSG"
printf "  %-28s %s\n" "Parallel send workers:" "$CONCURRENCY"
printf "  %-28s %s\n" "Config:"                "$CONF_RUNTIME"
printf "  %-28s %s\n" "smsbox instances:"      "$NUM_SMSBOX (ports $SMSBOX_PORTS)"
cat <<'HDR'

  SEND PHASE  (sendsms HTTP API -> smsbox)
  -------------------------------------------------------------
HDR
printf "  %-28s %s\n" "MTs accepted:"   "$ACCEPTED / $MSG"
printf "  %-28s %s\n" "MTs rejected:"   "$REJECTED"
printf "  %-28s %s\n" "Send errors:"    "$SEND_ERRORS"
printf "  %-28s %s\n" "Send elapsed:"   "${SEND_ELAPSED}s"
printf "  %-28s %s\n" "Send TPS:"       "${SEND_TPS} msg/s"
cat <<'HDR'

  BEARERBOX PROCESSING  (crash-recovery store peak)
  -------------------------------------------------------------
HDR
printf "  %-28s %s\n" "Peak msgs in store:"  "$PEAK_STORE  (max in-flight)"
printf "  %-28s %s\n" "Store at end:"        "$STORE_FINAL  (0=all delivered+removed)"
printf "  %-28s %s\n" "Monitor window:"      "${MONITOR_ELAPSED}s"
cat <<'HDR'

  DLR PIPELINE
  -------------------------------------------------------------
HDR
printf "  %-28s %s\n" "DLR entries in Redis:"    "$DLR_KEYS"
printf "  %-28s %s\n" "DLR callbacks queued:"    "$DLR_STREAM_FINAL / $ACCEPTED"
printf "  %-28s %s\n" "DLR receiver avg TPS:"    "${DLR_AVG_TPS} callbacks/s"
cat <<'HDR'

  ACCESS LOG
  -------------------------------------------------------------
HDR
printf "  %-28s %s\n" "Access log lines:" "$ACCESS_LINES"
cat <<'HDR'

  ERRORS
  -------------------------------------------------------------
HDR
printf "  %-28s %s\n" "bearerbox errors:" "$BB_ERRORS"
printf "  %-28s %s\n" "smsbox errors:"    "$SB_ERRORS"
echo ""
echo "  BOTTLENECK ANALYSIS"
echo "  -------------------------------------------------------------"

if [[ $REJECTED -gt 0 ]]; then
    echo "  WARNING: $REJECTED MTs rejected — smsbox queue full or max-messages limit"
fi
if [[ $DLR_STREAM_FINAL -eq $ACCEPTED && $BB_ERRORS -eq 0 && $SB_ERRORS -eq 0 ]]; then
    echo "  OK: All $ACCEPTED MTs processed end-to-end — DLR callbacks match accepted count"
elif [[ $DLR_STREAM_FINAL -lt $ACCEPTED ]]; then
    MISSING=$(( ACCEPTED - DLR_STREAM_FINAL ))
    echo "  WARNING: $MISSING DLR callbacks missing (expected $ACCEPTED, got $DLR_STREAM_FINAL)"
fi
if [[ $SEND_TPS -lt 500 && $MSG -ge 1000 ]]; then
    echo "  WARNING: Send TPS < 500 — smsbox or network bottleneck"
fi
if [[ $BB_ERRORS -gt 0 ]]; then
    echo "  FAIL: bearerbox errors — check $BB_LOG"
fi
if [[ $STORE_FINAL -gt 0 ]]; then
    echo "  INFO: $STORE_FINAL messages still in crash-recovery store (in-flight at shutdown)"
fi
echo "  INFO: $DLR_STREAM_FINAL DLR callbacks pending in stream (dlr_writer not running — expected)"

echo ""
echo "  LOG FILES"
echo "  -------------------------------------------------------------"
printf "  %-14s %s\n" "bearerbox:"    "$BB_LOG"
for _i in $(seq 1 $NUM_SMSBOX); do
    printf "  %-14s %s\n" "smsbox${_i}:" "$LOG_DIR/smsbox${_i}.log"
done
printf "  %-14s %s\n" "drive_smpp:"   "$DRIVE_LOG"
printf "  %-14s %s\n" "dlr_receiver:" "$DLR_LOG"
printf "  %-14s %s\n" "access log:"   "$ACCESS_LOG"
printf "  %-14s %s\n" "send results:" "$SEND_LOG"
printf "  %-14s %s\n" "this report:"  "$REPORT"
echo "==============================================================="
} | tee "$REPORT"

# ── 11. Pass/Fail ─────────────────────────────────────────────
# STORE_FINAL=0 is expected: bearerbox removes msgs from crash-recovery
# store after delivery ACK. Use send TPS + DLR count as success indicators.
PASS=1
[[ "$ACCEPTED"        -eq 0 ]] && { log "FAIL: no MTs accepted";                    PASS=0; }
[[ "$BB_ERRORS"       -gt 0 ]] && { log "FAIL: bearerbox errors";                   PASS=0; }
[[ "$SB_ERRORS"       -gt 0 ]] && { log "FAIL: smsbox errors ($SB_ERRORS)";          PASS=0; }
[[ "$DLR_STREAM_FINAL" -eq 0 && "$ACCEPTED" -gt 0 ]] && { log "FAIL: no DLR callbacks received"; PASS=0; }

if [[ "$PASS" -eq 1 ]]; then
    log "=== PASSED  Report: $REPORT ==="
else
    log "=== FAILED  Report: $REPORT ==="
    exit 1
fi

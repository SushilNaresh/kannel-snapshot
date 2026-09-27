#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Kannel Sustained TPS Benchmark
# Tests whether 500 DLR TPS is stable over a long run.
#
# Usage: bash test/sustained_bench.sh [MSG_COUNT] [CONCURRENCY]
# Defaults: 50000 messages, 200 workers
# ─────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MSG=${1:-50000}
CONCURRENCY=${2:-200}

BB="$ROOT/gw/.libs/bearerbox"
SMSBOX="$ROOT/gw/.libs/smsbox"
DRIVE="$ROOT/test/.libs/drive_smpp"
CONF="$ROOT/test/full_stack.conf"

LOG_DIR="/tmp/kannel_sustained"
mkdir -p "$LOG_DIR"

BB_LOG="$LOG_DIR/bearerbox.log"
SB_LOG="$LOG_DIR/smsbox1.log"
DRIVE_LOG="$LOG_DIR/drive_smpp.log"
DLR_LOG="$LOG_DIR/dlr_receiver.log"
ACCESS_LOG="$LOG_DIR/access.log"
SEND_LOG="$LOG_DIR/send_results.log"
SAMPLES_LOG="$LOG_DIR/tps_samples.log"
REPORT="$LOG_DIR/sustained_report.txt"

CONF_RUNTIME="$LOG_DIR/full_stack_sustained.conf"
sed "s|/tmp/kannel_load|$LOG_DIR|g" "$CONF" > "$CONF_RUNTIME"

log() { echo "[$(date +%H:%M:%S)] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

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
redis-cli ping &>/dev/null || die "Redis not running"
command -v python3 &>/dev/null || die "python3 not found"
python3 -c "import aiohttp, redis" 2>/dev/null || die "pip install aiohttp redis"

ulimit -n 65536 2>/dev/null || log "WARN: could not raise fd limit"

KILL_PORTS="2345,13000,13001,13013,8080"
lsof -ti tcp:${KILL_PORTS} | xargs kill -9 2>/dev/null || true
sleep 0.5

redis-cli -n 1 FLUSHDB &>/dev/null
redis-cli -n 2 FLUSHDB &>/dev/null
rm -f "$BB_LOG" "$SB_LOG" "$DRIVE_LOG" "$DLR_LOG" "$ACCESS_LOG" "$SEND_LOG" "$SAMPLES_LOG"

log "MSG=$MSG  CONCURRENCY=$CONCURRENCY  TARGET=500 DLR TPS sustained"

# ── 2. Start DLR receiver ─────────────────────────────────────
log "=== Starting dlr_receiver ==="
python3 "$ROOT/test/dlr_receiver.py" > "$DLR_LOG" 2>&1 &
DLR_PID=$!
sleep 1
kill -0 "$DLR_PID" 2>/dev/null || die "dlr_receiver failed"

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
    grep -q "bind_transmitter_resp\|bind_receiver_resp" "$BB_LOG" 2>/dev/null && break
    sleep 0.5
done
grep -q "bind_transmitter_resp\|bind_receiver_resp" "$BB_LOG" \
    || log "WARN: SMPP bind not confirmed"
log "bearerbox ready"

# ── 5. Start smsbox ───────────────────────────────────────────
log "=== Starting smsbox (smsbox-max-pending=500, max-pending-requests=512) ==="
"$SMSBOX" -v 0 "$CONF_RUNTIME" > "$SB_LOG" 2>&1 &
SB_PID=$!
for i in $(seq 1 20); do
    grep -q "Connected\|Identified" "$SB_LOG" 2>/dev/null && break
    sleep 0.5
done
log "smsbox ready"

# ── 6. Send MTs in background ─────────────────────────────────
log "=== Sending $MSG MTs (background, concurrency=$CONCURRENCY) ==="
python3 "$ROOT/test/send_mts.py" "$MSG" "$CONCURRENCY" "13013" 2>"$SEND_LOG" &
SEND_PID=$!

# ── 7. Sample DLR stream every 5s for sustained TPS measurement ──
log "=== Sampling DLR TPS every 5s (target: 500/s sustained) ==="
echo "# elapsed_s  dlr_total  dlr_delta  dlr_tps  store_depth" > "$SAMPLES_LOG"

PREV_DLR=0
SAMPLE_START=$(date +%s)
PEAK_TPS=0
MIN_TPS=999999
SAMPLE_NUM=0
TPS_SUM=0
BELOW_TARGET=0   # count of samples below 400/s (80% of target)

while kill -0 "$SEND_PID" 2>/dev/null; do
    sleep 5
    NOW_DLR=$(redis-cli -n 2 XLEN dlr_stream 2>/dev/null || echo 0)
    NOW_STORE=$(redis-cli -n 1 HLEN kannel_test_store 2>/dev/null || echo 0)
    ELAPSED=$(( $(date +%s) - SAMPLE_START ))
    DELTA=$(( NOW_DLR - PREV_DLR ))
    TPS=$(( DELTA / 5 ))
    PREV_DLR=$NOW_DLR
    SAMPLE_NUM=$(( SAMPLE_NUM + 1 ))
    TPS_SUM=$(( TPS_SUM + TPS ))
    [[ $TPS -gt $PEAK_TPS ]] && PEAK_TPS=$TPS
    [[ $TPS -lt $MIN_TPS && $SAMPLE_NUM -gt 1 ]] && MIN_TPS=$TPS
    [[ $TPS -lt 400 && $SAMPLE_NUM -gt 1 ]] && BELOW_TARGET=$(( BELOW_TARGET + 1 ))

    printf "  t=%-4ss  dlr=%-6s  +%-5s  tps=%-5s/s  store=%-6s\n" \
        "$ELAPSED" "$NOW_DLR" "$DELTA" "$TPS" "$NOW_STORE" | tee -a "$SAMPLES_LOG"
done

wait "$SEND_PID" 2>/dev/null || true
TOTAL_ELAPSED=$(( $(date +%s) - SAMPLE_START ))

# Final drain wait — keep sampling TPS until all DLRs arrive or 60s timeout
log "=== Waiting for final DLR callbacks (up to 60s) ==="
for i in $(seq 1 60); do
    DLR_NOW=$(redis-cli -n 2 XLEN dlr_stream 2>/dev/null || echo 0)
    STORE_NOW=$(redis-cli -n 1 HLEN kannel_test_store 2>/dev/null || echo 0)
    printf "\r  dlr_stream=%-6s  store=%-6s" "$DLR_NOW" "$STORE_NOW"
    # Sample TPS every 5s during drain too
    if (( i % 5 == 0 )); then
        DELTA=$(( DLR_NOW - PREV_DLR ))
        TPS=$(( DELTA / 5 ))
        PREV_DLR=$DLR_NOW
        ELAPSED=$(( $(date +%s) - SAMPLE_START ))
        SAMPLE_NUM=$(( SAMPLE_NUM + 1 ))
        TPS_SUM=$(( TPS_SUM + TPS ))
        [[ $TPS -gt $PEAK_TPS ]] && PEAK_TPS=$TPS
        [[ $TPS -lt $MIN_TPS && $TPS -gt 0 ]] && MIN_TPS=$TPS
        [[ $TPS -lt 400 && $TPS -gt 0 ]] && BELOW_TARGET=$(( BELOW_TARGET + 1 ))
        printf "\n  t=%-4ss  dlr=%-6s  +%-5s  tps=%-5s/s  store=%-6s\n" \
            "$ELAPSED" "$DLR_NOW" "$DELTA" "$TPS" "$STORE_NOW" | tee -a "$SAMPLES_LOG"
    fi
    [[ $DLR_NOW -ge $MSG ]] && break || true
    sleep 1
done
echo ""

# ── 8. Final metrics ──────────────────────────────────────────
ACCEPTED=$(grep -o "accepted=[0-9]*"   "$SEND_LOG" | cut -d= -f2 || echo 0)
REJECTED=$(grep -o "rejected=[0-9]*"   "$SEND_LOG" | cut -d= -f2 || echo 0)
SEND_TPS=$(grep -o "tps=[0-9]*"        "$SEND_LOG" | cut -d= -f2 || echo 0)
SEND_ELAPSED=$(grep -o "elapsed=[0-9.]*" "$SEND_LOG" | cut -d= -f2 || echo "$TOTAL_ELAPSED")

DLR_FINAL=$(redis-cli -n 2 XLEN dlr_stream 2>/dev/null || echo 0)
STORE_FINAL=$(redis-cli -n 1 HLEN kannel_test_store 2>/dev/null || echo 0)
ACCESS_LINES=$(wc -l < "$ACCESS_LOG" 2>/dev/null || echo 0)

BB_ERRORS=$(( $(grep "ERROR\|PANIC\|CRITICAL" "$BB_LOG" 2>/dev/null \
    | grep -vc "SIGINT\|aborting program\|shutting down\|no.*message_id" 2>/dev/null) + 0 )) || BB_ERRORS=0
SB_ERRORS=$(( $(grep "ERROR\|PANIC\|CRITICAL" "$SB_LOG" 2>/dev/null \
    | grep -vc "SIGINT\|aborting program\|shutting down" 2>/dev/null) + 0 )) || SB_ERRORS=0

AVG_TPS=0
[[ $SAMPLE_NUM -gt 0 ]] && AVG_TPS=$(( TPS_SUM / SAMPLE_NUM ))
[[ $MIN_TPS -eq 999999 ]] && MIN_TPS=0

OVERALL_DLR_TPS=0
[[ $TOTAL_ELAPSED -gt 0 ]] && OVERALL_DLR_TPS=$(( DLR_FINAL / TOTAL_ELAPSED ))

# ── 9. Report ─────────────────────────────────────────────────
{
cat <<'HDR'
===============================================================
  KANNEL SUSTAINED TPS BENCHMARK REPORT
HDR
echo "  $(date)"
cat <<'HDR'
===============================================================

  CONFIGURATION
  -------------------------------------------------------------
HDR
printf "  %-32s %s\n" "smsbox-max-pending (core):"    "500"
printf "  %-32s %s\n" "max-pending-requests (smsbox):" "512"
printf "  %-32s %s\n" "max-pending-submits (smsc):"   "100"
printf "  %-32s %s\n" "http-timeout (smsbox):"        "10s"
printf "  %-32s %s\n" "http-request-retry (smsbox):"  "0"
printf "  %-32s %s\n" "Redis max-connections:"        "10 (store) + 10 (DLR)"
echo ""
cat <<'HDR'
  SEND PHASE
  -------------------------------------------------------------
HDR
printf "  %-32s %s\n" "Messages sent:"       "$ACCEPTED / $MSG"
printf "  %-32s %s\n" "Rejected:"            "$REJECTED"
printf "  %-32s %s\n" "Send elapsed:"        "${SEND_ELAPSED}s"
printf "  %-32s %s\n" "Send TPS:"            "${SEND_TPS} msg/s"
echo ""
cat <<'HDR'
  DLR PIPELINE — SUSTAINED TPS
  -------------------------------------------------------------
HDR
printf "  %-32s %s\n" "Total DLR callbacks:"    "$DLR_FINAL / $ACCEPTED"
printf "  %-32s %s\n" "Overall DLR TPS:"        "${OVERALL_DLR_TPS} callbacks/s"
printf "  %-32s %s\n" "Peak 5s-window TPS:"     "${PEAK_TPS} callbacks/s"
printf "  %-32s %s\n" "Min 5s-window TPS:"      "${MIN_TPS} callbacks/s"
printf "  %-32s %s\n" "Avg 5s-window TPS:"      "${AVG_TPS} callbacks/s"
printf "  %-32s %s\n" "Samples below 400/s:"    "${BELOW_TARGET} / ${SAMPLE_NUM}"
printf "  %-32s %s\n" "Total elapsed:"          "${TOTAL_ELAPSED}s"
echo ""
cat <<'HDR'
  STABILITY VERDICT
  -------------------------------------------------------------
HDR
if [[ $BELOW_TARGET -eq 0 && $MIN_TPS -ge 400 ]]; then
    echo "  STABLE: All samples >= 400/s — 500 TPS target is sustainable"
elif [[ $BELOW_TARGET -le 2 ]]; then
    echo "  MOSTLY STABLE: $BELOW_TARGET sample(s) dipped below 400/s (transient)"
else
    echo "  UNSTABLE: $BELOW_TARGET / $SAMPLE_NUM samples below 400/s — not sustainable at 500 TPS"
fi
[[ $BB_ERRORS -gt 0 ]] && echo "  FAIL: $BB_ERRORS bearerbox errors — check $BB_LOG"
[[ $SB_ERRORS -gt 0 ]] && echo "  FAIL: $SB_ERRORS smsbox errors — check $SB_LOG"
[[ $REJECTED -gt 0 ]]  && echo "  WARN: $REJECTED MTs rejected"
echo ""
cat <<'HDR'
  ACCESS LOG
  -------------------------------------------------------------
HDR
printf "  %-32s %s\n" "Access log lines:" "$ACCESS_LINES"
echo ""
cat <<'HDR'
  ERRORS
  -------------------------------------------------------------
HDR
printf "  %-32s %s\n" "bearerbox errors:" "$BB_ERRORS"
printf "  %-32s %s\n" "smsbox errors:"    "$SB_ERRORS"
echo ""
echo "  TPS SAMPLES (5s windows)"
echo "  -------------------------------------------------------------"
grep -v "^#" "$SAMPLES_LOG" | head -60
echo ""
echo "  LOG FILES"
echo "  -------------------------------------------------------------"
printf "  %-14s %s\n" "bearerbox:"    "$BB_LOG"
printf "  %-14s %s\n" "smsbox:"       "$SB_LOG"
printf "  %-14s %s\n" "drive_smpp:"   "$DRIVE_LOG"
printf "  %-14s %s\n" "dlr_receiver:" "$DLR_LOG"
printf "  %-14s %s\n" "access log:"   "$ACCESS_LOG"
printf "  %-14s %s\n" "tps samples:"  "$SAMPLES_LOG"
printf "  %-14s %s\n" "this report:"  "$REPORT"
echo "==============================================================="
} | tee "$REPORT"

# ── 10. Pass/Fail ─────────────────────────────────────────────
PASS=1
[[ "$ACCEPTED"   -eq 0 ]] && { log "FAIL: no MTs accepted";   PASS=0; }
[[ "$BB_ERRORS"  -gt 0 ]] && { log "FAIL: bearerbox errors";  PASS=0; }
[[ "$SB_ERRORS"  -gt 0 ]] && { log "FAIL: smsbox errors";     PASS=0; }
[[ "$DLR_FINAL"  -eq 0 ]] && { log "FAIL: no DLR callbacks";  PASS=0; }

if [[ "$PASS" -eq 1 ]]; then
    log "=== PASSED  Report: $REPORT ==="
else
    log "=== FAILED  Report: $REPORT ==="
    exit 1
fi

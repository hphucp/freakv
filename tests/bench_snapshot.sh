#!/usr/bin/env bash
# bench_snapshot.sh — Asynchronous Snapshot Performance & Latency Stress Test
# Tests system stability and p99 latency during heavy background I/O.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}╔══ BENCH: ASYNCHRONOUS SNAPSHOT STRESS ══╗${RST}\n\n"

# Configuration
WARMUP_OPS=50000
STRESS_OPS=40000000
KEYSPACE=2000000
CLIENTS=50
PIPELINE=32
VALUE_SIZE=128

# Handle engine-specific intervals (FreaKV: ms, Redis: sec, Dragonfly: min/cron)
if [[ "$ENGINE" == "redis" ]]; then
  SNAP_INTERVAL=1
  SNAP_EXT="rdb"
elif [[ "$ENGINE" == "dragonfly" ]]; then
  SNAP_INTERVAL=1 # cron will fire at next minute
  SNAP_EXT="rdb"
else
  SNAP_INTERVAL=5000
  SNAP_EXT="fsnap"
fi

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}Testing Engine: $ENGINE${RST}\n\n"

# ════════════════════════════════════════════════════════════════════
_run_bench_stress() {
  local label="$1"
  local raw

  printf "[stress] %-30s ... running\r" "$label"

  # For Redis/Dragonfly, trigger a manual background save right before the bench
  # to ensure background I/O pressure is active during the run.
  if [[ "$ENGINE" == "redis" || "$ENGINE" == "dragonfly" ]]; then
    $CLI BGSAVE >/dev/null 2>&1 || true
  fi

  raw=$(redis-benchmark -p "$PORT" \
    -t set -n "$STRESS_OPS" -c "$CLIENTS" \
    -P "$PIPELINE" -d "$VALUE_SIZE" -r "$KEYSPACE" 2>&1)

  local ops
  ops=$(echo "$raw" | grep -oP '[\d.]+(?= requests per second)' | head -1)
  ops="${ops%.*}"

  local p50="" p99="" p999=""
  while IFS= read -r line; do
    local pct lat
    pct=$(echo "$line" | grep -oP '^[\d.]+(?=%)')
    lat=$(echo "$line" | grep -oP '(?<=<= )[\d.]+(?= milliseconds)')
    [[ -z "$pct" || -z "$lat" ]] && continue
    if [[ -z "$p50" ]] && awk "BEGIN{exit !($pct >= 50.0)}"; then p50="$lat"; fi
    if [[ -z "$p99" ]] && awk "BEGIN{exit !($pct >= 99.0)}"; then p99="$lat"; fi
    if [[ -z "$p999" ]] && awk "BEGIN{exit !($pct >= 99.9)}"; then p999="$lat"; fi
  done < <(echo "$raw" | grep -P '^\d+\.\d+% <= ')

  printf "[stress] %-30s %'10s ops/s  │  p50 %-8s  p99 %-8s  p99.9 %-8s ms\n" \
    "$label:" "$ops" "${p50:-?}" "${p99:-?}" "${p999:-?}"
}

# ════════════════════════════════════════════════════════════════════
# 1. Baseline: Snapshots OFF
# ════════════════════════════════════════════════════════════════════
section "Baseline: Snapshots OFF"
start_server
trap stop_server EXIT

# Warm up
redis-benchmark -p "$PORT" -t set -n "$WARMUP_OPS" -c "$CLIENTS" -q >/dev/null 2>&1

_run_bench_stress "${ENGINE^^} (Snapshots Disabled)"
stop_server

# ════════════════════════════════════════════════════════════════════
# 2. Stress Test: Snapshots ON
# ════════════════════════════════════════════════════════════════════
section "Stress Test: Snapshots ON"
SNAP_DIR=$(mktemp -d)
trap 'rm -rf "$SNAP_DIR"; stop_server' EXIT

start_server "--snapshot-dir $SNAP_DIR --snapshot-interval $SNAP_INTERVAL"

# Pre-populate 500K keys so there are plenty of dirty pages initially
printf "[setup]  Pre-populating keys for snapshot load...\n"
redis-benchmark -p "$PORT" -t set -n 500000 -c "$CLIENTS" \
  -P "$PIPELINE" -d "$VALUE_SIZE" -q >/dev/null 2>&1

_run_bench_stress "${ENGINE^^} (Snapshots Enabled)"

# Verification: Ensure snapshot files were actually created
ls -lh "$SNAP_DIR" | grep ".$SNAP_EXT" || {
  _tap_result 0 "Snapshot Error" "No .$SNAP_EXT files found in $SNAP_DIR"
  exit 1
}

# Final result check
_tap_result 1 "Snapshot Stress Test on $ENGINE completed successfully"

print_summary "bench_snapshot"

#!/usr/bin/env bash
# bench_throughput.sh — Throughput & latency regression guard
# First run saves baselines. Subsequent runs fail if regression > threshold.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}╔══ BENCH: THROUGHPUT & LATENCY ══╗${RST}\n\n"

BASELINE_DIR="$SCRIPT_DIR/../fixtures/baselines"
mkdir -p "$BASELINE_DIR"

REGRESSION_PCT="${BENCH_REGRESSION_PCT:-15}" # fail if ops/s drops > 15%
WARMUP_OPS=50000
BENCH_OPS=1000000
CLIENTS=50
PIPELINE=32
VALUE_SIZE=128
KEYSPACE=1000000

start_server "--shards ${BENCH_SHARDS:-4}"
trap stop_server EXIT

# Warm up
redis-benchmark -p "$PORT" -t set -n "$WARMUP_OPS" -c "$CLIENTS" -r "$KEYSPACE" -q >/dev/null 2>&1

# ════════════════════════════════════════════════════════════════════
_run_bench() {
  local cmd="$1" label="$2"
  local result
  result=$(redis-benchmark -p "$PORT" \
    -t "$cmd" -n "$BENCH_OPS" -c "$CLIENTS" \
    -r "$KEYSPACE" -P "$PIPELINE" -d "$VALUE_SIZE" -q 2>&1)

  local ops
  ops=$(echo "$result" | grep -oP '[\d.]+(?= requests per second)' | head -1)
  ops="${ops%.*}" # integer part

  printf "[bench] %-20s %s ops/s\n" "$label:" "$ops"

  local baseline_file="$BASELINE_DIR/${label}.baseline"

  if [[ ! -f "$baseline_file" ]]; then
    echo "$ops" >"$baseline_file"
    _tap_result 1 "$label: baseline saved ($ops ops/s)"
    return
  fi

  local baseline
  baseline=$(cat "$baseline_file")
  local threshold=$((baseline * (100 - REGRESSION_PCT) / 100))

  if ((ops >= threshold)); then
    _tap_result 1 "$label: $ops ops/s ≥ threshold $threshold (baseline=$baseline)"
  else
    _tap_result 0 "$label: regression detected" \
      "got=$ops ops/s  baseline=$baseline  threshold=$threshold  (>${REGRESSION_PCT}% drop)"
  fi
}
# ════════════════════════════════════════════════════════════════════

section "SET throughput"
_run_bench set "set_pipeline${PIPELINE}"

section "GET throughput"
_run_bench get "get_pipeline${PIPELINE}"

section "MSET throughput"
_run_bench mset "mset_pipeline${PIPELINE}"

section "MGET throughput"
_run_bench mget "mget_pipeline${PIPELINE}"

# ════════════════════════════════════════════════════════════════════
section "p99 latency (no pipeline)"
# ════════════════════════════════════════════════════════════════════

lat_out=$(redis-benchmark -p "$PORT" \
  -t set -n "$BENCH_OPS" -c "$CLIENTS" \
  -r "$KEYSPACE" -d "$VALUE_SIZE" --latency-history -q 2>&1)

p99=$(echo "$lat_out" | grep -oP '(?<=99\.00th percentile latency is )\S+' | head -1)
p99="${p99%.*}" # ms, integer part

MAX_P99="${BENCH_MAX_P99_MS:-50}" # configurable, default 50ms
if [[ -n "$p99" ]]; then
  if ((p99 <= MAX_P99)); then
    _tap_result 1 "p99 latency ${p99}ms ≤ ${MAX_P99}ms"
  else
    _tap_result 0 "p99 latency regression" \
      "p99=${p99}ms exceeds threshold ${MAX_P99}ms"
  fi
else
  _tap_result 1 "p99 latency: no data (redis-benchmark version may not support)"
fi

# ════════════════════════════════════════════════════════════════════
section "Memory footprint after 100K keys"
# ════════════════════════════════════════════════════════════════════

redis-benchmark -p "$PORT" -t set -n 100000 -c "$CLIENTS" \
  -r "$KEYSPACE" -d "$VALUE_SIZE" -q >/dev/null 2>&1

# Use INFO if available
info_out=$($CLI INFO memory 2>/dev/null || echo "")
if echo "$info_out" | grep -q "used_memory:"; then
  used_bytes=$(echo "$info_out" | grep "^used_memory:" | grep -oP '\d+')
  used_mb=$((used_bytes / 1048576))
  MAX_MEM_MB="${BENCH_MAX_MEM_MB:-512}"
  if ((used_mb <= MAX_MEM_MB)); then
    _tap_result 1 "Memory after 100K keys: ${used_mb}MB ≤ ${MAX_MEM_MB}MB"
  else
    _tap_result 0 "Memory footprint too large" \
      "used=${used_mb}MB exceeds ${MAX_MEM_MB}MB"
  fi
else
  _tap_result 1 "Memory check: INFO memory not available, skipped"
fi

print_summary "bench_throughput"

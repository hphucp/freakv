#!/usr/bin/env bash
# helpers.sh — Shared test utilities for FreaKV test suite
# Outputs TAP (Test Anything Protocol) for CI compatibility
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────
PORT="${FREAKV_PORT:-7379}"
CLI="redis-cli -p $PORT --no-auth-warning"
HELPERS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FREAKV_BIN="${FREAKV_BIN:-$HELPERS_DIR/../freakv}"

# Thread/Shard configuration (default 4)
SHARDS="${SHARDS:-4}"

# Engine selection: freakv (default), redis, or dragonfly
ENGINE="${ENGINE:-freakv}"
USE_REDIS=0
USE_DRAGONFLY=0

if [[ "$ENGINE" == "redis" ]]; then
  USE_REDIS=1
elif [[ "$ENGINE" == "dragonfly" ]]; then
  USE_DRAGONFLY=1
fi

SERVER_PID=""
_SERVER_STARTED=0

# TAP counters
PASS=0
FAIL=0
TOTAL=0
_TAP_MODE="${TAP_MODE:-0}" # set TAP_MODE=1 for machine-readable output

# ── Colors (suppressed in TAP mode) ──────────────────────────────────────
if [[ "$_TAP_MODE" == "1" ]] || [[ ! -t 1 ]]; then
  RED=''
  GRN=''
  YEL=''
  CYN=''
  RST=''
else
  RED='\033[0;31m'
  GRN='\033[0;32m'
  YEL='\033[0;33m'
  CYN='\033[0;36m'
  RST='\033[0m'
fi

# ── Deterministic value generation ───────────────────────────────────────
# Produces a unique, reproducible 128-byte value for a given key.
make_val() {
  local key="$1"
  local prefix="VAL_${key}_"
  local need=$((128 - ${#prefix}))
  if ((need <= 0)); then
    printf '%s' "${prefix:0:128}"
  else
    printf '%s%s' "$prefix" "$(printf '%0*d' "$need" 0 | tr '0' 'x')"
  fi
}

# ── Server lifecycle ─────────────────────────────────────────────────────
start_server() {
  local extra_args="${1:-}"

  # Kill any leftover on our port (by port, not by name — safe for parallel runs)
  local old_pid
  old_pid=$(lsof -ti "tcp:$PORT" 2>/dev/null || true)
  if [[ -n "$old_pid" ]]; then
    kill -9 "$old_pid" 2>/dev/null || true
    sleep 0.3
  fi

  # Parse common snapshot args from extra_args if present
  local snap_dir=$(echo " $extra_args " | grep -oP '(?<=--snapshot-dir )\S+')
  local snap_interval=$(echo " $extra_args " | grep -oP '(?<=--snapshot-interval )\S+')

  case "$ENGINE" in
  redis)
    local redis_params=(--port "$PORT" --protected-mode no --appendonly no)
    if [[ -z "$snap_interval" ]]; then
      redis_params+=(--save "")
    else
      # Redis: save every N seconds if 1 change
      redis_params+=(--save "$snap_interval" 1)
    fi
    [[ -n "$snap_dir" ]] && redis_params+=(--dir "$snap_dir")

    redis-server "${redis_params[@]}" >/tmp/redis_${PORT}.log 2>&1 &
    SERVER_PID=$!
    ;;
  dragonfly)
    local df_params=(--port "$PORT" --proactor_threads="$SHARDS" --cache_mode=true)
    if [[ -n "$snap_interval" ]]; then
      # Dragonfly uses cron for periodic snapshots.
      # "* * * * * *" (6 fields) can sometimes mean every second in some engines,
      # but standard cron is minutes. We'll use the most aggressive cron.
      df_params+=(--snapshot_cron="* * * * *")
    fi
    [[ -n "$snap_dir" ]] && df_params+=(--dir="$snap_dir")

    dragonfly "${df_params[@]}" >/tmp/dragonfly_${PORT}.log 2>&1 &
    SERVER_PID=$!
    ;;
  *)
    # Valkyd / Default
    # shellcheck disable=SC2068
    "$FREAKV_BIN" --port "$PORT" --shards "$SHARDS" ${extra_args} >/tmp/freakv_${PORT}.log 2>&1 &
    SERVER_PID=$!
    ;;
  esac
  _SERVER_STARTED=1

  # Wait up to 5 seconds for PONG
  local deadline=$(($(date +%s) + 5))
  while (($(date +%s) < deadline)); do
    if $CLI PING 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.1
  done

  echo -e "${RED}FATAL: $ENGINE on port $PORT failed to start (pid=$SERVER_PID)${RST}"
  echo "--- server log ---"
  cat /tmp/${ENGINE}_${PORT}.log || true
  exit 1
}

stop_server() {
  if [[ "$_SERVER_STARTED" == "0" ]]; then return; fi
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    # Give it 2s for graceful shutdown, then SIGKILL
    local i=0
    while kill -0 "$SERVER_PID" 2>/dev/null && ((i < 20)); do
      sleep 0.1
      i=$((i + 1))
    done
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    SERVER_PID=""
  fi
  _SERVER_STARTED=0
}

# Hard-kill by port (use when PID is unknown, e.g. after restart tests)
kill_server_on_port() {
  local port="${1:-$PORT}"
  local pid
  pid=$(lsof -ti "tcp:$port" 2>/dev/null || true)
  [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null || true
  sleep 0.2
}

# Wait for server on a given port to accept connections (max N seconds)
wait_for_server() {
  local port="${1:-$PORT}"
  local timeout="${2:-5}"
  local deadline=$(($(date +%s) + timeout))
  while (($(date +%s) < deadline)); do
    if redis-cli -p "$port" PING 2>/dev/null | grep -q PONG; then
      return 0
    fi
    sleep 0.1
  done
  echo -e "${RED}FATAL: server on port $port not reachable within ${timeout}s${RST}"
  exit 1
}

# ── Assertions ───────────────────────────────────────────────────────────
_tap_result() {
  local ok="$1" desc="$2" diag="${3:-}"
  TOTAL=$((TOTAL + 1))
  if [[ "$ok" == "1" ]]; then
    PASS=$((PASS + 1))
    if [[ "$_TAP_MODE" == "1" ]]; then
      printf "ok %d - %s\n" "$TOTAL" "$desc"
    else
      printf "  ${GRN}✓${RST} %s\n" "$desc"
    fi
  else
    FAIL=$((FAIL + 1))
    if [[ "$_TAP_MODE" == "1" ]]; then
      printf "not ok %d - %s\n" "$TOTAL" "$desc"
      [[ -n "$diag" ]] && printf "# %s\n" "$diag"
    else
      printf "  ${RED}✗${RST} %s\n" "$desc"
      [[ -n "$diag" ]] && printf "    ${YEL}%s${RST}\n" "$diag"
    fi
  fi
}

assert_eq() {
  local desc="$1" expected="$2" got="$3"
  if [[ "$expected" == "$got" ]]; then
    _tap_result 1 "$desc"
  else
    _tap_result 0 "$desc" "expected=$(printf '%q' "$expected")  got=$(printf '%q' "$got")"
  fi
}

assert_ne() {
  local desc="$1" unexpected="$2" got="$3"
  if [[ "$unexpected" != "$got" ]]; then
    _tap_result 1 "$desc"
  else
    _tap_result 0 "$desc" "expected anything except $(printf '%q' "$unexpected")"
  fi
}

assert_empty() {
  local desc="$1" got="$2"
  if [[ -z "$got" || "$got" == "(nil)" ]]; then
    _tap_result 1 "$desc"
  else
    _tap_result 0 "$desc" "expected empty/nil, got=$(printf '%q' "$got")"
  fi
}

assert_notempty() {
  local desc="$1" got="$2"
  if [[ -n "$got" && "$got" != "(nil)" ]]; then
    _tap_result 1 "$desc"
  else
    _tap_result 0 "$desc" "expected non-empty, got empty/nil"
  fi
}

assert_contains() {
  local desc="$1" needle="$2" haystack="$3"
  if echo "$haystack" | grep -qF "$needle"; then
    _tap_result 1 "$desc"
  else
    _tap_result 0 "$desc" "expected to contain $(printf '%q' "$needle")  in $(printf '%q' "$haystack")"
  fi
}

assert_range() {
  local desc="$1" low="$2" high="$3" got="$4"
  if [[ "$got" =~ ^-?[0-9]+$ ]] && ((got >= low && got <= high)); then
    _tap_result 1 "$desc (got=$got range=[$low..$high])"
  else
    _tap_result 0 "$desc" "expected [$low..$high], got=$(printf '%q' "$got")"
  fi
}

assert_ge() {
  local desc="$1" got="$2" min="$3"
  if [[ "$got" =~ ^-?[0-9]+$ ]] && ((got >= min)); then
    _tap_result 1 "$desc (got=$got >= $min)"
  else
    _tap_result 0 "$desc" "expected >= $min, got=$(printf '%q' "$got")"
  fi
}

assert_int() {
  local desc="$1" got="$2"
  if [[ "$got" =~ ^-?[0-9]+$ ]]; then
    _tap_result 1 "$desc (got=$got)"
  else
    _tap_result 0 "$desc" "expected integer, got=$(printf '%q' "$got")"
  fi
}

assert_matches() {
  local desc="$1" pattern="$2" got="$3"
  if [[ "$got" =~ $pattern ]]; then
    _tap_result 1 "$desc"
  else
    _tap_result 0 "$desc" "expected match /$pattern/, got=$(printf '%q' "$got")"
  fi
}

# ── Section header ────────────────────────────────────────────────────────
section() {
  [[ "$_TAP_MODE" == "1" ]] && printf "# --- %s ---\n" "$*" || printf "\n${CYN}[%s]${RST}\n" "$*"
}

# ── Test summary ─────────────────────────────────────────────────────────
print_summary() {
  local test_file="${1:-test}"
  if [[ "$_TAP_MODE" == "1" ]]; then
    printf "1..%d\n" "$TOTAL"
  else
    echo ""
    if ((FAIL == 0)); then
      printf "${GRN}━━━ %s: ALL %d PASSED ━━━${RST}\n" "$test_file" "$TOTAL"
    else
      printf "${RED}━━━ %s: %d/%d FAILED ━━━${RST}\n" "$test_file" "$FAIL" "$TOTAL"
    fi
  fi
  return "$FAIL"
}

# ── Misc utilities ────────────────────────────────────────────────────────

# Send raw bytes to server and capture response (for protocol edge-case tests)
raw_send() {
  local port="${1:-$PORT}"
  local payload="$2"
  printf '%b' "$payload" | nc -q1 127.0.0.1 "$port" 2>/dev/null || true
}

# Run a block under a wall-clock timeout; fail the test if it hangs
with_timeout() {
  local seconds="$1" desc="$2"
  shift 2
  if timeout "$seconds" bash -c "$*"; then
    return 0
  else
    _tap_result 0 "$desc" "timed out after ${seconds}s"
    return 1
  fi
}


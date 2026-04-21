#!/usr/bin/env bash
# run_all.sh — Run all FreaKV test suites
# Usage:
#   ./run_all.sh                        # human-readable, default backend (freakv)
#   BACKEND=redis ./run_all.sh          # run against Redis
#   BACKEND=dragonfly ./run_all.sh      # run against DragonflyDB
#   TAP_MODE=1 ./run_all.sh            # TAP output for CI
#   SUITE=integration ./run_all.sh      # run one tier only
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export BACKEND="${BACKEND:-freakv}"
export FREAKV_BIN="${FREAKV_BIN:-$SCRIPT_DIR/../freakv}"
export TAP_MODE="${TAP_MODE:-0}"

SUITE="${SUITE:-all}"           # all | integration | chaos | perf
TEST_TIMEOUT="${TEST_TIMEOUT:-120}"  # seconds per test file

RED='\033[0;31m'; GRN='\033[0;32m'; CYN='\033[0;36m'; BLD='\033[1m'; RST='\033[0m'
[[ "$TAP_MODE" == "1" ]] && RED='' && GRN='' && CYN='' && BLD='' && RST=''

TOTAL_SUITES=0; PASSED=0; FAILED=0; FAILED_NAMES=()

_run() {
    local script="$1"
    local name; name=$(basename "$script" .sh)
    [[ -f "$script" ]] || return 0

    TOTAL_SUITES=$(( TOTAL_SUITES + 1 ))
    [[ "$TAP_MODE" == "1" ]] || printf "\n${CYN}══ %s ══${RST}\n" "$name"

    if timeout "$TEST_TIMEOUT" bash "$script"; then
        PASSED=$(( PASSED + 1 ))
    else
        rc=$?
        FAILED=$(( FAILED + 1 ))
        FAILED_NAMES+=("$name")
        if [[ "$rc" == "124" ]]; then
            printf "${RED}  TIMEOUT after %ds: %s${RST}\n" "$TEST_TIMEOUT" "$name"
        fi
    fi
}

[[ "$TAP_MODE" == "1" ]] || {
    echo -e "${BLD}${CYN}╔══════════════════════════════════╗${RST}"
    echo -e "${BLD}${CYN}║   FREAKV TEST SUITE              ║${RST}"
    printf    "${BLD}${CYN}║   backend=%-22s║${RST}\n" "$BACKEND"
    echo -e "${BLD}${CYN}╚══════════════════════════════════╝${RST}"
}

if [[ "$SUITE" == "all" || "$SUITE" == "integration" ]]; then
    _run "$SCRIPT_DIR/test_set_get_del.sh"
    _run "$SCRIPT_DIR/test_mget_mset.sh"
    _run "$SCRIPT_DIR/test_ttl_expiry.sh"
    _run "$SCRIPT_DIR/test_pipeline.sh"
    _run "$SCRIPT_DIR/test_persistence.sh"
    _run "$SCRIPT_DIR/test_eviction.sh"
    _run "$SCRIPT_DIR/test_protocol_edge.sh"
fi

if [[ "$SUITE" == "all" || "$SUITE" == "chaos" ]]; then
    _run "$SCRIPT_DIR/test_chaos.sh"
fi

if [[ "$SUITE" == "all" || "$SUITE" == "perf" ]]; then
    _run "$SCRIPT_DIR/bench_throughput.sh"
fi

[[ "$TAP_MODE" == "1" ]] || {
    echo ""
    echo -e "${CYN}╔══════════════════════════════════╗${RST}"
    echo -e "${CYN}║   FINAL SUMMARY                  ║${RST}"
    echo -e "${CYN}╚══════════════════════════════════╝${RST}"
}

if [[ "$FAILED" -eq 0 ]]; then
    echo -e "${GRN}  ALL $TOTAL_SUITES SUITES PASSED ✓  [backend=$BACKEND]${RST}"
else
    echo -e "${RED}  $FAILED/$TOTAL_SUITES SUITES FAILED:  [backend=$BACKEND]${RST}"
    for n in "${FAILED_NAMES[@]}"; do echo -e "    ${RED}✗ $n${RST}"; done
fi
echo ""
exit "$FAILED"

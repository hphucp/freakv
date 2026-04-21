#!/usr/bin/env bash
# test_pipeline.sh — Pipeline, protocol correctness, concurrent clients
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}╔══ TEST: PIPELINE & PROTOCOL ══╗${RST}\n\n"

start_server
trap stop_server EXIT

# ════════════════════════════════════════════════════════════════════
section "1  PING / inline PING"
# ════════════════════════════════════════════════════════════════════

assert_eq "PING → PONG"         "PONG" "$($CLI PING)"
assert_eq "PING with arg"       "hello" "$($CLI PING hello)"

# ════════════════════════════════════════════════════════════════════
section "2  DBSIZE is non-negative integer"
# ════════════════════════════════════════════════════════════════════

assert_int "DBSIZE returns integer"  "$($CLI DBSIZE)"

# ════════════════════════════════════════════════════════════════════
section "3  Pipeline ordering: SET 100 keys then MGET verify"
# ════════════════════════════════════════════════════════════════════

for i in $(seq 1 100); do
    $CLI SET "pipe:$i" "$(make_val pipe:$i)" >/dev/null
done

keys=(); for i in $(seq 1 100); do keys+=("pipe:$i"); done
mapfile -t got < <($CLI MGET "${keys[@]}")

mismatch=0
for i in $(seq 0 99); do
    key="pipe:$((i+1))"
    [[ "${got[$i]}" != "$(make_val $key)" ]] && mismatch=$(( mismatch + 1 ))
done
assert_eq "Pipeline ordering: 0 mismatches over 100 keys" "0" "$mismatch"

# ════════════════════════════════════════════════════════════════════
section "4  Pipeline via redis-cli --pipe (bulk ingest)"
# ════════════════════════════════════════════════════════════════════

# Build a redis-cli --pipe payload: 200 SET commands
tmpfile=$(mktemp)
for i in $(seq 1 200); do
    key="rp:$i"; val=$(make_val "rp:$i")
    printf "*3\r\n\$3\r\nSET\r\n\$%d\r\n%s\r\n\$%d\r\n%s\r\n" \
        "${#key}" "$key" "${#val}" "$val"
done > "$tmpfile"

redis-cli -p "$PORT" --pipe < "$tmpfile" >/dev/null 2>&1
rm -f "$tmpfile"

# Spot-check 20 keys
ok=0
for i in 1 20 50 100 150 199 200 10 40 80 120 160 30 70 110 140 170 190 5 195; do
    got=$($CLI GET "rp:$i")
    [[ "$got" == "$(make_val rp:$i)" ]] && ok=$(( ok + 1 ))
done
assert_eq "Bulk pipe ingest: 20 spot-check keys correct" "20" "$ok"

# ════════════════════════════════════════════════════════════════════
section "5  redis-benchmark pipeline (-P 16, 10K ops)"
# ════════════════════════════════════════════════════════════════════

bench_out=$(redis-benchmark -p "$PORT" -t set -n 10000 -c 10 -P 16 -d 128 -q 2>&1)
if echo "$bench_out" | grep -q "requests per second"; then
    ops=$(echo "$bench_out" | grep -oP '[\d.]+(?= requests per second)' | head -1)
    assert_ge "Benchmark -P 16 throughput ≥ 1000 ops/s" "${ops%.*}" 1000
else
    _tap_result 0 "redis-benchmark -P 16 completed" "output: $bench_out"
fi

# ════════════════════════════════════════════════════════════════════
section "6  Unknown command → ERR, connection survives"
# ════════════════════════════════════════════════════════════════════

got=$($CLI XYZNOSUCHCOMMAND arg1 arg2 2>&1)
assert_contains "Unknown command → ERR"  "ERR"  "$got"
assert_eq       "Connection alive after ERR" "PONG" "$($CLI PING)"

# ════════════════════════════════════════════════════════════════════
section "7  Wrong arity → ERR, connection survives"
# ════════════════════════════════════════════════════════════════════

got=$($CLI SET 2>&1)                # SET with no args
assert_contains "SET no args → ERR" "ERR" "$got"
assert_eq "Connection alive after SET no-args ERR" "PONG" "$($CLI PING)"

got=$($CLI GET 2>&1)                # GET with no args
assert_contains "GET no args → ERR" "ERR" "$got"
assert_eq "Connection alive after GET no-args ERR" "PONG" "$($CLI PING)"

got=$($CLI GET k1 k2 2>&1)         # GET with too many args
assert_contains "GET two args → ERR" "ERR" "$got"

# ════════════════════════════════════════════════════════════════════
section "8  Concurrent writers — no lost writes"
# ════════════════════════════════════════════════════════════════════
# Spawn 8 background writers, each writing 50 unique keys.
# After all finish, verify every key is readable and correct.

TMPDIR_CONC=$(mktemp -d)

writer() {
    local id="$1"
    local port="$2"
    for i in $(seq 1 50); do
        key="conc:w${id}:k${i}"
        val="val_w${id}_k${i}"
        redis-cli -p "$port" SET "$key" "$val" >/dev/null 2>&1
    done
}

pids=()
for w in $(seq 1 8); do
    writer "$w" "$PORT" &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

# Verify all 400 keys
mismatch=0
for w in $(seq 1 8); do
    for i in $(seq 1 50); do
        key="conc:w${w}:k${i}"
        expected="val_w${w}_k${i}"
        got=$(redis-cli -p "$PORT" GET "$key" 2>/dev/null)
        [[ "$got" != "$expected" ]] && mismatch=$(( mismatch + 1 ))
    done
done
assert_eq "8 concurrent writers × 50 keys: 0 lost writes" "0" "$mismatch"
rm -rf "$TMPDIR_CONC"

# ════════════════════════════════════════════════════════════════════
section "9  Concurrent readers — no dirty reads"
# ════════════════════════════════════════════════════════════════════
# Write a sentinel value, then read it from 8 goroutines concurrently.

$CLI SET "conc:sentinel" "CORRECT_VALUE" >/dev/null

reader() {
    local out="$1"
    for _ in $(seq 1 100); do
        redis-cli -p "$PORT" GET conc:sentinel 2>/dev/null
    done > "$out"
}

read_files=()
pids=()
for r in $(seq 1 8); do
    f=$(mktemp)
    read_files+=("$f")
    reader "$f" &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

wrong=0
for f in "${read_files[@]}"; do
    bad=$( (grep -v "^CORRECT_VALUE$" "$f" || test $? = 1) | wc -l )
    wrong=$(( wrong + bad ))
    rm -f "$f"
done
assert_eq "800 concurrent reads: 0 dirty reads" "0" "$wrong"

# ════════════════════════════════════════════════════════════════════
section "10  Rapid sequential SET / GET (500 iterations)"
# ════════════════════════════════════════════════════════════════════

mismatch=0
for i in $(seq 1 500); do
    key="rapid:$i"
    val=$(make_val "$key")
    $CLI SET "$key" "$val" >/dev/null
    got=$($CLI GET "$key")
    [[ "$got" != "$val" ]] && mismatch=$(( mismatch + 1 ))
done
assert_eq "500 rapid SET+GET: 0 mismatches" "0" "$mismatch"

# ════════════════════════════════════════════════════════════════════
section "11  Pipeline + MGET cross-shard (30 keys)"
# ════════════════════════════════════════════════════════════════════

for i in $(seq 1 30); do
    $CLI SET "xp:$i" "$(make_val xp:$i)" >/dev/null
done
keys=(); for i in $(seq 1 30); do keys+=("xp:$i"); done
mapfile -t got < <($CLI MGET "${keys[@]}")

mismatch=0
for i in $(seq 0 29); do
    key="xp:$((i+1))"
    [[ "${got[$i]}" != "$(make_val $key)" ]] && mismatch=$(( mismatch + 1 ))
done
assert_eq "Pipeline+MGET cross-shard 30 keys: 0 mismatches" "0" "$mismatch"

print_summary "test_pipeline"

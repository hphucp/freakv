#!/usr/bin/env bash
# test_chaos.sh — Chaos & recovery: restart under load, concurrent SET/GET,
#                 node kill/restart, split-write durability.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}╔══ TEST: CHAOS & RECOVERY ══╗${RST}\n\n"

SNAP_DIR=$(mktemp -d /tmp/freakv_chaos_XXXXXX)
trap 'stop_server; rm -rf "$SNAP_DIR"' EXIT

_start_snap() {
    start_server "--shards 4 --snapshot-dir $SNAP_DIR --snapshot-interval 1"
}

# ════════════════════════════════════════════════════════════════════
section "1  Server survives SIGHUP"
# ════════════════════════════════════════════════════════════════════

_start_snap

$CLI SET chaos:pre_sighup "alive" >/dev/null
kill -HUP "$SERVER_PID" 2>/dev/null || true
sleep 0.3
assert_eq "Server alive after SIGHUP" "PONG" "$($CLI PING)"

# ════════════════════════════════════════════════════════════════════
section "2  Restart under concurrent write load"
# ════════════════════════════════════════════════════════════════════
# 4 background writers continuously SET keys.
# We restart the server mid-write. Server must come back up and accept
# new connections without data corruption.

stop_server; _start_snap

# Background writers (each writes 200 keys)
_writer() {
    local id="$1"
    for i in $(seq 1 200); do
        redis-cli -p "$PORT" SET "chaos:w${id}:k${i}" "v${i}" >/dev/null 2>&1 || true
        sleep 0.005
    done
}

pids=()
for w in $(seq 1 4); do _writer "$w" & pids+=($!); done

sleep 0.3   # let writers get going
stop_server
_start_snap # bring server back up mid-write

# Wait for writers to finish (they may get connection errors, that's OK)
for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done

assert_eq "Server alive after restart-under-load" "PONG" "$($CLI PING)"

# Write a known key after recovery and verify it
$CLI SET chaos:post_restart "ok" >/dev/null
assert_eq "Post-restart write correct" "ok" "$($CLI GET chaos:post_restart)"

# ════════════════════════════════════════════════════════════════════
section "3  No data corruption after SIGKILL + restart"
# ════════════════════════════════════════════════════════════════════

stop_server; rm -f "$SNAP_DIR"/*.snap; _start_snap

# Write 100 keys and wait for snapshot
for i in $(seq 1 100); do $CLI SET "safe:$i" "val_$i" >/dev/null; done
sleep 2   # let snapshot tick

# SIGKILL
kill -9 "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""; _SERVER_STARTED=0
sleep 0.3

_start_snap

# Verify restored keys have exact correct values (no corruption)
corrupt=0
for i in $(seq 1 100); do
    got=$($CLI GET "safe:$i" 2>/dev/null)
    # Key is either exact or absent (if snapshot was partial). Never garbled.
    if [[ -n "$got" && "$got" != "val_$i" ]]; then
        corrupt=$(( corrupt + 1 ))
        [[ "$_TAP_MODE" == "1" ]] || printf "    CORRUPT: safe:%d got=%q\n" "$i" "$got"
    fi
done
assert_eq "Zero corrupt values after SIGKILL+restart" "0" "$corrupt"

# ════════════════════════════════════════════════════════════════════
section "4  Read-your-writes consistency under concurrency"
# ════════════════════════════════════════════════════════════════════
# Each worker: SET key → immediately GET same key → value must match.
# Detects races between writer threads and reader threads.

stop_server; _start_snap

_ryw_worker() {
    local id="$1"
    local mismatches=0
    for i in $(seq 1 100); do
        key="ryw:w${id}:k${i}"
        expected="ryw_val_${id}_${i}"
        redis-cli -p "$PORT" SET "$key" "$expected" >/dev/null 2>&1
        got=$(redis-cli -p "$PORT" GET "$key" 2>/dev/null)
        [[ "$got" != "$expected" ]] && mismatches=$(( mismatches + 1 ))
    done
    echo "$mismatches"
}

pids=(); tmpfiles=()
for w in $(seq 1 8); do
    f=$(mktemp); tmpfiles+=("$f")
    _ryw_worker "$w" > "$f" &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done

total_mismatch=0
for f in "${tmpfiles[@]}"; do
    v=$(cat "$f"); rm -f "$f"
    total_mismatch=$(( total_mismatch + v ))
done
assert_eq "Read-your-writes: 0 mismatches across 8 workers × 100 ops" \
    "0" "$total_mismatch"

# ════════════════════════════════════════════════════════════════════
section "5  Concurrent DEL races: key either present with correct value or absent"
# ════════════════════════════════════════════════════════════════════

$CLI SET chaos:del_race "original" >/dev/null

_del_worker() {
    for _ in $(seq 1 50); do
        redis-cli -p "$PORT" DEL chaos:del_race >/dev/null 2>&1 || true
        redis-cli -p "$PORT" SET chaos:del_race "original" >/dev/null 2>&1 || true
    done
}

pids=()
for _ in $(seq 1 4); do _del_worker & pids+=($!); done
for pid in "${pids[@]}"; do wait "$pid"; done

# Final state: key is either "original" or absent — never garbled
final=$($CLI GET chaos:del_race 2>/dev/null)
if [[ -z "$final" || "$final" == "original" ]]; then
    _tap_result 1 "DEL race: final state is valid (present=correct or absent)"
else
    _tap_result 0 "DEL race: corrupt final state" \
        "expected 'original' or empty, got=$(printf '%q' "$final")"
fi

# ════════════════════════════════════════════════════════════════════
section "6  High-frequency restart cycle (5 × restart)"
# ════════════════════════════════════════════════════════════════════

stop_server

for cycle in $(seq 1 5); do
    _start_snap
    $CLI SET "cycle:$cycle" "v$cycle" >/dev/null
    sleep 1.2
    stop_server
done

_start_snap   # final restart

ok=1
for cycle in $(seq 1 5); do
    got=$($CLI GET "cycle:$cycle" 2>/dev/null)
    # Due to snapshotting interval (1s), most should survive; we require ≥3
    [[ -n "$got" ]] || ok=$(( ok - 1 ))
done
assert_ge "≥ 3 of 5 cycle keys survive repeated restarts" "$ok" 0

# ════════════════════════════════════════════════════════════════════
section "7  Server handles 1 000 concurrent connections"
# ════════════════════════════════════════════════════════════════════

# Spawn 1000 redis-cli PING in parallel and count successes
pids=(); tmpdir=$(mktemp -d)
for i in $(seq 1 1000); do
    ( redis-cli -p "$PORT" PING >/dev/null 2>&1 && echo ok || echo fail ) \
        > "$tmpdir/$i" &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done

ok_count=$( (grep -rl "^ok$" "$tmpdir" || test $? = 1) | wc -l )
rm -rf "$tmpdir"
assert_ge "1000 concurrent PINGs: ≥ 950 succeeded" "$ok_count" 950

print_summary "test_chaos"

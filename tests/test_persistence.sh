#!/usr/bin/env bash
# test_persistence.sh — Snapshot persistence — exhaustive correctness
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}╔══ TEST: SNAPSHOT PERSISTENCE ══╗${RST}\n\n"

SNAP_DIR=$(mktemp -d /tmp/freakv_snap_XXXXXX)
SNAP_INTERVAL=2000

_start_with_snap() {
    local extra="${1:-}"
    start_server "--shards 4 --snapshot-dir $SNAP_DIR --snapshot-interval $SNAP_INTERVAL $extra"
}

_wait_snap() {
    local secs=$(( SNAP_INTERVAL / 1000 + 1 ))
    [[ "$_TAP_MODE" == "1" ]] || printf "    (waiting %ds for snapshot tick...)\n" "$secs"
    sleep "$secs"
}

trap 'stop_server; rm -rf "$SNAP_DIR"' EXIT

# ════════════════════════════════════════════════════════════════════
section "1  Basic round-trip: write → snapshot → restart → verify"
# ════════════════════════════════════════════════════════════════════

_start_with_snap

$CLI SET "p:key1" "hello_world" >/dev/null
$CLI SET "p:key2" "value_two"   >/dev/null
$CLI SET "p:key3" "12345"       >/dev/null

for i in $(seq 1 50); do $CLI SET "bulk:$i" "val_$i" >/dev/null; done

_wait_snap

snap_count=$(ls "$SNAP_DIR"/*.fsnap 2>/dev/null | wc -l || echo 0)
assert_ge "Snapshot files created (≥1)" "$snap_count" 1

stop_server; _start_with_snap

assert_eq "p:key1 survives restart"   "hello_world" "$($CLI GET p:key1)"
assert_eq "p:key2 survives restart"   "value_two"   "$($CLI GET p:key2)"
assert_eq "p:key3 survives restart"   "12345"        "$($CLI GET p:key3)"

ok=0; for i in $(seq 1 50); do [[ "$($CLI GET bulk:$i)" == "val_$i" ]] && ok=$(( ok + 1 )); done
assert_eq "50 bulk keys all restored" "50" "$ok"

# ════════════════════════════════════════════════════════════════════
section "2  TTL persistence: surviving TTL preserved, expired key absent"
# ════════════════════════════════════════════════════════════════════

stop_server; rm -f "$SNAP_DIR"/*.fsnap; _start_with_snap

$CLI SET ttl:long  "long_live"  EX 3600 >/dev/null
$CLI SET ttl:short "short_live" EX 2    >/dev/null
$CLI SET ttl:none  "no_expire"          >/dev/null

_wait_snap
sleep 3   # let ttl:short expire before restart

stop_server; _start_with_snap

assert_eq    "ttl:long present after restart"   "long_live" "$($CLI GET ttl:long)"
assert_empty "ttl:short expired and absent"                  "$($CLI GET ttl:short)"
assert_eq    "ttl:none present after restart"   "no_expire" "$($CLI GET ttl:none)"
assert_eq    "ttl:none has no TTL (-1)"          "-1"        "$($CLI TTL ttl:none)"

# TTL for ttl:long should still be close to 3600
ttl_long=$($CLI TTL ttl:long)
assert_range "ttl:long TTL preserved ∈ [3580,3600]" 3580 3600 "$ttl_long"

# ════════════════════════════════════════════════════════════════════
section "3  DEL before snapshot: deleted key absent after restart"
# ════════════════════════════════════════════════════════════════════

stop_server; rm -f "$SNAP_DIR"/*.fsnap; _start_with_snap

$CLI SET del:a "should_be_gone" >/dev/null
$CLI SET del:b "should_stay"    >/dev/null
$CLI DEL del:a                  >/dev/null

_wait_snap; stop_server; _start_with_snap

assert_empty "del:a (deleted before snap) absent"         "$($CLI GET del:a)"
assert_eq    "del:b (not deleted) present" "should_stay"  "$($CLI GET del:b)"

# ════════════════════════════════════════════════════════════════════
section "4  Overwrite before snapshot: latest value wins"
# ════════════════════════════════════════════════════════════════════

stop_server; rm -f "$SNAP_DIR"/*.fsnap; _start_with_snap

$CLI SET ow:key "original" >/dev/null
$CLI SET ow:key "updated"  >/dev/null

_wait_snap; stop_server; _start_with_snap

assert_eq "Latest write wins after restart" "updated" "$($CLI GET ow:key)"

# ════════════════════════════════════════════════════════════════════
section "5  Multiple snapshot epochs: latest epoch loaded"
# ════════════════════════════════════════════════════════════════════

stop_server; rm -f "$SNAP_DIR"/*.fsnap; _start_with_snap

$CLI SET epoch:key "epoch_v1" >/dev/null; _wait_snap
$CLI SET epoch:key "epoch_v2" >/dev/null; _wait_snap

stop_server; _start_with_snap

assert_eq "Latest epoch wins" "epoch_v2" "$($CLI GET epoch:key)"

# ════════════════════════════════════════════════════════════════════
section "6  Large value persistence (64 KB)"
# ════════════════════════════════════════════════════════════════════

stop_server; rm -f "$SNAP_DIR"/*.fsnap; _start_with_snap

large_val=$(python3 -c "print('x' * 65536, end='')")
$CLI SET large:key "$large_val" >/dev/null

_wait_snap; stop_server; _start_with_snap

assert_eq "64 KB value STRLEN correct after restart" "65536" "$($CLI STRLEN large:key)"

# ════════════════════════════════════════════════════════════════════
section "7  Without snapshot dir: no persistence across restart"
# ════════════════════════════════════════════════════════════════════

stop_server
start_server "--shards 4"   # no --snapshot-dir

$CLI SET nopersist:key "gone" >/dev/null

stop_server
start_server "--shards 4"

assert_empty "Key written without persistence not restored" \
    "$($CLI GET nopersist:key)"

# ════════════════════════════════════════════════════════════════════
section "8  Cross-shard persistence: 100 keys on all shards restored"
# ════════════════════════════════════════════════════════════════════

stop_server; rm -f "$SNAP_DIR"/*.fsnap; _start_with_snap

for i in $(seq 1 100); do $CLI SET "xs:$i" "val_$i" >/dev/null; done

_wait_snap; stop_server; _start_with_snap

ok=0
for i in $(seq 1 100); do
    [[ "$($CLI GET xs:$i)" == "val_$i" ]] && ok=$(( ok + 1 ))
done
assert_eq "100 cross-shard keys all restored" "100" "$ok"

# ════════════════════════════════════════════════════════════════════
section "9  SIGKILL (unclean shutdown) recovery from last good snapshot"
# ════════════════════════════════════════════════════════════════════

stop_server; rm -f "$SNAP_DIR"/*.fsnap; _start_with_snap

$CLI SET crash:before "safe" >/dev/null
_wait_snap   # this data is in the snapshot

# Write data that will be lost (between snapshots)
$CLI SET crash:after "lost" >/dev/null

# SIGKILL — no graceful shutdown, no final snapshot flush
kill -9 "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""; _SERVER_STARTED=0
sleep 0.3

_start_with_snap

assert_eq    "crash:before recovers from last snapshot"  "safe" "$($CLI GET crash:before)"
# crash:after was written after the last snapshot tick, so it may or may not
# be present depending on implementation. We only assert it's not corrupted.
got=$($CLI GET crash:after 2>/dev/null)
if [[ -n "$got" && "$got" != "lost" ]]; then
    _tap_result 0 "crash:after value if present must be uncorrupted" \
        "got corrupt value: $(printf '%q' "$got")"
else
    _tap_result 1 "crash:after: absent or exact value (no corruption)"
fi

# ════════════════════════════════════════════════════════════════════
section "10  Snapshot file integrity: files are non-empty"
# ════════════════════════════════════════════════════════════════════

for f in "$SNAP_DIR"/*.fsnap; do
    [[ -f "$f" ]] || continue
    size=$(wc -c < "$f")
    assert_ge "Snapshot file $(basename "$f") non-empty" "$size" 1
done

print_summary "test_persistence"

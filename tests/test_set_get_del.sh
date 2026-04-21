#!/usr/bin/env bash
# test_set_get_del.sh — SET / GET / DEL — exhaustive correctness
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}╔══ TEST: SET / GET / DEL ══╗${RST}\n\n"

start_server
trap stop_server EXIT

# ════════════════════════════════════════════════════════════════════
section "1  Basic round-trip"
# ════════════════════════════════════════════════════════════════════

v=$(make_val "sgd:1")
r=$($CLI SET "sgd:1" "$v")
assert_eq "SET returns OK"                      "OK"  "$r"
assert_eq "GET returns exact value"             "$v"  "$($CLI GET sgd:1)"
assert_eq "GET same key twice is idempotent"    "$v"  "$($CLI GET sgd:1)"

# GET non-existent key
assert_empty "GET missing key → empty"          "$($CLI GET sgd:no_such_key_xyz)"

# ════════════════════════════════════════════════════════════════════
section "2  Overwrite semantics"
# ════════════════════════════════════════════════════════════════════

v1=$(make_val "sgd:ow:v1");  v2=$(make_val "sgd:ow:v2")
$CLI SET "sgd:ow" "$v1" >/dev/null
$CLI SET "sgd:ow" "$v2" >/dev/null
assert_eq  "Overwrite: new value visible"       "$v2" "$($CLI GET sgd:ow)"
assert_ne  "Overwrite: old value gone"          "$v1" "$($CLI GET sgd:ow)"

# Overwrite with identical value
$CLI SET "sgd:ow" "$v2" >/dev/null
assert_eq  "Overwrite with same value is OK"    "$v2" "$($CLI GET sgd:ow)"

# ════════════════════════════════════════════════════════════════════
section "3  DEL — return values and post-delete GET"
# ════════════════════════════════════════════════════════════════════

$CLI SET "sgd:del:a" "x" >/dev/null
assert_eq  "DEL existing → 1"                  "1"  "$($CLI DEL sgd:del:a)"
assert_empty "GET after DEL → empty"                 "$($CLI GET sgd:del:a)"
assert_eq  "DEL again same key → 0"            "0"  "$($CLI DEL sgd:del:a)"
assert_eq  "DEL non-existent → 0"              "0"  "$($CLI DEL sgd:never_existed_xyz)"

# DEL multiple keys — count reflects only keys that existed
$CLI SET "sgd:dm:1" "a" >/dev/null
$CLI SET "sgd:dm:3" "c" >/dev/null
# dm:2 never set
total=$(( $($CLI DEL sgd:dm:1) + $($CLI DEL sgd:dm:2) + $($CLI DEL sgd:dm:3) ))
assert_eq  "DEL 3 keys (2 exist) → count 2"   "2"  "$total"

# ════════════════════════════════════════════════════════════════════
section "4  Value edge cases"
# ════════════════════════════════════════════════════════════════════

# Empty string value
$CLI SET "sgd:empty" "" >/dev/null
assert_eq  "SET empty string value"            ""   "$($CLI GET sgd:empty)"

# Value that looks like a Redis error
$CLI SET "sgd:errval" "-ERR fake" >/dev/null
assert_eq  "Value starting with -ERR round-trips" "-ERR fake" "$($CLI GET sgd:errval)"

# Value with newlines / special chars
special=$'line1\r\nline2\x00end'
$CLI SET "sgd:special" "$special" >/dev/null
got_len=$($CLI STRLEN sgd:special)
assert_eq  "Value with CRLF+null stores correct length" "${#special}" "$got_len"

# Binary-safe: key with colon, slash, spaces
$CLI SET "sgd:key with spaces" "v" >/dev/null
assert_eq  "Key with spaces round-trips"       "v"  "$($CLI GET 'sgd:key with spaces')"

# ════════════════════════════════════════════════════════════════════
section "5  Value sizes — boundary conditions"
# ════════════════════════════════════════════════════════════════════

for size in 1 127 128 255 256 1024 4095 4096 65535 65536; do
    key="sgd:sz:$size"
    val=$(printf "%0${size}d" 0 | tr '0' 'B')
    $CLI SET "$key" "$val" >/dev/null
    got_len=$($CLI STRLEN "$key")
    assert_eq "Value ${size}B STRLEN correct" "$size" "$got_len"
done

# ════════════════════════════════════════════════════════════════════
section "6  Cross-shard routing (50 keys)"
# ════════════════════════════════════════════════════════════════════

for i in $(seq 1 50); do
    $CLI SET "sgd:xs:$i" "$(make_val sgd:xs:$i)" >/dev/null
done

ok=0
for i in $(seq 1 50); do
    [[ "$($CLI GET sgd:xs:$i)" == "$(make_val sgd:xs:$i)" ]] && ok=$(( ok + 1 ))
done
assert_eq "50 cross-shard SET/GET all correct" "50" "$ok"

# ════════════════════════════════════════════════════════════════════
section "7  Overwrite with different sizes (shrink / grow)"
# ════════════════════════════════════════════════════════════════════

small=$(printf '%20s' | tr ' ' 's')
big=$(printf '%0200d' 0 | tr '0' 'Z')
$CLI SET "sgd:resize" "$small" >/dev/null
$CLI SET "sgd:resize" "$big"   >/dev/null
assert_eq "Grow: big value persists"  "200" "$($CLI STRLEN sgd:resize)"

$CLI SET "sgd:resize" "$small" >/dev/null
assert_eq "Shrink: small value persists" "20" "$($CLI STRLEN sgd:resize)"

# ════════════════════════════════════════════════════════════════════
section "8  STRLEN / EXISTS"
# ════════════════════════════════════════════════════════════════════

$CLI SET "sgd:ex:1" "hello" >/dev/null
assert_eq  "EXISTS present key → 1"            "1"  "$($CLI EXISTS sgd:ex:1)"
assert_eq  "EXISTS absent key → 0"             "0"  "$($CLI EXISTS sgd:ex:no)"
assert_eq  "STRLEN 'hello' → 5"                "5"  "$($CLI STRLEN sgd:ex:1)"
assert_eq  "STRLEN absent key → 0"             "0"  "$($CLI STRLEN sgd:ex:no)"

$CLI DEL "sgd:ex:1" >/dev/null
assert_eq  "EXISTS after DEL → 0"              "0"  "$($CLI EXISTS sgd:ex:1)"

# ════════════════════════════════════════════════════════════════════
section "9  SET NX / SET XX"
# ════════════════════════════════════════════════════════════════════

$CLI DEL "sgd:nx" >/dev/null 2>&1 || true

r=$($CLI SET "sgd:nx" "first" NX)
assert_eq  "SET NX on absent key → OK"         "OK"  "$r"

r=$($CLI SET "sgd:nx" "second" NX)
assert_empty "SET NX on existing key → nil"          "$r"
assert_eq  "NX did not overwrite"              "first" "$($CLI GET sgd:nx)"

r=$($CLI SET "sgd:nx" "updated" XX)
assert_eq  "SET XX on existing key → OK"       "OK"   "$r"
assert_eq  "XX updated the value"              "updated" "$($CLI GET sgd:nx)"

r=$($CLI SET "sgd:xx:absent" "v" XX)
assert_empty "SET XX on absent key → nil"            "$r"
assert_empty "XX absent key: not created"            "$($CLI GET sgd:xx:absent)"

# ════════════════════════════════════════════════════════════════════
section "10  GET / SET after server restart"
# ════════════════════════════════════════════════════════════════════

v=$(make_val "sgd:restart")
$CLI SET "sgd:restart" "$v" >/dev/null

stop_server
start_server

# Without persistence configured, key should be gone
assert_empty "Key absent after cold restart (no persistence)" \
    "$($CLI GET sgd:restart)"

# ════════════════════════════════════════════════════════════════════
section "11  High-volume SET correctness (1 000 keys)"
# ════════════════════════════════════════════════════════════════════

for i in $(seq 1 1000); do
    $CLI SET "sgd:bulk:$i" "$(make_val sgd:bulk:$i)" >/dev/null
done

mismatch=0
for i in $(seq 1 1000); do
    got=$($CLI GET "sgd:bulk:$i")
    expected=$(make_val "sgd:bulk:$i")
    [[ "$got" != "$expected" ]] && mismatch=$(( mismatch + 1 ))
done
assert_eq "1000 bulk SET/GET — zero mismatches" "0" "$mismatch"

print_summary "test_set_get_del"

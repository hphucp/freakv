#!/usr/bin/env bash
# test_ttl_expiry.sh — TTL / EXPIRE / PEXPIRE — exhaustive correctness
# All commands go through the shared port; no internal shard port assumed.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}╔══ TEST: TTL / EXPIRY ══╗${RST}\n\n"

start_server
trap stop_server EXIT

# ════════════════════════════════════════════════════════════════════
section "1  SET EX — key exists then expires"
# ════════════════════════════════════════════════════════════════════

$CLI SET ttl:ex "$(make_val ttl:ex)" EX 2 >/dev/null
assert_eq   "Key present before EX expiry" "$(make_val ttl:ex)" "$($CLI GET ttl:ex)"
sleep 6
assert_empty "Key gone after EX 2 + sleep 6"  "$($CLI GET ttl:ex)"

# ════════════════════════════════════════════════════════════════════
section "2  SET PX — millisecond expiry"
# ════════════════════════════════════════════════════════════════════

$CLI SET ttl:px "$(make_val ttl:px)" PX 1500 >/dev/null
assert_eq    "Key present before PX expiry" "$(make_val ttl:px)" "$($CLI GET ttl:px)"
sleep 2
assert_empty "Key gone after PX 1500 + sleep 2"  "$($CLI GET ttl:px)"

# ════════════════════════════════════════════════════════════════════
section "3  TTL / PTTL accuracy"
# ════════════════════════════════════════════════════════════════════

$CLI SET ttl:acc "v" EX 10 >/dev/null
assert_range "TTL immediately ∈ [8,10]"   8     10     "$($CLI TTL  ttl:acc)"
assert_range "PTTL immediately ∈ [8000,10100]" 8000 10100 "$($CLI PTTL ttl:acc)"

sleep 3
assert_range "TTL after 3s ∈ [5,8]"      5     8      "$($CLI TTL  ttl:acc)"
assert_range "PTTL after 3s ∈ [4000,8100]" 4000 8100  "$($CLI PTTL ttl:acc)"

# ════════════════════════════════════════════════════════════════════
section "4  EXPIRE / PEXPIRE return values and effect"
# ════════════════════════════════════════════════════════════════════

$CLI SET ttl:expire "v" >/dev/null
assert_eq  "EXPIRE on existing key → 1"    "1" "$($CLI EXPIRE  ttl:expire  5)"
assert_range "TTL after EXPIRE 5 ∈ [3,5]" 3 5  "$($CLI TTL    ttl:expire)"

assert_eq  "EXPIRE on absent key → 0"     "0" "$($CLI EXPIRE  ttl:no_key_xyz 5)"

$CLI SET ttl:pexpire "v" >/dev/null
assert_eq  "PEXPIRE on existing key → 1"   "1" "$($CLI PEXPIRE ttl:pexpire 5000)"
assert_range "PTTL after PEXPIRE 5000 ∈ [3000,5100]" 3000 5100 \
    "$($CLI PTTL ttl:pexpire)"

# ════════════════════════════════════════════════════════════════════
section "5  TTL edge cases — -1 and -2"
# ════════════════════════════════════════════════════════════════════

assert_eq  "TTL non-existent key → -2"    "-2" "$($CLI TTL  ttl:missing:xyz)"
assert_eq  "PTTL non-existent key → -2"   "-2" "$($CLI PTTL ttl:missing:xyz)"

$CLI SET ttl:no_ttl "v" >/dev/null
assert_eq  "TTL key without TTL → -1"     "-1" "$($CLI TTL  ttl:no_ttl)"
assert_eq  "PTTL key without TTL → -1"    "-1" "$($CLI PTTL ttl:no_ttl)"

# ════════════════════════════════════════════════════════════════════
section "6  Overwrite clears / replaces TTL"
# ════════════════════════════════════════════════════════════════════

# SET without EX clears existing TTL
$CLI SET ttl:ow1 "v1" EX 30 >/dev/null
$CLI SET ttl:ow1 "v2"       >/dev/null
assert_eq  "SET without EX clears TTL → -1" "-1" "$($CLI TTL ttl:ow1)"

# SET with new EX replaces TTL
$CLI SET ttl:ow2 "v1" EX 10 >/dev/null
$CLI SET ttl:ow2 "v2" EX 20 >/dev/null
assert_range "SET EX 20 over EX 10 → TTL ∈ [18,20]" 18 20 "$($CLI TTL ttl:ow2)"

# ════════════════════════════════════════════════════════════════════
section "7  KEEPTTL"
# ════════════════════════════════════════════════════════════════════

$CLI SET ttl:keep "$(make_val ttl:keep:v1)" EX 30 >/dev/null
sleep 1
$CLI SET ttl:keep "$(make_val ttl:keep:v2)" KEEPTTL >/dev/null
assert_eq  "KEEPTTL: new value visible"      "$(make_val ttl:keep:v2)" "$($CLI GET  ttl:keep)"
assert_range "KEEPTTL: TTL preserved ∈ [25,30]" 25 30 "$($CLI TTL ttl:keep)"

# ════════════════════════════════════════════════════════════════════
section "8  PERSIST removes TTL"
# ════════════════════════════════════════════════════════════════════

$CLI SET ttl:persist "v" EX 30 >/dev/null
assert_eq  "PERSIST on TTL key → 1"       "1"  "$($CLI PERSIST ttl:persist)"
assert_eq  "After PERSIST TTL → -1"       "-1" "$($CLI TTL ttl:persist)"
assert_eq  "PERSIST on key without TTL → 0" "0" "$($CLI PERSIST ttl:persist)"
assert_eq  "PERSIST on absent key → 0"    "0"  "$($CLI PERSIST ttl:missing:xyz)"

# ════════════════════════════════════════════════════════════════════
section "9  EXPIREAT / PEXPIREAT"
# ════════════════════════════════════════════════════════════════════

now=$(date +%s)

$CLI SET ttl:eat "v" >/dev/null
future=$(( now + 30 ))
assert_eq  "EXPIREAT future → 1"          "1"  "$($CLI EXPIREAT  ttl:eat  $future)"
assert_range "TTL after EXPIREAT ∈ [27,30]" 27 30 "$($CLI TTL ttl:eat)"

$CLI SET ttl:peat "v" >/dev/null
future_ms=$(( (now + 30) * 1000 ))
assert_eq  "PEXPIREAT future → 1"         "1"  "$($CLI PEXPIREAT ttl:peat $future_ms)"
assert_range "PTTL after PEXPIREAT ∈ [27000,30100]" 27000 30100 "$($CLI PTTL ttl:peat)"

# EXPIREAT in the past → key expires immediately
$CLI SET ttl:past "v" >/dev/null
past=$(( now - 1 ))
$CLI EXPIREAT ttl:past "$past" >/dev/null
sleep 0.2
assert_empty "EXPIREAT in past → key gone"  "$($CLI GET ttl:past)"

# ════════════════════════════════════════════════════════════════════
section "10  Cross-shard TTL propagation"
# ════════════════════════════════════════════════════════════════════

for i in $(seq 1 20); do
    $CLI SET "ttl:xs:$i" "$(make_val ttl:xs:$i)" EX 2 >/dev/null
done

# Verify all present before expiry
ok=0
for i in $(seq 1 20); do
    got=$($CLI GET "ttl:xs:$i")
    [[ "$got" == "$(make_val ttl:xs:$i)" ]] && ok=$(( ok + 1 ))
done
assert_eq "20 cross-shard TTL keys present before expiry" "20" "$ok"

sleep 3

# Verify all expired
expired=0
for i in $(seq 1 20); do
    got=$($CLI GET "ttl:xs:$i")
    [[ -z "$got" ]] && expired=$(( expired + 1 ))
done
assert_eq "20 cross-shard TTL keys expired after TTL" "20" "$expired"

# ════════════════════════════════════════════════════════════════════
section "11  Active expiry drain (200 short-lived keys)"
# ════════════════════════════════════════════════════════════════════

flush_all

for i in $(seq 1 200); do
    $CLI SET "ttl:drain:$i" "$(make_val ttl:drain:$i)" EX 4 >/dev/null
done

size_before=$($CLI DBSIZE)
assert_ge "DBSIZE ≥ 200 before expiry" "$size_before" 200

sleep 6

size_after=$($CLI DBSIZE)
decrease=$(( size_before - size_after ))
assert_ge "Active expiry reduced DBSIZE by ≥ 180" "$decrease" 180

# ════════════════════════════════════════════════════════════════════
section "12  Very short PX (100ms) expiry precision"
# ════════════════════════════════════════════════════════════════════

$CLI SET ttl:fast "v" PX 100 >/dev/null
assert_notempty "Key present at T+0ms"        "$($CLI GET ttl:fast)"
sleep 0.5
assert_empty    "Key gone at T+500ms (PX 100)"  "$($CLI GET ttl:fast)"

# ════════════════════════════════════════════════════════════════════
section "13  EXPIRE resets existing TTL"
# ════════════════════════════════════════════════════════════════════

$CLI SET ttl:reset "v" EX 5 >/dev/null
sleep 1
$CLI EXPIRE ttl:reset 20 >/dev/null
assert_range "TTL after EXPIRE reset ∈ [17,20]" 17 20 "$($CLI TTL ttl:reset)"

# ════════════════════════════════════════════════════════════════════
section "14  DEL on expired key"
# ════════════════════════════════════════════════════════════════════

$CLI SET ttl:delexp "v" PX 200 >/dev/null
sleep 0.5
assert_eq "DEL on expired key → 0" "0" "$($CLI DEL ttl:delexp)"

print_summary "test_ttl_expiry"

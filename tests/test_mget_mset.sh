#!/usr/bin/env bash
# test_mget_mset.sh — MGET / MSET — exhaustive correctness
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}╔══ TEST: MGET / MSET ══╗${RST}\n\n"

start_server
trap stop_server EXIT

# ════════════════════════════════════════════════════════════════════
section "1  Basic MSET/MGET round-trip"
# ════════════════════════════════════════════════════════════════════

v1=$(make_val "mm:1"); v2=$(make_val "mm:2"); v3=$(make_val "mm:3")
assert_eq "MSET 3 keys → OK" "OK" "$($CLI MSET mm:1 "$v1" mm:2 "$v2" mm:3 "$v3")"

mapfile -t got < <($CLI MGET mm:1 mm:2 mm:3)
assert_eq "MGET[0] correct" "$v1" "${got[0]}"
assert_eq "MGET[1] correct" "$v2" "${got[1]}"
assert_eq "MGET[2] correct" "$v3" "${got[2]}"

# ════════════════════════════════════════════════════════════════════
section "2  Result count matches request count"
# ════════════════════════════════════════════════════════════════════

# Request N keys → must get exactly N lines back (including nil for misses)
$CLI SET "mm:cnt:1" "a" >/dev/null
# mm:cnt:2 intentionally missing
$CLI SET "mm:cnt:3" "c" >/dev/null

mapfile -t got < <($CLI MGET mm:cnt:1 mm:cnt:2 mm:cnt:3)
assert_eq "MGET 3 keys → exactly 3 results" "3" "${#got[@]}"

# ════════════════════════════════════════════════════════════════════
section "3  Mixed hit / miss ordering"
# ════════════════════════════════════════════════════════════════════

$CLI SET "mm:h1" "$(make_val mm:h1)" >/dev/null
# mm:m1 missing
$CLI SET "mm:h2" "$(make_val mm:h2)" >/dev/null
# mm:m2 missing
$CLI SET "mm:h3" "$(make_val mm:h3)" >/dev/null

mapfile -t got < <($CLI MGET mm:h1 mm:m1 mm:h2 mm:m2 mm:h3)
assert_eq   "MGET pos 0: hit"   "$(make_val mm:h1)" "${got[0]}"
assert_empty "MGET pos 1: miss"                      "${got[1]}"
assert_eq   "MGET pos 2: hit"   "$(make_val mm:h2)" "${got[2]}"
assert_empty "MGET pos 3: miss"                      "${got[3]}"
assert_eq   "MGET pos 4: hit"   "$(make_val mm:h3)" "${got[4]}"

# ════════════════════════════════════════════════════════════════════
section "4  All-miss MGET"
# ════════════════════════════════════════════════════════════════════

mapfile -t got < <($CLI MGET no:key:a no:key:b no:key:c)
assert_eq   "All-miss: 3 results"     "3"  "${#got[@]}"
assert_empty "All-miss pos 0: empty"        "${got[0]}"
assert_empty "All-miss pos 1: empty"        "${got[1]}"
assert_empty "All-miss pos 2: empty"        "${got[2]}"

# ════════════════════════════════════════════════════════════════════
section "5  Cross-shard scatter (20 keys)"
# ════════════════════════════════════════════════════════════════════

args=()
for i in $(seq 1 20); do args+=("cs:$i" "$(make_val cs:$i)"); done
$CLI MSET "${args[@]}" >/dev/null

keys=(); for i in $(seq 1 20); do keys+=("cs:$i"); done
mapfile -t got < <($CLI MGET "${keys[@]}")

mismatch=0
for i in $(seq 0 19); do
    key="cs:$((i+1))"
    [[ "${got[$i]}" != "$(make_val $key)" ]] && mismatch=$(( mismatch + 1 ))
done
assert_eq "20 cross-shard MGET — zero mismatches" "0" "$mismatch"

# ════════════════════════════════════════════════════════════════════
section "6  Large MGET (500 keys)"
# ════════════════════════════════════════════════════════════════════

args=()
for i in $(seq 1 500); do args+=("lg:$i" "$(make_val lg:$i)"); done
$CLI MSET "${args[@]}" >/dev/null

keys=(); for i in $(seq 1 500); do keys+=("lg:$i"); done
mapfile -t got < <($CLI MGET "${keys[@]}")

assert_eq "Large MGET: 500 results returned" "500" "${#got[@]}"
mismatch=0
for i in $(seq 0 499); do
    key="lg:$((i+1))"
    [[ "${got[$i]}" != "$(make_val $key)" ]] && mismatch=$(( mismatch + 1 ))
done
assert_eq "Large MGET (500): zero mismatches" "0" "$mismatch"

# ════════════════════════════════════════════════════════════════════
section "7  MSET overwrite atomicity"
# ════════════════════════════════════════════════════════════════════

$CLI MSET ow:a "$(make_val ow:a:v1)" ow:b "$(make_val ow:b:v1)" >/dev/null
$CLI MSET ow:a "$(make_val ow:a:v2)" ow:b "$(make_val ow:b:v2)" >/dev/null

mapfile -t got < <($CLI MGET ow:a ow:b)
assert_eq "MSET overwrite key a" "$(make_val ow:a:v2)" "${got[0]}"
assert_eq "MSET overwrite key b" "$(make_val ow:b:v2)" "${got[1]}"

# ════════════════════════════════════════════════════════════════════
section "8  MSET then individual GET"
# ════════════════════════════════════════════════════════════════════

$CLI MSET "ind:1" "alpha" "ind:2" "beta" "ind:3" "gamma" >/dev/null
assert_eq "Individual GET after MSET [1]" "alpha" "$($CLI GET ind:1)"
assert_eq "Individual GET after MSET [2]" "beta"  "$($CLI GET ind:2)"
assert_eq "Individual GET after MSET [3]" "gamma" "$($CLI GET ind:3)"

# ════════════════════════════════════════════════════════════════════
section "9  Individual SET then MGET"
# ════════════════════════════════════════════════════════════════════

$CLI SET "mix:1" "one"   >/dev/null
$CLI SET "mix:2" "two"   >/dev/null
$CLI SET "mix:3" "three" >/dev/null

mapfile -t got < <($CLI MGET mix:1 mix:2 mix:3)
assert_eq "MGET after individual SETs [0]" "one"   "${got[0]}"
assert_eq "MGET after individual SETs [1]" "two"   "${got[1]}"
assert_eq "MGET after individual SETs [2]" "three" "${got[2]}"

# ════════════════════════════════════════════════════════════════════
section "10  MGET ordering stress (100 keys, 10 repeated runs)"
# ════════════════════════════════════════════════════════════════════

for i in $(seq 1 100); do
    $CLI SET "ord:$i" "$(make_val ord:$i)" >/dev/null
done

keys=(); for i in $(seq 1 100); do keys+=("ord:$i"); done

total_mismatch=0
for run in $(seq 1 10); do
    mapfile -t got < <($CLI MGET "${keys[@]}")
    for i in $(seq 0 99); do
        key="ord:$((i+1))"
        [[ "${got[$i]}" != "$(make_val $key)" ]] && total_mismatch=$(( total_mismatch + 1 ))
    done
done
assert_eq "MGET ordering: 0 mismatches across 10 repeated runs (1000 checks)" \
    "0" "$total_mismatch"

# ════════════════════════════════════════════════════════════════════
section "11  Duplicate keys in MGET"
# ════════════════════════════════════════════════════════════════════

$CLI SET "dup:k" "hello" >/dev/null
mapfile -t got < <($CLI MGET dup:k dup:k dup:k)
assert_eq "Duplicate key in MGET: 3 results" "3" "${#got[@]}"
assert_eq "Duplicate key result [0]" "hello" "${got[0]}"
assert_eq "Duplicate key result [1]" "hello" "${got[1]}"
assert_eq "Duplicate key result [2]" "hello" "${got[2]}"

print_summary "test_mget_mset"

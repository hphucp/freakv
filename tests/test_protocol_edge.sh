#!/usr/bin/env bash
# test_protocol_edge.sh — Raw RESP protocol edge cases
# Uses raw nc to send malformed / boundary-case bytes, bypassing redis-cli.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

[[ "$_TAP_MODE" == "1" ]] || printf "${CYN}╔══ TEST: PROTOCOL EDGE CASES ══╗${RST}\n\n"

start_server
trap stop_server EXIT

# Helper: send raw payload to server, capture response
_raw() {
    printf '%b' "$1" | nc -q 1 127.0.0.1 "$PORT" 2>/dev/null || true
}

# Verify server is up first
assert_eq "Server reachable before protocol tests" "PONG" "$($CLI PING)"

# ════════════════════════════════════════════════════════════════════
section "1  Inline command (PING without RESP framing)"
# ════════════════════════════════════════════════════════════════════

r=$(_raw "PING\r\n")
assert_contains "Inline PING → +PONG" "+PONG" "$r"

# ════════════════════════════════════════════════════════════════════
section "2  RESP PING"
# ════════════════════════════════════════════════════════════════════

r=$(_raw "*1\r\n\$4\r\nPING\r\n")
assert_contains "RESP PING → +PONG" "+PONG" "$r"

# ════════════════════════════════════════════════════════════════════
section "3  Multiple pipelined commands in single TCP write"
# ════════════════════════════════════════════════════════════════════

# SET foo bar \n GET foo — both in one send
r=$(_raw "*3\r\n\$3\r\nSET\r\n\$3\r\nfoo\r\n\$3\r\nbar\r\n*2\r\n\$3\r\nGET\r\n\$3\r\nfoo\r\n")
assert_contains "Pipeline SET: +OK present"  "+OK"  "$r"
assert_contains "Pipeline GET: \$3/bar present" "bar" "$r"

# ════════════════════════════════════════════════════════════════════
section "4  Empty bulk string value ($0)"
# ════════════════════════════════════════════════════════════════════

r=$(_raw "*3\r\n\$3\r\nSET\r\n\$8\r\nproto:e1\r\n\$0\r\n\r\n")
assert_contains "SET empty bulk string → +OK" "+OK" "$r"

assert_eq "GET after SET empty value → empty" "" "$($CLI GET proto:e1)"
assert_eq "STRLEN after SET empty value → 0"  "0" "$($CLI STRLEN proto:e1)"

# ════════════════════════════════════════════════════════════════════
section "5  Null bulk string in MGET response"
# ════════════════════════════════════════════════════════════════════
# Requesting a missing key via raw MGET must return $-1 (null bulk)

r=$(_raw "*2\r\n\$4\r\nMGET\r\n\$15\r\nno_such_key_xyz\r\n")
assert_contains "MGET missing key → \$-1 null bulk" '$-1' "$r"

# ════════════════════════════════════════════════════════════════════
section "6  Large key name (512 bytes)"
# ════════════════════════════════════════════════════════════════════

long_key=$(printf '%0512d' 0 | tr '0' 'k')
$CLI SET "$long_key" "bigkey_val" >/dev/null
assert_eq "GET 512-byte key name" "bigkey_val" "$($CLI GET $long_key)"

# ════════════════════════════════════════════════════════════════════
section "7  Command with extra whitespace / CRLF variations"
# ════════════════════════════════════════════════════════════════════

# Send with \n only (no \r) — server should still handle it
r=$(_raw "*1\n\$4\nPING\n")
# Some servers accept LF-only; result may be +PONG or an error.
# We just assert the server doesn't crash (next PING via redis-cli must work).
assert_eq "Server alive after LF-only framing attempt" "PONG" "$($CLI PING)"

# ════════════════════════════════════════════════════════════════════
section "8  Pipelining 50 GETs in one send"
# ════════════════════════════════════════════════════════════════════

# First set 50 keys
for i in $(seq 1 50); do $CLI SET "pp:$i" "v$i" >/dev/null; done

# Build 50 GET commands in one blob
payload=""
for i in $(seq 1 50); do
    payload+="*2\r\n\$3\r\nGET\r\n\$$(echo -n "pp:$i" | wc -c)\r\npp:$i\r\n"
done

r=$(_raw "$payload")
# Count the number of "$" responses (bulk strings) — should be 50
count=$(echo "$r" | grep -c '^\$' || true)
assert_ge "Pipeline 50 GETs: ≥ 50 bulk responses" "$count" 50

# ════════════════════════════════════════════════════════════════════
section "9  Connection survives garbage input"
# ════════════════════════════════════════════════════════════════════

# Send binary garbage — server should reject gracefully, not crash
_raw "\xff\xfe\x00\x01garbage\r\n" >/dev/null 2>&1 || true
sleep 0.1
# Open a fresh connection and verify server responds
assert_eq "Server alive after garbage input" "PONG" "$($CLI PING)"

# ════════════════════════════════════════════════════════════════════
section "10  Zero-length key"
# ════════════════════════════════════════════════════════════════════

r=$(_raw "*3\r\n\$3\r\nSET\r\n\$0\r\n\r\n\$5\r\nhello\r\n")
# Some implementations reject zero-length keys, others allow them.
# Either response is acceptable as long as the server stays alive.
assert_eq "Server alive after zero-length key attempt" "PONG" "$($CLI PING)"

print_summary "test_protocol_edge"

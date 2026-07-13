#!/bin/sh
# Scenario 1: change tool-a's upstream subject/version with NO rollout.
# Proves: all three tool containers keep their IDs and start times, tool-a's durable is
# updated IN PLACE (same name/creation, cursor not reset), and tool-b/tool-c keep advancing.
set -eu
cd "$(dirname "$0")/.."
. demo/demo-lib.sh
demo_require_runtime

BACKUP=$(mktemp)
cp config/sessions.yaml "$BACKUP"
cleanup() {
  DEMO_STATUS=$?
  trap - EXIT INT TERM
  if [ -f "$BACKUP" ]; then cp "$BACKUP" config/sessions.yaml || DEMO_STATUS=1; rm -f "$BACKUP"; fi
  demo_wait_state tool-a ACTIVE 40 >/dev/null 2>&1 || true
  exit "$DEMO_STATUS"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Scenario 1: safe upstream subject change (tool.a.events -> tool.a.events.v2), no rollout."

# baseline: known-good config, all three ACTIVE and consuming so the tool-a cursor is non-zero.
demo_write_config tool.a.events v1 RUNNING
demo_wait_config tool-a tool.a.events v1 40
demo_wait_state tool-a ACTIVE 40
demo_wait_state tool-b ACTIVE 40
demo_wait_state tool-c ACTIVE 40
sleep 6

# capture BEFORE identities: container id + start time for every tool, tool-a durable, admitted counts.
A_CID=$(demo_cid listener-tool-a); B_CID=$(demo_cid listener-tool-b); C_CID=$(demo_cid listener-tool-c)
A_START=$(demo_started listener-tool-a); B_START=$(demo_started listener-tool-b); C_START=$(demo_started listener-tool-c)
INFO=$(demo_consumer_info listener-tool-a)
[ -n "$INFO" ] || { echo "FAIL: cannot read durable info for listener-tool-a"; exit 1; }
DUR_NAME=$(printf '%s' "$INFO" | jq -r '.name')
DUR_CREATED=$(printf '%s' "$INFO" | jq -r '.created')
DUR_CURSOR=$(printf '%s' "$INFO" | jq -r '.ack_floor.stream_seq')
B_ADMITTED=$(demo_admitted_of tool-b); C_ADMITTED=$(demo_admitted_of tool-c)
A_ADMITTED=$(demo_admitted_of tool-a)
echo "BEFORE: a=$A_CID b=$B_CID c=$C_CID"
echo "BEFORE: durable=$DUR_NAME created=$DUR_CREATED cursor=$DUR_CURSOR"
demo_show_status

demo_pause "Change ONLY tool-a: subject -> tool.a.events.v2, configVersion -> v2 (b/c untouched)."
demo_write_config tool.a.events.v2 v2 RUNNING
demo_wait_config tool-a tool.a.events.v2 v2 40
demo_wait_state tool-a ACTIVE 40

# feed the new subject so admitted advances on the SAME durable (its filter was updated in place).
demo_publish tool.a.events.v2 5
demo_wait_admitted_at_least tool-a "$((A_ADMITTED + 5))" 60

# capture AFTER identities and assert every invariant.
A_CID2=$(demo_cid listener-tool-a); B_CID2=$(demo_cid listener-tool-b); C_CID2=$(demo_cid listener-tool-c)
A_START2=$(demo_started listener-tool-a); B_START2=$(demo_started listener-tool-b); C_START2=$(demo_started listener-tool-c)
INFO2=$(demo_consumer_info listener-tool-a)
DUR_NAME2=$(printf '%s' "$INFO2" | jq -r '.name')
DUR_CREATED2=$(printf '%s' "$INFO2" | jq -r '.created')
DUR_CURSOR2=$(printf '%s' "$INFO2" | jq -r '.ack_floor.stream_seq')
DUR_FILTER2=$(printf '%s' "$INFO2" | jq -r '.config.filter_subject')

[ "$A_CID" = "$A_CID2" ] && [ "$A_START" = "$A_START2" ] || { echo "FAIL: tool-a container changed"; exit 1; }
[ "$B_CID" = "$B_CID2" ] && [ "$B_START" = "$B_START2" ] || { echo "FAIL: tool-b container changed"; exit 1; }
[ "$C_CID" = "$C_CID2" ] && [ "$C_START" = "$C_START2" ] || { echo "FAIL: tool-c container changed"; exit 1; }
echo "OK: a/b/c container IDs and start times unchanged (dynamic change, no rollout)"

[ "$DUR_NAME2" = "$DUR_NAME" ] || { echo "FAIL: durable name changed"; exit 1; }
[ "$DUR_CREATED2" = "$DUR_CREATED" ] || { echo "FAIL: durable creation timestamp changed -> recreated"; exit 1; }
[ "$DUR_FILTER2" = "tool.a.events.v2" ] || { echo "FAIL: durable filter not updated in place ($DUR_FILTER2)"; exit 1; }
[ "$DUR_CURSOR2" -ge "$DUR_CURSOR" ] 2>/dev/null || { echo "FAIL: durable cursor reset ($DUR_CURSOR -> $DUR_CURSOR2)"; exit 1; }
echo "OK: same durable $DUR_NAME updated in place (created=$DUR_CREATED, cursor $DUR_CURSOR -> $DUR_CURSOR2)"

demo_wait_state tool-b ACTIVE 10
demo_wait_state tool-c ACTIVE 10
demo_wait_admitted_at_least tool-b "$((B_ADMITTED + 1))" 15
demo_wait_admitted_at_least tool-c "$((C_ADMITTED + 1))" 15
echo "OK: tool-b/tool-c stayed ACTIVE and kept advancing"
demo_show_status

echo "Scenario 1 done: subject/version change applied in place, zero rollout across all three tools."

#!/bin/sh
# Scenario 3: PROCESS/Pod isolation. Stop the whole listener-tool-b workload (not just its
# config) and prove tool-a/tool-c keep running: still ACTIVE, admitted counts advance, and
# their container IDs/start times never change. Then restart tool-b and prove recovery.
# This validates process/Pod isolation, not in-JVM session isolation.
set -eu
cd "$(dirname "$0")/.."
. demo/demo-lib.sh
demo_require_runtime

DEMO_B_STOPPED=0
restore_b() {
  DEMO_STATUS=$?
  trap - EXIT INT TERM
  [ "$DEMO_B_STOPPED" = "1" ] && docker compose start listener-tool-b >/dev/null 2>&1 || true
  exit "$DEMO_STATUS"
}
trap restore_b EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

demo_wait_state tool-a ACTIVE 40
demo_wait_state tool-c ACTIVE 40
A_CID=$(demo_cid listener-tool-a); C_CID=$(demo_cid listener-tool-c)
A_START=$(demo_started listener-tool-a); C_START=$(demo_started listener-tool-c)
A_ADMITTED=$(demo_admitted_of tool-a); C_ADMITTED=$(demo_admitted_of tool-c)
echo "BEFORE: tool-a=$A_CID tool-c=$C_CID"

demo_pause "Stop the entire listener-tool-b container; tool-a/tool-c must keep processing."
docker compose stop listener-tool-b
DEMO_B_STOPPED=1

# tool-b's own endpoint should now be unreachable (its process is gone, not just paused).
if curl -sf "$(demo_url_of tool-b)" >/dev/null 2>&1; then
  echo "FAIL: tool-b endpoint still answering after its container was stopped"; exit 1
fi
echo "OK: tool-b endpoint (:8082) is down; its process was stopped"

# tool-a/tool-c keep advancing while tool-b is down.
demo_wait_admitted_at_least tool-a "$((A_ADMITTED + 3))" 30
demo_wait_admitted_at_least tool-c "$((C_ADMITTED + 3))" 30
demo_wait_state tool-a ACTIVE 5
demo_wait_state tool-c ACTIVE 5
[ "$A_CID" = "$(demo_cid listener-tool-a)" ] && [ "$A_START" = "$(demo_started listener-tool-a)" ] \
  || { echo "FAIL: tool-a container changed while tool-b was down"; exit 1; }
[ "$C_CID" = "$(demo_cid listener-tool-c)" ] && [ "$C_START" = "$(demo_started listener-tool-c)" ] \
  || { echo "FAIL: tool-c container changed while tool-b was down"; exit 1; }
echo "OK: tool-a/tool-c stayed ACTIVE, advanced, and kept their container IDs/start times"
demo_show_status

demo_pause "Restart listener-tool-b and prove it recovers to ACTIVE from its durable."
docker compose start listener-tool-b
DEMO_B_STOPPED=0
demo_wait_state tool-b ACTIVE 90
[ "$A_CID" = "$(demo_cid listener-tool-a)" ] || { echo "FAIL: tool-a restarted during tool-b recovery"; exit 1; }
[ "$C_CID" = "$(demo_cid listener-tool-c)" ] || { echo "FAIL: tool-c restarted during tool-b recovery"; exit 1; }
demo_show_status

echo "Scenario 3 done: stopping one tool's process left the others untouched (Pod-level isolation)."

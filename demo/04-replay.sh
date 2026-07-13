#!/bin/sh
# Scenario 4: STANDBY holds the durable but stops fetching; backlog accumulates server-side.
# Reach STANDBY first, THEN capture the admitted baseline and the ACTUAL pending backlog,
# resume RUNNING, and wait for admittedCount >= baseline + capturedPending.
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

demo_write_config tool.a.events v1 RUNNING
demo_wait_state tool-a ACTIVE 40

demo_pause "Pause tool-a: set desiredState -> STANDBY (backlog will build up)."
demo_write_config tool.a.events v1 STANDBY
demo_wait_state tool-a STANDBY 30

# STANDBY does not fetch, so admittedCount is frozen: capture it, then the real pending backlog.
ADMITTED_BASELINE=$(demo_admitted_of tool-a)
echo "Waiting for a real backlog to accumulate on the durable..."
demo_wait_pending_at_least tool-a 10 40
CAPTURED_PENDING=$(demo_pending_of tool-a)
REPLAY_TARGET=$((ADMITTED_BASELINE + CAPTURED_PENDING))
echo "STANDBY baseline: admitted=$ADMITTED_BASELINE pending=$CAPTURED_PENDING -> replay target=$REPLAY_TARGET"
demo_show_status

demo_pause "Resume tool-a: set desiredState -> RUNNING; the captured backlog replays from the durable."
demo_write_config tool.a.events v1 RUNNING
demo_wait_state tool-a ACTIVE 30
demo_wait_admitted_at_least tool-a "$REPLAY_TARGET" 60
demo_wait_pending_zero tool-a 60
demo_show_status

echo "Scenario 4 done: no fetch during STANDBY; on resume the durable replayed the full captured backlog."

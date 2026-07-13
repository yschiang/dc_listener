#!/bin/sh
# Scenario 5: onboarding a new tool is a WORKLOAD operation, not an in-process config trick.
# Adding tool-d's declaration alone does nothing: a new process/container must be created.
# We add the YAML entry, show port 8084 stays absent, then explicitly start the tool-d
# workload under the onboarding profile and prove tool-a/b/c are untouched.
#
# Cleanup stops/removes ONLY the tool-d demo workload and restores the config. It does NOT
# offboard permanently and does NOT delete tool-d's durable consumer (normal shutdown retains
# the durable, per ADR-0001). In production the controller performs this workload operation:
# a ToolListener declaration makes the controller create the workload; a finalizer-driven
# offboarding is the only path that deletes the durable.
set -eu
cd "$(dirname "$0")/.."
. demo/demo-lib.sh
demo_require_runtime

BACKUP=$(mktemp)
cp config/sessions.yaml "$BACKUP"
cleanup() {
  DEMO_STATUS=$?
  trap - EXIT INT TERM
  # remove ONLY the tool-d demo workload (durable is retained, not deleted), then restore config.
  docker compose rm -sf listener-tool-d >/dev/null 2>&1 || true
  if [ -f "$BACKUP" ]; then cp "$BACKUP" config/sessions.yaml || DEMO_STATUS=1; rm -f "$BACKUP"; fi
  demo_wait_state tool-a ACTIVE 40 >/dev/null 2>&1 || true
  exit "$DEMO_STATUS"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Scenario 5: onboarding tool-d as a new workload (not an in-process add)."

# precondition: tool-d must be absent (no process, port 8084 not answering).
if curl -sf "$(demo_url_of tool-d)" >/dev/null 2>&1; then
  echo "FAIL: port 8084 already answering; tool-d workload must be absent at start"; exit 1
fi
[ -z "$(docker compose ps -q listener-tool-d 2>/dev/null)" ] \
  || { echo "FAIL: a listener-tool-d container already exists"; exit 1; }
echo "OK: tool-d and port 8084 are absent to start"

# capture a/b/c identities so we can prove onboarding does not redeploy existing tools.
A_CID=$(demo_cid listener-tool-a); B_CID=$(demo_cid listener-tool-b); C_CID=$(demo_cid listener-tool-c)
A_START=$(demo_started listener-tool-a); B_START=$(demo_started listener-tool-b); C_START=$(demo_started listener-tool-c)

demo_pause "Add tool-d's declaration to config/sessions.yaml (desired entry only)."
demo_write_config tool.a.events v1 RUNNING with-tool-d
sleep 5

# YAML alone must NOT create a process: no controller/operator has generated the workload yet.
if curl -sf "$(demo_url_of tool-d)" >/dev/null 2>&1; then
  echo "FAIL: port 8084 answered from a YAML edit alone; no workload should exist yet"; exit 1
fi
[ "$A_CID" = "$(demo_cid listener-tool-a)" ] && [ "$A_START" = "$(demo_started listener-tool-a)" ] \
  || { echo "FAIL: tool-a changed from a YAML edit"; exit 1; }
[ "$B_CID" = "$(demo_cid listener-tool-b)" ] && [ "$B_START" = "$(demo_started listener-tool-b)" ] \
  || { echo "FAIL: tool-b changed from a YAML edit"; exit 1; }
[ "$C_CID" = "$(demo_cid listener-tool-c)" ] && [ "$C_START" = "$(demo_started listener-tool-c)" ] \
  || { echo "FAIL: tool-c changed from a YAML edit"; exit 1; }
echo "OK: adding YAML alone left port 8084 absent and did not touch tool-a/b/c"

demo_pause "Explicitly create the tool-d workload: docker compose --profile onboarding up -d listener-tool-d"
docker compose --profile onboarding up -d listener-tool-d
demo_wait_state tool-d ACTIVE 90

# the new endpoint exposes ONLY tool-d (single-session ownership).
KEYS=$(curl -sf "$(demo_url_of tool-d)" | jq -r '.sessions | keys | join(",")')
[ "$KEYS" = "tool-d" ] || { echo "FAIL: :8084 exposes [$KEYS], want only [tool-d]"; exit 1; }
echo "OK: tool-d ACTIVE on :8084, exposing only its own tool"

# onboarding a new workload must not redeploy the existing tools.
[ "$A_CID" = "$(demo_cid listener-tool-a)" ] && [ "$A_START" = "$(demo_started listener-tool-a)" ] \
  || { echo "FAIL: tool-a redeployed by onboarding"; exit 1; }
[ "$B_CID" = "$(demo_cid listener-tool-b)" ] && [ "$B_START" = "$(demo_started listener-tool-b)" ] \
  || { echo "FAIL: tool-b redeployed by onboarding"; exit 1; }
[ "$C_CID" = "$(demo_cid listener-tool-c)" ] && [ "$C_START" = "$(demo_started listener-tool-c)" ] \
  || { echo "FAIL: tool-c redeployed by onboarding"; exit 1; }
echo "OK: tool-a/b/c identities unchanged; onboarding created a new workload without rolling out existing tools"
demo_show_status

echo "Scenario 5 done: onboarding = create a new workload. Cleanup removes ONLY the tool-d workload and"
echo "retains its durable (no offboarding, no durable deletion). Production drives this through the controller."

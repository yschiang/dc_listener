#!/bin/sh
# Demo artifact contract：檔案齊全、可執行、POSIX shell 語法與 Compose config 有效。
set -eu
cd "$(dirname "$0")/.."

scripts="
demo/run-demo.sh
demo/watch-status.sh
demo/01-change-flow.sh
demo/02-degraded.sh
demo/03-isolation.sh
demo/04-replay.sh
demo/05-onboarding.sh
"

# Task 6 committed these demo artifacts, so the block is mandatory (no SKIP fallback).
for script in $scripts; do
  [ -x "$script" ] || { echo "FAIL: missing executable $script"; exit 1; }
  sh -n "$script"
done
[ -f demo/demo-lib.sh ] || { echo "FAIL: missing demo/demo-lib.sh"; exit 1; }
sh -n demo/demo-lib.sh
[ -s demo/README.md ] || { echo "FAIL: missing demo/README.md"; exit 1; }
demo/run-demo.sh help >/dev/null

# --- Demo script contract (Task 6): per-tool endpoints, aggregate status, service logs, S5 wording ---
runtime_scripts="demo/run-demo.sh demo/watch-status.sh demo/demo-lib.sh
demo/01-change-flow.sh demo/02-degraded.sh demo/03-isolation.sh demo/04-replay.sh demo/05-onboarding.sh"
for s in $runtime_scripts; do
  grep -q 'listener-runtime' "$s" && { echo "FAIL: $s references removed listener-runtime service"; exit 1; }
done

# Endpoint map tool-a->8081, tool-b->8082, tool-c->8083 (watch-status aggregates; demo-lib routes per tool).
check_port() { grep -q "$2" "$1" || { echo "FAIL: $1 missing endpoint port $2"; exit 1; }; }
check_port demo/watch-status.sh 8081
check_port demo/watch-status.sh 8082
check_port demo/watch-status.sh 8083
check_port demo/demo-lib.sh 8081
check_port demo/demo-lib.sh 8082
check_port demo/demo-lib.sh 8083

# Aggregate status output: single-snapshot mode plus a 2-second default poll.
grep -q -- '-1' demo/watch-status.sh || { echo "FAIL: watch-status.sh must support single-snapshot mode (-1)"; exit 1; }

# Service-specific logs: run-demo.sh exposes a logs command targeting per-tool services.
grep -q 'logs)' demo/run-demo.sh || { echo "FAIL: run-demo.sh missing a logs command"; exit 1; }
grep -q 'listener-tool-a' demo/run-demo.sh || { echo "FAIL: run-demo.sh must target per-tool services (listener-tool-a)"; exit 1; }

# Revised scenario-5 wording: workload operation via the onboarding profile + controller, no durable deletion.
grep -q -- '--profile onboarding' demo/05-onboarding.sh \
  || { echo "FAIL: scenario 5 must onboard via the onboarding compose profile"; exit 1; }
grep -qi 'controller' demo/05-onboarding.sh \
  || { echo "FAIL: scenario 5 must state production onboards through the controller"; exit 1; }
# must not actually delete a consumer (prose may say it does NOT; forbid the real commands only).
if grep -qE 'consumer rm|consumer delete|deleteConsumer|--profile onboarding down' demo/05-onboarding.sh; then
  echo "FAIL: scenario 5 must not delete the durable consumer / tear down offboarding"; exit 1
fi

docker compose config --quiet
command -v jq >/dev/null 2>&1 || { echo "FAIL: jq is required"; exit 1; }

# --- Compose contract (Task 5): one tool per service, obsolete listener-runtime gone ---
# NOTE: dash's `echo` builtin interprets backslash escapes (e.g. the \n inside
# JSON-escaped strings), corrupting JSON piped through it. Use `printf '%s\n'` instead.
DEFAULT_JSON="$(docker compose config --format json)"
ONBOARDING_JSON="$(docker compose --profile onboarding config --format json)"
default_services="$(printf '%s\n' "$DEFAULT_JSON" | jq -r '.services | keys[]')"
onboarding_services="$(printf '%s\n' "$ONBOARDING_JSON" | jq -r '.services | keys[]')"

printf '%s\n' "$default_services" | grep -qx "listener-runtime" \
  && { echo "FAIL: obsolete listener-runtime service still present"; exit 1; }

check_tool_service() {  # service session_name host_port
  svc=$1; session=$2; host_port=$3
  printf '%s\n' "$default_services" | grep -qx "$svc" || { echo "FAIL: missing default service $svc"; exit 1; }
  json="$(printf '%s\n' "$DEFAULT_JSON" | jq -c --arg s "$svc" '.services[$s]')"
  actual_session="$(printf '%s\n' "$json" | jq -r '.environment.SESSION_NAME')"
  [ "$actual_session" = "$session" ] \
    || { echo "FAIL: $svc SESSION_NAME=$actual_session, want $session"; exit 1; }
  published="$(printf '%s\n' "$json" | jq -r '.ports[0].published')"
  target="$(printf '%s\n' "$json" | jq -r '.ports[0].target')"
  [ "$published" = "$host_port" ] \
    || { echo "FAIL: $svc host port=$published, want $host_port"; exit 1; }
  [ "$target" = "8080" ] || { echo "FAIL: $svc container port=$target, want 8080"; exit 1; }
}

check_tool_service listener-tool-a tool-a 8081
check_tool_service listener-tool-b tool-b 8082
check_tool_service listener-tool-c tool-c 8083

printf '%s\n' "$default_services" | grep -qx "listener-tool-d" \
  && { echo "FAIL: listener-tool-d must not run by default (onboarding profile only)"; exit 1; }
printf '%s\n' "$onboarding_services" | grep -qx "listener-tool-d" \
  || { echo "FAIL: listener-tool-d missing under the onboarding profile"; exit 1; }

tool_d_json="$(printf '%s\n' "$ONBOARDING_JSON" | jq -c '.services["listener-tool-d"]')"
tool_d_session="$(printf '%s\n' "$tool_d_json" | jq -r '.environment.SESSION_NAME')"
[ "$tool_d_session" = "tool-d" ] \
  || { echo "FAIL: listener-tool-d SESSION_NAME=$tool_d_session, want tool-d"; exit 1; }
tool_d_published="$(printf '%s\n' "$tool_d_json" | jq -r '.ports[0].published')"
[ "$tool_d_published" = "8084" ] \
  || { echo "FAIL: listener-tool-d host port=$tool_d_published, want 8084"; exit 1; }
tool_d_target="$(printf '%s\n' "$tool_d_json" | jq -r '.ports[0].target')"
[ "$tool_d_target" = "8080" ] \
  || { echo "FAIL: listener-tool-d container port=$tool_d_target, want 8080"; exit 1; }

echo "DEMO CONTRACT PASS"

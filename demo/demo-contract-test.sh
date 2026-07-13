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

# ponytail: the demo scripts above are uncommitted Task-6 artifacts; when absent, SKIP
# this block (not fail) so the committed Task-5 compose gate is standalone-reproducible.
# Task 6 makes this block mandatory when it commits those files.
if [ -e demo/run-demo.sh ]; then
  for script in $scripts; do
    [ -x "$script" ] || { echo "FAIL: missing executable $script"; exit 1; }
    sh -n "$script"
  done
  [ -f demo/demo-lib.sh ] || { echo "FAIL: missing demo/demo-lib.sh"; exit 1; }
  sh -n demo/demo-lib.sh
  [ -s demo/README.md ] || { echo "FAIL: missing demo/README.md"; exit 1; }
  demo/run-demo.sh help >/dev/null
else
  echo "SKIP: demo artifacts not present (Task 6 scope)"
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

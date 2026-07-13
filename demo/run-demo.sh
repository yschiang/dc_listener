#!/bin/sh
# One-person demo entry point: stack lifecycle, aggregate status window, five scenarios, logs, smoke.
set -eu
cd "$(dirname "$0")/.."

usage() {
  cat <<'EOF'
Usage: demo/run-demo.sh COMMAND [ARG]

  up            build/start the three tool processes and wait for their /status
  status        continuously render the aggregate status table (2s poll)
  once          render the aggregate status table once
  1..5          run one scenario (set DEMO_AUTO=1 to run unattended)
  logs [tool]   tail one tool's process logs (tool-a|tool-b|tool-c|tool-d); default: all three
  smoke         run the automated smoke test
  down          stop and remove the demo stack/data
  menu          interactive command menu (default)
  help          show this help

Scenarios drive their own config/docker edits and restore them on exit; keep a second
terminal on `demo/run-demo.sh status` to watch convergence.
EOF
}

wait_runtime() {
  for DEMO_TP in tool-a:8081 tool-b:8082 tool-c:8083; do
    DEMO_T=${DEMO_TP%:*}; DEMO_P=${DEMO_TP#*:}
    DEMO_I=0
    while [ "$DEMO_I" -lt 90 ]; do
      curl -sf "http://localhost:$DEMO_P/status" >/dev/null 2>&1 && break
      DEMO_I=$((DEMO_I + 1))
      sleep 1
    done
    if [ "$DEMO_I" -ge 90 ]; then
      echo "FAIL: $DEMO_T did not expose /status on :$DEMO_P within 90 seconds"
      docker compose logs "listener-$DEMO_T" || true
      return 1
    fi
  done
}

logs_cmd() {  # optional tool arg -> service-specific logs
  case "${1:-}" in
    "") docker compose logs --tail=100 listener-tool-a listener-tool-b listener-tool-c ;;
    tool-a|tool-b|tool-c|tool-d) docker compose logs --tail=100 "listener-$1" ;;
    *) echo "Unknown tool: $1 (use tool-a|tool-b|tool-c|tool-d)"; return 2 ;;
  esac
}

dispatch() {
  case "$1" in
    up)
      command -v docker >/dev/null 2>&1 || { echo "FAIL: docker is required"; return 1; }
      command -v curl >/dev/null 2>&1 || { echo "FAIL: curl is required"; return 1; }
      docker compose up -d --build
      wait_runtime
      demo/watch-status.sh -1
      ;;
    status) demo/watch-status.sh ;;
    once) demo/watch-status.sh -1 ;;
    1) demo/01-change-flow.sh ;;
    2) demo/02-degraded.sh ;;
    3) demo/03-isolation.sh ;;
    4) demo/04-replay.sh ;;
    5) demo/05-onboarding.sh ;;
    logs) logs_cmd "${2:-}" ;;
    smoke) demo/smoke-test.sh ;;
    down) docker compose down -v --remove-orphans ;;
    help|-h|--help) usage ;;
    *) echo "Unknown command: $1"; usage; return 2 ;;
  esac
}

menu() {
  while true; do
    printf '\nCommands: up status once 1 2 3 4 5 logs smoke down help quit\n> '
    read -r DEMO_LINE || return 0
    [ "$DEMO_LINE" = "quit" ] && return 0
    [ -z "$DEMO_LINE" ] && continue
    # shellcheck disable=SC2086
    dispatch $DEMO_LINE || true
  done
}

DEMO_COMMAND=${1:-menu}
if [ "$DEMO_COMMAND" = "menu" ]; then menu; else dispatch "$@"; fi

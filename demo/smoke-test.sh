#!/bin/sh
# 端到端自檢（spec §9.3, Task 5 cutover）：per-tool endpoints on 8081/8082/8083 →
# ACTIVE → each endpoint exposes ONLY its own tool → counts advance independently →
# STANDBY pauses only that tool while the others keep advancing → down.
set -eu
cd "$(dirname "$0")/.."

command -v docker >/dev/null 2>&1 || { echo "FAIL: docker is required"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "FAIL: curl is required"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "FAIL: jq is required"; exit 1; }
[ -f config/sessions.yaml ] || { echo "FAIL: config/sessions.yaml is missing"; exit 1; }

BACKUP=$(mktemp)
cp config/sessions.yaml "$BACKUP"
cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [ -f "$BACKUP" ]; then
    cp "$BACKUP" config/sessions.yaml || status=1
    rm -f "$BACKUP"
  fi
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

tool_port() {  # tool-a -> 8081, tool-b -> 8082, tool-c -> 8083
  case "$1" in
    tool-a) echo 8081 ;;
    tool-b) echo 8082 ;;
    tool-c) echo 8083 ;;
    *) echo "FAIL: unknown tool $1" >&2; exit 1 ;;
  esac
}

status_url() { echo "http://localhost:$(tool_port "$1")/status"; }

state_of() {
  curl -sf "$(status_url "$1")" | jq -r ".sessions[\"$1\"].observedState"
}

admitted_of() {
  curl -sf "$(status_url "$1")" | jq -r ".sessions[\"$1\"].admittedCount"
}

wait_state() {
  tool=$1
  expected=$2
  timeout=$3
  i=0
  while [ "$i" -lt "$timeout" ]; do
    [ "$(state_of "$tool" 2>/dev/null || true)" = "$expected" ] && return 0
    i=$((i + 1))
    sleep 1
  done
  echo "FAIL: timeout waiting $tool -> $expected (now: $(state_of "$tool" 2>/dev/null || echo unavailable))"
  exit 1
}

cat > config/sessions.yaml <<'EOF'
sessions:
  tool-a:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.a.events
      durable: smoke-tool-a
  tool-b:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.b.events
      durable: smoke-tool-b
  tool-c:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.c.events
      durable: smoke-tool-c
EOF

docker compose up -d --build
wait_state tool-a ACTIVE 90
wait_state tool-b ACTIVE 90
wait_state tool-c ACTIVE 90
echo "OK: tool-a/b/c all ACTIVE on their own endpoints (8081/8082/8083)"

for tool in tool-a tool-b tool-c; do
  keys="$(curl -sf "$(status_url "$tool")" | jq -r '.sessions | keys | join(",")')"
  [ "$keys" = "$tool" ] || { echo "FAIL: $(status_url "$tool") exposes [$keys], want only [$tool]"; exit 1; }
done
echo "OK: each endpoint exposes only its own tool"

a1=$(admitted_of tool-a); b1=$(admitted_of tool-b); c1=$(admitted_of tool-c)
sleep 5
a2=$(admitted_of tool-a); b2=$(admitted_of tool-b); c2=$(admitted_of tool-c)
[ "$a2" -gt "$a1" ] || { echo "FAIL: tool-a admittedCount not growing ($a1 -> $a2)"; exit 1; }
[ "$b2" -gt "$b1" ] || { echo "FAIL: tool-b admittedCount not growing ($b1 -> $b2)"; exit 1; }
[ "$c2" -gt "$c1" ] || { echo "FAIL: tool-c admittedCount not growing ($c1 -> $c2)"; exit 1; }
echo "OK: consuming independently (a:$a1->$a2, b:$b1->$b2, c:$c1->$c2)"

# pause only tool-a; tool-b must keep advancing (process isolation, not just config isolation)
cat > config/sessions.yaml <<'EOF'
sessions:
  tool-a:
    desiredState: STANDBY
    configVersion: v1
    config:
      subject: tool.a.events
      durable: smoke-tool-a
  tool-b:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.b.events
      durable: smoke-tool-b
  tool-c:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.c.events
      durable: smoke-tool-c
EOF

wait_state tool-a STANDBY 30
sleep 2
a3=$(admitted_of tool-a); b3=$(admitted_of tool-b)
sleep 4
a4=$(admitted_of tool-a); b4=$(admitted_of tool-b)
[ "$a4" -eq "$a3" ] || { echo "FAIL: tool-a still consuming in STANDBY ($a3 -> $a4)"; exit 1; }
[ "$b4" -gt "$b3" ] || { echo "FAIL: tool-b stopped advancing while tool-a was in STANDBY ($b3 -> $b4)"; exit 1; }
echo "OK: STANDBY stops only tool-a ($a3 = $a4); tool-b keeps advancing ($b3 -> $b4)"

echo "SMOKE TEST PASS"

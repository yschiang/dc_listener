#!/bin/sh
# 端到端自檢（spec §9.3）：up → ACTIVE → 計數增加 → STANDBY → 計數停 → down
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

state_of() {
  curl -sf http://localhost:8080/status | jq -r ".sessions[\"$1\"].observedState"
}

admitted_of() {
  curl -sf http://localhost:8080/status | jq -r ".sessions[\"$1\"].admittedCount"
}

wait_state() {
  session=$1
  expected=$2
  timeout=$3
  i=0
  while [ "$i" -lt "$timeout" ]; do
    [ "$(state_of "$session" 2>/dev/null || true)" = "$expected" ] && return 0
    i=$((i + 1))
    sleep 1
  done
  echo "FAIL: timeout waiting $session -> $expected (now: $(state_of "$session" 2>/dev/null || echo unavailable))"
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
EOF

docker compose up -d --build
wait_state tool-a ACTIVE 90

c1=$(admitted_of tool-a)
sleep 5
c2=$(admitted_of tool-a)
[ "$c2" -gt "$c1" ] || { echo "FAIL: admittedCount not growing ($c1 -> $c2)"; exit 1; }
echo "OK: consuming ($c1 -> $c2)"

cat > config/sessions.yaml <<'EOF'
sessions:
  tool-a:
    desiredState: STANDBY
    configVersion: v1
    config:
      subject: tool.a.events
      durable: smoke-tool-a
EOF

wait_state tool-a STANDBY 30
sleep 2
c3=$(admitted_of tool-a)
sleep 4
c4=$(admitted_of tool-a)
[ "$c4" -eq "$c3" ] || { echo "FAIL: still consuming in STANDBY ($c3 -> $c4)"; exit 1; }
echo "OK: STANDBY stops consumption ($c3 = $c4)"

echo "SMOKE TEST PASS"

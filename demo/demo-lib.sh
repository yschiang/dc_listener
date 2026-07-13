# Shared POSIX-shell helpers for the interactive demo scripts.
#
# Single-session runtime (ADR-0001): each tool is its own process/container on its own
# host status port, and each /status endpoint reports ONLY its own selected tool.
# Endpoint map: tool-a->8081, tool-b->8082, tool-c->8083, tool-d->8084 (onboarding profile).
#
# The scenario scripts drive their own config/docker mutations so a run is deterministic and
# repeatable. Pauses are observation checkpoints; set DEMO_AUTO=1 to run unattended.

demo_port_of() {
  case "$1" in
    tool-a) echo 8081 ;;
    tool-b) echo 8082 ;;
    tool-c) echo 8083 ;;
    tool-d) echo 8084 ;;
    *) echo "FAIL: unknown tool $1" >&2; return 1 ;;
  esac
}

demo_url_of() { echo "http://localhost:$(demo_port_of "$1")/status"; }

demo_require_runtime() {
  command -v curl >/dev/null 2>&1 || { echo "FAIL: curl is required"; exit 1; }
  command -v jq >/dev/null 2>&1 || { echo "FAIL: jq is required"; exit 1; }
  command -v docker >/dev/null 2>&1 || { echo "FAIL: docker is required"; exit 1; }
  for DEMO_T in tool-a tool-b tool-c; do
    curl -sf "$(demo_url_of "$DEMO_T")" >/dev/null 2>&1 \
      || { echo "FAIL: $DEMO_T unreachable at $(demo_url_of "$DEMO_T"); run: demo/run-demo.sh up"; exit 1; }
  done
}

demo_pause() {
  if [ -n "${DEMO_AUTO:-}" ]; then
    printf '\n>>> %s\n(DEMO_AUTO: continuing)\n' "$1"
    return 0
  fi
  printf '\n>>> %s\n(press Enter to continue) ' "$1"
  read -r DEMO_REPLY || true
}

# --- per-tool /status field readers (each tool has its own endpoint) ---

demo_field_of() {  # tool jq-path
  curl -sf "$(demo_url_of "$1")" | jq -r ".sessions[\"$1\"].$2 // \"ABSENT\""
}

demo_state_of()    { demo_field_of "$1" observedState; }
demo_pending_of()  { curl -sf "$(demo_url_of "$1")" | jq -r ".sessions[\"$1\"].pendingCount // -1"; }
demo_admitted_of() { curl -sf "$(demo_url_of "$1")" | jq -r ".sessions[\"$1\"].admittedCount // -1"; }

# --- container / durable identity (proves process isolation and in-place durable updates) ---

demo_cid() { docker compose ps -q "$1" 2>/dev/null; }

demo_started() {
  DEMO_ID=$(demo_cid "$1")
  [ -n "$DEMO_ID" ] && docker inspect -f '{{.State.StartedAt}}' "$DEMO_ID" 2>/dev/null
}

demo_nats() {
  docker compose exec -T "${DEMO_NATS_BOX:-upstream-publisher}" \
    nats -s "${DEMO_NATS_URL:-nats://upstream-nats:4222}" "$@"
}

demo_consumer_info() {  # durable -> consumer info JSON
  demo_nats consumer info "${DEMO_STREAM:-TOOL_EVENTS}" "$1" --json 2>/dev/null
}

demo_publish() {  # subject count
  DEMO_I=0
  while [ "$DEMO_I" -lt "$2" ]; do
    DEMO_I=$((DEMO_I + 1))
    demo_nats pub "$1" "demo-$DEMO_I" >/dev/null 2>&1 || true
  done
}

# --- deterministic config writer: canonical a/b/c baseline with a parameterized tool-a ---
# demo_write_config [a_subject] [a_version] [a_desired] [with-tool-d]

demo_write_config() {
  DEMO_A_SUB=${1:-tool.a.events}
  DEMO_A_VER=${2:-v1}
  DEMO_A_DES=${3:-RUNNING}
  cat > config/sessions.yaml <<EOF
sessions:
  tool-a:
    desiredState: ${DEMO_A_DES}
    configVersion: ${DEMO_A_VER}
    config:
      subject: ${DEMO_A_SUB}
      durable: listener-tool-a
  tool-b:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.b.events
      durable: listener-tool-b
      retry:
        interval: 5s
        maxAttempts: 10
  tool-c:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.c.events
      durable: listener-tool-c
EOF
  if [ "${4:-}" = "with-tool-d" ]; then
    cat >> config/sessions.yaml <<EOF
  tool-d:
    desiredState: RUNNING
    configVersion: v1
    config:
      subject: tool.d.events
      durable: listener-tool-d
EOF
  fi
}

# --- polling waiters (all route through the per-tool endpoint) ---

demo_log_state() {  # tool -> append one "config -> observed state" line to demo.log
  DEMO_JSON=$(curl -sf "$(demo_url_of "$1")" 2>/dev/null) || return 0
  printf '%s' "$DEMO_JSON" | jq -r --arg t "$1" '
    .sessions[$t] as $s
    | "\(now | gmtime | strftime("%Y-%m-%dT%H:%M:%SZ")) tool=\($t) desired=\($s.desiredState) configVersion=\($s.appliedConfigVersion) observed=\($s.observedState) reason=\(if ($s.reason // "") == "" then "-" else $s.reason end)"
  ' >> "${DEMO_LOG:-demo/demo.log}" 2>/dev/null || true
}

demo_wait_state() {  # tool expected timeout
  DEMO_I=0
  while [ "$DEMO_I" -lt "$3" ]; do
    if [ "$(demo_state_of "$1" 2>/dev/null || true)" = "$2" ]; then
      demo_log_state "$1"
      return 0
    fi
    DEMO_I=$((DEMO_I + 1))
    sleep 1
  done
  echo "FAIL: timeout waiting $1 -> $2 (now: $(demo_state_of "$1" 2>/dev/null || echo unavailable))"
  exit 1
}

demo_wait_pending_zero() {  # tool timeout
  DEMO_I=0
  while [ "$DEMO_I" -lt "$2" ]; do
    [ "$(demo_pending_of "$1" 2>/dev/null || echo -1)" = "0" ] && return 0
    DEMO_I=$((DEMO_I + 1))
    sleep 1
  done
  echo "FAIL: timeout waiting $1 pendingCount -> 0"
  exit 1
}

demo_wait_pending_at_least() {  # tool minimum timeout
  DEMO_I=0
  while [ "$DEMO_I" -lt "$3" ]; do
    DEMO_PENDING=$(demo_pending_of "$1" 2>/dev/null || echo -1)
    [ "$DEMO_PENDING" -ge "$2" ] 2>/dev/null && return 0
    DEMO_I=$((DEMO_I + 1))
    sleep 1
  done
  echo "FAIL: timeout waiting $1 pendingCount >= $2"
  exit 1
}

demo_wait_admitted_at_least() {  # tool minimum timeout
  DEMO_I=0
  while [ "$DEMO_I" -lt "$3" ]; do
    DEMO_ADMITTED=$(demo_admitted_of "$1" 2>/dev/null || echo -1)
    [ "$DEMO_ADMITTED" -ge "$2" ] 2>/dev/null && return 0
    DEMO_I=$((DEMO_I + 1))
    sleep 1
  done
  echo "FAIL: timeout waiting $1 admittedCount >= $2 (now: $(demo_admitted_of "$1" 2>/dev/null || echo unavailable))"
  exit 1
}

demo_wait_config() {  # tool subject version timeout
  DEMO_I=0
  while [ "$DEMO_I" -lt "$4" ]; do
    DEMO_MATCH=$(curl -sf "$(demo_url_of "$1")" \
      | jq -r --arg session "$1" --arg subject "$2" --arg version "$3" '
          .sessions[$session] as $s
          | ($s.subject == $subject
             and $s.declaredConfigVersion == $version
             and $s.appliedConfigVersion == $version)') || DEMO_MATCH=false
    [ "$DEMO_MATCH" = "true" ] && return 0
    DEMO_I=$((DEMO_I + 1))
    sleep 1
  done
  echo "FAIL: timeout waiting $1 config -> $2 $3/$3"
  exit 1
}

demo_wait_reason() {  # tool reason timeout
  DEMO_I=0
  while [ "$DEMO_I" -lt "$3" ]; do
    DEMO_ACTUAL=$(curl -sf "$(demo_url_of "$1")" \
      | jq -r --arg session "$1" '.sessions[$session].reason // ""') || DEMO_ACTUAL=unavailable
    [ "$DEMO_ACTUAL" = "$2" ] && return 0
    DEMO_I=$((DEMO_I + 1))
    sleep 1
  done
  echo "FAIL: timeout waiting $1 reason -> $2 (now: $DEMO_ACTUAL)"
  exit 1
}

demo_show_status() {
  demo/watch-status.sh -1 || true
}

#!/bin/sh
# Real-NATS durable ownership acceptance (ADR-0001, Task 2).
#
# Proves, against a live JetStream server, that the runtime:
#   1. selects and OWNS one durable consumer (captures name, creation timestamp, cursor/pending);
#   2. applies a subject/filter change by updating that SAME durable IN PLACE (no delete+recreate:
#      creation timestamp and acked cursor are preserved);
#   3. rejects a durable mutation fail-closed (FAILED/INVALID_SPEC) and creates NO second consumer;
#   4. never deletes a consumer when a declaration disappears.
#
# STATUS_URL / RUNTIME_SERVICE are overridable so the same evidence runs before and after the
# Task 5 Compose cutover (single listener-runtime:8080 -> now listener-tool-a:8081 by default).
#
# Assumes the stack is already up:  docker compose up -d --build
# On exit it restores the original config and waits for ACTIVE again. It NEVER deletes a consumer.
set -eu
cd "$(dirname "$0")/.."

STATUS_URL="${STATUS_URL:-http://localhost:8081}"
RUNTIME_SERVICE="${RUNTIME_SERVICE:-listener-tool-a}"
TOOL="${TOOL:-tool-a}"
DURABLE="${DURABLE:-listener-tool-a}"
STREAM="${STREAM:-TOOL_EVENTS}"
NATS_BOX="${NATS_BOX:-upstream-publisher}"
NATS_URL_INTERNAL="${NATS_URL_INTERNAL:-nats://upstream-nats:4222}"

command -v docker >/dev/null 2>&1 || { echo "FAIL: docker is required"; exit 1; }
command -v curl   >/dev/null 2>&1 || { echo "FAIL: curl is required"; exit 1; }
command -v jq     >/dev/null 2>&1 || { echo "FAIL: jq is required"; exit 1; }
[ -f config/sessions.yaml ] || { echo "FAIL: config/sessions.yaml is missing"; exit 1; }

# --- source-level safety gate: no consumer-delete API/path in production source (ADR-0001) ---
if grep -rn "deleteConsumer" runtime/src/main >/dev/null 2>&1; then
  echo "FAIL: runtime/src/main still references deleteConsumer"; exit 1
fi
echo "OK: runtime/src/main contains no deleteConsumer / consumer-delete call"

nats_cli()  { docker compose exec -T "$NATS_BOX" nats -s "$NATS_URL_INTERNAL" "$@"; }
info_json() { nats_cli consumer info "$STREAM" "$1" --json 2>/dev/null; }
# durable names as one-per-line (tolerate an array of strings OR of objects across nats CLI versions)
consumer_names() {
  nats_cli consumer ls "$STREAM" --json 2>/dev/null \
    | jq -r '.[] | if type == "object" then .name else . end' 2>/dev/null
}
# membership over the durable list (an empty `consumer info` + `jq -e` gives a false positive)
consumer_exists() { consumer_names | grep -qxF "$1"; }
consumer_count()  { nats_cli consumer ls "$STREAM" --json 2>/dev/null | jq 'length' 2>/dev/null || printf '%s\n' -1; }

field_of() { curl -sf "$STATUS_URL/status" | jq -r ".sessions[\"$TOOL\"].$1"; }
state_of()   { field_of observedState; }
applied_of() { field_of appliedConfigVersion; }
reason_of()  { field_of reason; }

wait_status() {  # key expected timeout
  i=0
  while [ "$i" -lt "$3" ]; do
    [ "$(field_of "$1" 2>/dev/null || true)" = "$2" ] && return 0
    i=$((i + 1)); sleep 1
  done
  echo "FAIL: timeout waiting $TOOL .$1 -> $2 (now: $(field_of "$1" 2>/dev/null || echo unavailable))"
  echo "--- recent runtime logs ($RUNTIME_SERVICE) ---"
  docker compose logs --tail 30 "$RUNTIME_SERVICE" 2>/dev/null || true
  return 1
}

# controlled, deterministic single-tool config for the duration of the test
write_config() {  # subject durable version
  cat > config/sessions.yaml <<EOF
sessions:
  ${TOOL}:
    desiredState: RUNNING
    configVersion: $3
    config:
      subject: $1
      durable: $2
EOF
}

BACKUP="$(mktemp)"
cp config/sessions.yaml "$BACKUP"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [ -f "$BACKUP" ]; then
    cp "$BACKUP" config/sessions.yaml || status=1   # NEVER delete a consumer on the way out
    rm -f "$BACKUP"
  fi
  echo "--- cleanup: restored original config, waiting for ACTIVE ---"
  # a slow restore must not mask a real result (PASS=0, BLOCKED=2, signals) — only downgrade a clean exit
  if ! wait_status observedState ACTIVE 40 >/dev/null 2>&1; then
    [ "$status" -eq 0 ] && status=1
    echo "WARN: $TOOL did not return to ACTIVE within 40s after config restore"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "=== 0. baseline: durable $DURABLE ACTIVE on subject tool.a.events ==="
write_config "tool.a.events" "$DURABLE" "v1"
wait_status observedState ACTIVE 90
wait_status appliedConfigVersion v1 30
# let the consumer make progress so the cursor is non-zero
sleep 6

BASE_INFO="$(info_json "$DURABLE")"
[ -n "$BASE_INFO" ] || { echo "FAIL: cannot read consumer info for $DURABLE"; exit 1; }
BASE_NAME="$(echo "$BASE_INFO"    | jq -r '.name')"
BASE_CREATED="$(echo "$BASE_INFO" | jq -r '.created')"
BASE_CURSOR="$(echo "$BASE_INFO"  | jq -r '.ack_floor.stream_seq')"
BASE_PENDING="$(echo "$BASE_INFO" | jq -r '.num_pending')"
BASE_COUNT="$(consumer_count)"
echo "consumer name=$BASE_NAME created=$BASE_CREATED cursor(ack_floor.stream_seq)=$BASE_CURSOR pending=$BASE_PENDING"
echo "stream consumer count=$BASE_COUNT"
[ "$BASE_NAME" = "$DURABLE" ] || { echo "FAIL: selected consumer name mismatch"; exit 1; }
[ "$BASE_CURSOR" -ge 1 ] 2>/dev/null || { echo "FAIL: cursor did not advance from 0 (no consumption?)"; exit 1; }

echo "=== 1. subject/filter update -> SAME durable updated IN PLACE (no delete+recreate) ==="
write_config "tool.a.events.v2" "$DURABLE" "v2"
# if the pinned NATS cannot update the filter in place, the runtime cannot reach ACTIVE/v2:
# it stays DEGRADED and (bounded) FAILED/RETRY_EXHAUSTED. That is the BLOCKED signal.
if ! wait_status appliedConfigVersion v2 60; then
  echo "BLOCKED: this NATS version could not update filterSubject in place."
  echo "         Do NOT reintroduce delete+recreate; revise the design per the task brief."
  exit 2
fi
wait_status observedState ACTIVE 30

UPD_INFO="$(info_json "$DURABLE")"
UPD_NAME="$(echo "$UPD_INFO"    | jq -r '.name')"
UPD_CREATED="$(echo "$UPD_INFO" | jq -r '.created')"
UPD_CURSOR="$(echo "$UPD_INFO"  | jq -r '.ack_floor.stream_seq')"
UPD_FILTER="$(echo "$UPD_INFO"  | jq -r '.config.filter_subject')"
echo "after update: name=$UPD_NAME created=$UPD_CREATED cursor=$UPD_CURSOR filter=$UPD_FILTER"
[ "$UPD_NAME" = "$DURABLE" ]        || { echo "FAIL: durable name changed"; exit 1; }
[ "$UPD_CREATED" = "$BASE_CREATED" ] || { echo "FAIL: creation timestamp changed -> consumer was recreated"; exit 1; }
[ "$UPD_FILTER" = "tool.a.events.v2" ] || { echo "FAIL: filter_subject was not updated in place ($UPD_FILTER)"; exit 1; }
[ "$UPD_CURSOR" -ge "$BASE_CURSOR" ] 2>/dev/null || { echo "FAIL: cursor reset ($BASE_CURSOR -> $UPD_CURSOR)"; exit 1; }
[ "$(consumer_count)" = "$BASE_COUNT" ] || { echo "FAIL: consumer count changed on filter update"; exit 1; }
echo "OK: durable updated in place; name+creation preserved, cursor not reset ($BASE_CURSOR -> $UPD_CURSOR)"

echo "=== 2. durable mutation -> FAILED/INVALID_SPEC, NO second consumer ==="
MUTATED="${DURABLE}-v2"
write_config "tool.a.events.v2" "$MUTATED" "v3"
wait_status observedState FAILED 40
R="$(reason_of)"
echo "tool status: observedState=FAILED reason=$R"
case "$R" in
  INVALID_SPEC*) : ;;
  *) echo "FAIL: expected reason to start with INVALID_SPEC, got: $R"; exit 1 ;;
esac
if consumer_exists "$MUTATED"; then
  echo "FAIL: a second consumer '$MUTATED' was created by a durable mutation"; exit 1
fi
consumer_exists "$DURABLE" || { echo "FAIL: original durable '$DURABLE' disappeared"; exit 1; }
[ "$(consumer_count)" = "$BASE_COUNT" ] || { echo "FAIL: stream consumer count changed after mutation attempt"; exit 1; }
echo "OK: durable mutation rejected; '$MUTATED' absent; '$DURABLE' intact; count still $BASE_COUNT"

echo "=== 3. declaration disappearance retains the durable (no deletion) ==="
write_config "tool.a.events.v2" "$DURABLE" "v4"   # restore latched durable so tool-a recovers first
wait_status observedState ACTIVE 40
printf 'sessions: {}\n' > config/sessions.yaml     # every declaration gone
wait_status observedState FAILED 40
consumer_exists "$DURABLE" || { echo "FAIL: durable '$DURABLE' was deleted on declaration disappearance"; exit 1; }
echo "OK: '$DURABLE' still present after its declaration disappeared (fail closed, not offboarded)"

echo "CONSUMER SAFETY TEST PASS"

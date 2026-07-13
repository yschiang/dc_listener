#!/bin/sh
# Scenario 2: NATS outage -> every tool observed DEGRADED -> all recover to ACTIVE.
# One combined poll loop remembers the DEGRADED observation per tool so a short transient
# cannot be missed by sequential waits. A trap ALWAYS restarts NATS.
set -eu
cd "$(dirname "$0")/.."
. demo/demo-lib.sh
demo_require_runtime

DEMO_NATS_STOPPED=0
restore_nats() {
  DEMO_STATUS=$?
  trap - EXIT INT TERM
  [ "$DEMO_NATS_STOPPED" = "1" ] && docker compose start upstream-nats >/dev/null 2>&1 || true
  exit "$DEMO_STATUS"
}
trap restore_nats EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# containers must survive the dependency outage (state machine handles it, no process restart).
A_CID=$(demo_cid listener-tool-a); B_CID=$(demo_cid listener-tool-b); C_CID=$(demo_cid listener-tool-c)

demo_pause "Stop upstream-nats briefly; watch all three tools go DEGRADED, then auto-recover."
docker compose stop upstream-nats
DEMO_NATS_STOPPED=1

# combined poller: one pass over ALL endpoints per second, latching each DEGRADED sighting.
SEEN_A=0; SEEN_B=0; SEEN_C=0; I=0
while [ "$I" -lt 30 ]; do
  case "$(demo_state_of tool-a 2>/dev/null || true)" in DEGRADED) SEEN_A=1 ;; esac
  case "$(demo_state_of tool-b 2>/dev/null || true)" in DEGRADED) SEEN_B=1 ;; esac
  case "$(demo_state_of tool-c 2>/dev/null || true)" in DEGRADED) SEEN_C=1 ;; esac
  [ "$SEEN_A" = 1 ] && [ "$SEEN_B" = 1 ] && [ "$SEEN_C" = 1 ] && break
  I=$((I + 1)); sleep 1
done
demo_show_status
[ "$SEEN_A" = 1 ] || { echo "FAIL: tool-a never observed DEGRADED"; exit 1; }
[ "$SEEN_B" = 1 ] || { echo "FAIL: tool-b never observed DEGRADED"; exit 1; }
[ "$SEEN_C" = 1 ] || { echo "FAIL: tool-c never observed DEGRADED"; exit 1; }
echo "OK: all three tools observed DEGRADED during the outage"

# recover well before tool-b's ~50s retry exhaustion (5s x 10 attempts).
echo "Restoring upstream-nats (outage kept short, under tool-b's retry-exhaustion window)..."
docker compose start upstream-nats
DEMO_NATS_STOPPED=0
demo_wait_state tool-a ACTIVE 90
demo_wait_state tool-b ACTIVE 90
demo_wait_state tool-c ACTIVE 90

[ "$A_CID" = "$(demo_cid listener-tool-a)" ] || { echo "FAIL: tool-a container restarted"; exit 1; }
[ "$B_CID" = "$(demo_cid listener-tool-b)" ] || { echo "FAIL: tool-b container restarted"; exit 1; }
[ "$C_CID" = "$(demo_cid listener-tool-c)" ] || { echo "FAIL: tool-c container restarted"; exit 1; }
demo_show_status

echo "Scenario 2 done: dependency loss was handled as state, every container survived and recovered."

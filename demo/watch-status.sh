#!/bin/sh
# Aggregate /status across the per-tool runtime processes into one human table.
# Each process exposes ONLY its own tool, so we poll every endpoint and merge the rows.
#
#   watch-status.sh          poll every 2s (demo default)
#   watch-status.sh -1       single snapshot
#   watch-status.sh -i 30    operator interval (e.g. 30s) instead of the 2s default
set -u

INTERVAL="${WATCH_INTERVAL:-2}"
ONCE=0
# core tools are always shown (unreachable is flagged); tool-d only appears under onboarding.
CORE_TOOLS="tool-a:8081 tool-b:8082 tool-c:8083"
OPT_TOOLS="tool-d:8084"

while [ $# -gt 0 ]; do
  case "$1" in
    -1) ONCE=1 ;;
    -i) shift; INTERVAL="${1:?-i needs SECONDS}" ;;
    *) echo "usage: watch-status.sh [-1] [-i SECONDS]" >&2; exit 2 ;;
  esac
  shift
done

row_of() {  # tool port -> one @tsv row, or nonzero if unreachable / not present
  DEMO_JSON=$(curl -sf "http://localhost:$2/status" 2>/dev/null) || return 1
  printf '%s' "$DEMO_JSON" | jq -e -r --arg t "$1" '
    (.cell.specError // "") as $specError
    | .sessions[$t] as $v
    | if $v == null then error("absent") else
      [ $t, ($v.subject // "-"),
        (if $v.conditions.configurationReady then "ok" else "x" end),
        "\($v.declaredConfigVersion // "-")/\($v.appliedConfigVersion // "-")",
        ($v.desiredState // "-"), $v.observedState,
        (if $v.conditions.connectionReady then "ok" else "x" end),
        (if $v.conditions.admissionAllowed then "ok" else "x" end),
        ($v.admittedCount | tostring), ($v.pendingCount | tostring),
        ($v.retryAttempt | tostring),
        (if $specError != "" then $specError
         elif ($v.reason // "") != "" then $v.reason
         else "-" end)
      ] | @tsv end' 2>/dev/null
}

render() {
  {
    printf 'NAME\tNATS_SUBJECT\tCONFIG\tVER(d/a)\tDESIRED\tOBSERVED\tCONN\tADMIT\tMSG_CNT\tPENDING\tRETRY\tERROR_REASON\n'
    for DEMO_TP in $CORE_TOOLS; do
      DEMO_T=${DEMO_TP%:*}; DEMO_P=${DEMO_TP#*:}
      row_of "$DEMO_T" "$DEMO_P" || printf '%s\t(unreachable :%s)\n' "$DEMO_T" "$DEMO_P"
    done
    for DEMO_TP in $OPT_TOOLS; do
      DEMO_T=${DEMO_TP%:*}; DEMO_P=${DEMO_TP#*:}
      row_of "$DEMO_T" "$DEMO_P" || true   # tool-d is silent unless the onboarding workload is up
    done
  } | if command -v column >/dev/null 2>&1; then column -t -s "$(printf '\t')"; else cat; fi
}

if [ "$ONCE" = "1" ]; then
  render
  exit 0
fi

while true; do
  [ -t 1 ] && clear
  date "+%H:%M:%S  (poll ${INTERVAL}s; endpoints 8081/8082/8083)"
  render
  sleep "$INTERVAL"
done

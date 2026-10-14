#!/usr/bin/env bash
# End-to-end tests against the live docker compose stack. One entry point for
# every Go E2E scenario (the tests themselves live in e2e/).
#
#   scripts/e2e.sh              # everything (both phases)
#   scripts/e2e.sh api          # phase 1 only: lifecycle + API error contract
#   scripts/e2e.sh broker       # phase 2 only: broker scenarios, matrices, concurrency
#   scripts/e2e.sh OutOfOrder   # any test-name pattern; runs in whichever phase it needs
#
#   KEEP_FAST_WORKER=1 scripts/e2e.sh   # leave the fast test worker running afterwards
#
# Phase 1 (worker RUNNING): the real worker drives tenants through their lifecycle.
#   TestLifecycle_*            create → active → update → delete, end to end
#   TestAPIErrors_*            400/404/405/409/422 codes and field details
#
# Phase 2 (worker STOPPED): the tests play the worker by publishing straight to
# RabbitMQ, so every outcome is exact.
#   TestMessaging_OutOfOrder              done, then in_progress → task stays done
#   TestMessaging_DuplicateIsIdempotent   same update_id ×3 → applied once
#   TestMessaging_PoisonMessagesAreDeadLettered  garbage / bad status / unknown task → DLQ
#   TestMessaging_FailedOutcome           failed → tenant failed
#   TestMessaging_ManyTenantsInParallel   20 tenants, mixed outcomes, all settle
#   TestMatrix_TaskUpdates                every task status × every update (12 cases)
#   TestMatrix_TenantOperations           every tenant status × PATCH/DELETE (12 cases)
#   TestConcurrency_*                     version conflicts, racing writers
#
# Watch the consumer's decisions in another terminal:
#   docker compose logs -f control-plane | grep "task update consumer"
#
# Needs: Docker with Compose v2, Go (same version as go.mod), curl.
# Tests create uniquely-named tenants and never wipe data, so it's safe to run
# repeatedly against your dev stack. The worker is always restored on exit.
set -euo pipefail
cd "$(dirname "$0")/.."

API="${E2E_API_URL:-http://localhost:8080}"
GO_TEST=(go test -tags e2e -count=1 -v ./e2e/)

PHASE1='Lifecycle|APIErrors'
PHASE2='Matrix|Messaging|Concurrency'
usage() { sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
[[ $# -le 1 ]] || usage
case "${1:-all}" in
  -h|--help) usage ;;
  -*) echo "unknown option: $1 (the script already runs go test -v)" >&2; usage ;;
  all)    RUN1=$PHASE1 RUN2=$PHASE2 ;;
  api)    RUN1=$PHASE1 RUN2=''      ;;
  broker) RUN1=''      RUN2=$PHASE2 ;;
  *)      RUN1=$1      RUN2=$1      ;;  # tests skip themselves in the wrong phase
esac

# run_go <pattern>: go test, but fail if the pattern matched no tests.
run_go() {
  local out; out=$(mktemp)
  "${GO_TEST[@]}" -run "$1" | tee "$out"
  local rc=${PIPESTATUS[0]}
  if grep -q 'no tests to run' "$out"; then
    rm -f "$out"; echo "no tests match '$1'" >&2; exit 1
  fi
  rm -f "$out"; return "$rc"
}

log() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }

restore_worker() {
  if [[ -z "${KEEP_FAST_WORKER:-}" ]]; then
    log "Restoring the worker with default settings"
    docker compose up -d worker >/dev/null
  else
    docker compose start worker >/dev/null
  fi
}
trap restore_worker EXIT

log "Booting the stack with a fast worker (50-150 ms per task)"
WORKER_FAIL_RATE=0 WORKER_MIN_DELAY_MS=50 WORKER_MAX_DELAY_MS=150 \
  docker compose up -d --build postgres rabbitmq control-plane worker

log "Waiting for the API to be ready"
for i in $(seq 1 90); do
  if curl -fsS "$API/readyz" >/dev/null 2>&1; then echo "ready"; break; fi
  [[ $i == 90 ]] && { echo "API not ready after 90s"; docker compose logs --tail=50 control-plane; exit 1; }
  sleep 1
done
sleep 2 # let the worker attach to the tasks queue

if [[ -n $RUN1 ]]; then
  log "Phase 1: worker RUNNING ($RUN1)"
  run_go "$RUN1"
fi

if [[ -n $RUN2 ]]; then
  log "Stopping the worker; the tests now play the worker"
  docker compose stop worker >/dev/null
  sleep 1
  log "Phase 2: worker STOPPED ($RUN2)"
  run_go "$RUN2"
fi

log "All selected E2E tests passed"
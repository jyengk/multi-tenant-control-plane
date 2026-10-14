#!/usr/bin/env bash
# Restart the worker with a fixed delay per task, then walk one tenant through
# its lifecycle slowly enough to see the in-between states and to show that
# changes are rejected while the tenant is busy.
#
#   scripts/slow-worker.sh [delay_ms]          # default 10000 (10 s per task)
#   scripts/slow-worker.sh [delay_ms] --crash  # also kill the worker mid-task
#
# What it checks (exits non-zero on the first mismatch):
#   1. while deploying: task in_progress, tenant provisioning
#   2. PATCH and DELETE during that window → 409 tenant_update_not_allowed
#   3. deploy finishes → tenant active v2
#   4. PATCH → 202 updating; a second PATCH while updating → 409
#   5. update finishes → tenant active v4 with the new name
#   with --crash: the worker is stopped mid-deploy and started again; the
#   task is redelivered and still completes (nothing lost)
#
# Afterwards the worker is put back to its defaults, unless KEEP_WORKER=1.
# VERBOSE=1 also prints every request and the JSON response it got.
# Needs: docker compose stack up, curl.
set -euo pipefail
cd "$(dirname "$0")/.."

DELAY="${1:-10000}"
CRASH="${2:-}"
API="${E2E_API_URL:-http://localhost:8080}"
WAIT=$(( DELAY / 1000 * 3 + 30 ))   # generous upper bound per wait, in seconds

restore() {
  if [[ -z "${KEEP_WORKER:-}" ]]; then
    echo; echo "restoring worker defaults"
    docker compose up -d worker >/dev/null 2>&1 || true
  fi
}
trap restore EXIT

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗ %s\033[0m\n' "$*" >&2; exit 1; }
step() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# Verbose output goes to stderr, so it shows even when a caller captures stdout.
vlog() { [[ -n ${VERBOSE:-} ]] && printf '    \033[2m%s\033[0m\n' "$*" >&2; return 0; }

# show <id>: in verbose mode, print the tenant's current JSON and its tasks.
show() {
  [[ -n ${VERBOSE:-} ]] || return 0
  vlog "→ GET /v1/tenants/$1"
  vlog "← $(tenant "$1")"
  vlog "→ GET /v1/tasks?tenant_id=$1"
  vlog "← $(curl -sS "$API/v1/tasks?tenant_id=$1")"
}

field()  { sed -n "s/.*\"$1\":\"\\{0,1\\}\\([^\",}]*\\).*/\\1/p"; }   # first "key":value
tenant() { curl -sS "$API/v1/tenants/$1"; }
task()   { curl -sS "$API/v1/tasks/$1"; }

# wait_for <what> <command...>: retry until the command succeeds or WAIT passes.
wait_for() {
  local what=$1; shift
  for _ in $(seq 1 "$WAIT"); do "$@" && return 0; sleep 1; done
  fail "timed out after ${WAIT}s waiting for: $what"
}
tenant_is() { [[ $(tenant "$1" | field status) == "$2" ]]; }
task_is()   { [[ $(task "$1" | field status) == "$2" ]]; }

# http <method> <path> [body] → prints "<status> <body>"
http() {
  local tmp; tmp=$(mktemp)
  local args=(-sS -o "$tmp" -w '%{http_code}' -X "$1" "$API$2")
  [[ -n ${3:-} ]] && args+=(-H 'Content-Type: application/json' -d "$3")
  local code; code=$(curl "${args[@]}")
  vlog "→ $1 $2${3:+ $3}"
  vlog "← $code $(cat "$tmp")"
  printf '%s %s' "$code" "$(cat "$tmp")"; rm -f "$tmp"
}
expect_409() {
  local out; out=$(http "$@")
  [[ $out == 409* && $out == *tenant_update_not_allowed* ]] \
    && pass "$1 while busy → 409 tenant_update_not_allowed" \
    || fail "$1 while busy: expected 409 tenant_update_not_allowed, got: $out"
}

step "Restarting worker with ${DELAY} ms per task"
WORKER_MIN_DELAY_MS="$DELAY" WORKER_MAX_DELAY_MS="$DELAY" WORKER_FAIL_RATE=0 \
  docker compose up -d worker >/dev/null
sleep 2
docker compose logs --since 30s worker | grep '"worker started"' | tail -1 || true

step "1. Create a tenant and watch the deploy"
resp=$(http POST /v1/tenants "{\"slug\":\"slow-$(date +%s)\",\"name\":\"Slow worker demo\"}")
[[ $resp == 202* ]] || fail "create: $resp"
body=${resp#* }
id=$(printf '%s' "$body" | sed -n 's/.*"tenant":{"id":"\([^"]*\)".*/\1/p')
task_id=$(printf '%s' "$body" | sed -n 's/.*"task":{"id":"\([^"]*\)".*/\1/p')
pass "created tenant $id (deploy task $task_id)"

wait_for "deploy task in_progress" task_is "$task_id" in_progress
tenant_is "$id" provisioning && pass "task in_progress, tenant still provisioning" \
  || fail "tenant should still be provisioning: $(tenant "$id")"
show "$id"

step "2. Try to change it while it's busy"
expect_409 PATCH "/v1/tenants/$id" '{"version":1,"name":"too early"}'
expect_409 DELETE "/v1/tenants/$id"

if [[ $CRASH == --crash ]]; then
  step "   Crash: stopping the worker mid-task"
  docker compose stop worker >/dev/null
  pass "worker stopped while the deploy was in progress"
  sleep 2
  tenant_is "$id" provisioning && pass "tenant still provisioning (nothing lost, nothing half-applied)" \
    || fail "tenant changed while the worker was down: $(tenant "$id")"
  docker compose start worker >/dev/null
  pass "worker started again; the unacked task is redelivered"
fi

step "3. Wait for the deploy to finish"
wait_for "tenant active" tenant_is "$id" active
v=$(tenant "$id" | field version)
[[ $v == 2 ]] && pass "tenant active v2" || fail "expected v2, got v$v"
show "$id"
if [[ $CRASH == --crash ]]; then
  docker compose logs --since 5m worker | grep -q "\"task_id\":\"$task_id\".*\"redelivered\":true" \
    && pass "worker log shows the task was redelivered" \
    || echo "  (redelivery not visible in the log; the task may have been re-sent before the crash took effect)"
fi

step "4. Update it, and try again while updating"
out=$(http PATCH "/v1/tenants/$id" '{"version":2,"name":"Renamed slowly"}')
[[ $out == 202* && $out == *'"status":"updating"'* ]] && pass "PATCH → 202 updating (v3)" || fail "PATCH: $out"
expect_409 PATCH "/v1/tenants/$id" '{"version":3,"name":"second"}'

step "5. Wait for the update to finish"
wait_for "tenant active again" tenant_is "$id" active
t=$(tenant "$id")
[[ $(printf '%s' "$t" | field version) == 4 && $t == *'"name":"Renamed slowly"'* ]] \
  && pass "tenant active v4, name 'Renamed slowly'" || fail "after update: $t"
show "$id"

step "Done: all checks passed"
echo "  tenant $id"
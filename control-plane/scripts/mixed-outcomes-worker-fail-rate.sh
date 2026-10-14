#!/usr/bin/env bash
# Restart the worker with a given fail rate, create N tenants, wait for the
# worker to finish them all, and print how they ended up.
#
#   scripts/mixed-outcomes-worker-fail-rate.sh [fail_rate] [count]
#
#   scripts/mixed-outcomes-worker-fail-rate.sh            # fail rate 0.3, 10 tenants
#   scripts/mixed-outcomes-worker-fail-rate.sh 1 5        # every task fails
#   scripts/mixed-outcomes-worker-fail-rate.sh 0 20       # every task succeeds
#
# Expected: every tenant ends "active" or "failed" at version 2; none stay
# "provisioning". The split is random, roughly (1 - fail_rate) active.
#
# Afterwards the worker is put back to its defaults (fail rate 0), unless
# KEEP_WORKER=1 is set. VERBOSE=1 also prints each JSON response.
# Needs: docker compose stack up, curl.
set -euo pipefail
cd "$(dirname "$0")/.."

FAIL_RATE="${1:-0.3}"
COUNT="${2:-10}"
API="${E2E_API_URL:-http://localhost:8080}"
TIMEOUT=60   # seconds to wait for all tenants to settle

restore() {
  if [[ -z "${KEEP_WORKER:-}" ]]; then
    echo; echo "restoring worker defaults (fail rate 0)"
    docker compose up -d worker >/dev/null 2>&1 || true
  fi
}
trap restore EXIT

vlog() { [[ -n ${VERBOSE:-} ]] && printf '    \033[2m%s\033[0m\n' "$*" >&2; return 0; }

echo "restarting worker with WORKER_FAIL_RATE=$FAIL_RATE"
WORKER_FAIL_RATE="$FAIL_RATE" docker compose up -d worker >/dev/null
for _ in $(seq 1 20); do   # wait until it reports its settings
  docker compose logs --since 30s worker 2>/dev/null | grep -q "\"fail_rate\":$FAIL_RATE" && break
  sleep 0.5
done
docker compose logs --since 30s worker | grep '"worker started"' | tail -1 || true

echo; echo "creating $COUNT tenants"
ids=()
run=$(date +%s)
for i in $(seq 1 "$COUNT"); do
  slug="mix-$run-$i"
  resp=$(curl -sS -X POST "$API/v1/tenants" -H 'Content-Type: application/json' \
           -d "{\"slug\":\"$slug\",\"name\":\"Mix $i\"}")
  vlog "→ POST /v1/tenants {\"slug\":\"$slug\",...}"
  vlog "← $resp"
  id=$(printf '%s' "$resp" | sed -n 's/.*"tenant":{"id":"\([^"]*\)".*/\1/p')
  if [[ -z $id ]]; then echo "  create failed: $resp" >&2; exit 1; fi
  echo "  $slug  $id"
  ids+=("$id")
done

status_of() { curl -sS "$API/v1/tenants/$1" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p'; }

echo; echo "waiting for the worker (max ${TIMEOUT}s)"
for t in $(seq 1 "$TIMEOUT"); do
  pending=0
  for id in "${ids[@]}"; do
    [[ $(status_of "$id") == provisioning ]] && pending=$((pending + 1))
  done
  [[ $pending -eq 0 ]] && break
  echo "  ${t}s: $pending still provisioning"
  sleep 1
done

echo; echo "results:"
for id in "${ids[@]}"; do
  t=$(curl -sS "$API/v1/tenants/$id")
  printf '%s' "$t" | sed -n 's/.*"slug":"\([^"]*\)".*"status":"\([^"]*\)","version":\([0-9]*\).*/  \1  \2  v\3/p'
  vlog "← $t"
  vlog "← tasks: $(curl -sS "$API/v1/tasks?tenant_id=$id")"
done

echo; echo "summary:"
for id in "${ids[@]}"; do status_of "$id"; done | sort | uniq -c

stuck=$(for id in "${ids[@]}"; do status_of "$id"; done | grep -c provisioning || true)
if [[ $stuck -gt 0 ]]; then
  echo "FAIL: $stuck tenant(s) still provisioning after ${TIMEOUT}s" >&2
  exit 1
fi
echo "OK: every tenant settled"
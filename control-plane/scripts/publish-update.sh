#!/usr/bin/env bash
# Publish a task progress update to RabbitMQ, acting as the worker.
# Uses only curl (RabbitMQ management HTTP API), so nothing else is needed.
#
#   scripts/publish-update.sh <task_id> <status> [update_id]
#   scripts/publish-update.sh --raw '<any payload>'
#
#   status     in_progress | done | failed (anything else is dead-lettered)
#   update_id  reuse one to simulate a duplicate delivery; omitted → a new one
#   --raw      publish the payload as-is (poison-message testing)
#
# Examples:
#   scripts/publish-update.sh 0192f3a1-... done
#   scripts/publish-update.sh 0192f3a1-... done 11111111-1111-7111-8111-111111111111   # run twice = duplicate
#   scripts/publish-update.sh 0192f3a1-... in_progress                                 # after done = out of order
#   scripts/publish-update.sh --raw 'not json'                                         # poison → task-updates.dlq
#
# Stop the real worker first so it doesn't race you: docker compose stop worker
#
# Env (defaults match compose.yaml):
#   RABBITMQ_MGMT_URL  http://localhost:15672
#   RABBITMQ_USER      controlplane
#   RABBITMQ_PASSWORD  controlplane
set -euo pipefail

MGMT="${RABBITMQ_MGMT_URL:-http://localhost:15672}"
USER="${RABBITMQ_USER:-controlplane}"
PASS="${RABBITMQ_PASSWORD:-controlplane}"
QUEUE="task-updates"

usage() { sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }

new_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then uuidgen | tr 'A-Z' 'a-z'
  elif [[ -r /proc/sys/kernel/random/uuid ]]; then cat /proc/sys/kernel/random/uuid
  else echo "need uuidgen (or pass an update_id explicitly)" >&2; exit 1
  fi
}

# json_string turns arbitrary text into a JSON string literal (quotes included).
json_string() {
  local s=$1
  s=${s//\\/\\\\}    # backslash
  s=${s//\"/\\\"}    # double quote
  s=${s//$'\n'/\\n}  # newline
  s=${s//$'\r'/\\r}
  s=${s//$'\t'/\\t}
  printf '"%s"' "$s"
}

[[ $# -ge 1 ]] || usage

if [[ $1 == "--raw" ]]; then
  [[ $# -eq 2 ]] || usage
  payload=$2
  echo "publishing raw payload to $QUEUE"
else
  [[ $# -ge 2 && $# -le 3 ]] || usage
  task_id=$1 status=$2 update_id=${3:-$(new_uuid)}
  now=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
  payload="{\"update_id\":\"$update_id\",\"task_id\":\"$task_id\",\"status\":\"$status\",\"occurred_at\":\"$now\"}"
  echo "publishing to $QUEUE: task=$task_id status=$status update_id=$update_id"
fi

# Default exchange ("amq.default" in the API) routes by queue name.
# delivery_mode 2 = persistent, like the real worker.
request="{\"routing_key\":\"$QUEUE\",\"payload\":$(json_string "$payload"),\"payload_encoding\":\"string\",\"properties\":{\"delivery_mode\":2,\"content_type\":\"application/json\"}}"

response=$(curl -sS -u "$USER:$PASS" -H 'content-type: application/json' \
  -X POST "$MGMT/api/exchanges/%2F/amq.default/publish" -d "$request")

if [[ $response == *'"routed":true'* ]]; then
  echo "ok: routed to $QUEUE"
else
  echo "publish failed: $response" >&2
  exit 1
fi

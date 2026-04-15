#!/bin/sh
set -eu

SELF_PROJECT="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' "$HOSTNAME" 2>/dev/null || true)"
PROJECT="${COMPOSE_PROJECT_NAME:-${SELF_PROJECT:-ratchet-loadtest}}"
SERVICE="${CHAOS_TARGET_SERVICE:-ratchet-node}"
INTERVAL="${CHAOS_INTERVAL_SECONDS:-30}"
DOWN_MIN="${CHAOS_DOWN_SECONDS_MIN:-5}"
DOWN_MAX="${CHAOS_DOWN_SECONDS_MAX:-20}"
PROBABILITY="${CHAOS_PROBABILITY:-1.0}"
SEED="${CHAOS_SEED:-$(date +%s)}"
RAND_VALUE=0
RAND_BETWEEN=0

rand32() {
  SEED=$(( (1103515245 * SEED + 12345) % 2147483648 ))
  RAND_VALUE="$SEED"
}

rand_between() {
  min="$1"
  max="$2"
  if [ "$max" -le "$min" ]; then
    RAND_BETWEEN="$min"
    return
  fi
  span=$(( max - min + 1 ))
  rand32
  RAND_BETWEEN=$(( min + (RAND_VALUE % span) ))
}

probability_threshold() {
  awk -v p="$PROBABILITY" 'BEGIN {
    if (p < 0) p = 0;
    if (p > 1) p = 1;
    printf "%d", p * 10000;
  }'
}

echo "chaos-monkey: project=$PROJECT service=$SERVICE interval=${INTERVAL}s probability=$PROBABILITY"

while true; do
  sleep "$INTERVAL"

  threshold="$(probability_threshold)"
  rand32
  roll=$(( RAND_VALUE % 10000 ))
  if [ "$roll" -ge "$threshold" ]; then
    echo "chaos-monkey: skipping this interval roll=$roll threshold=$threshold"
    continue
  fi

  containers="$(docker ps \
    --filter "label=com.docker.compose.project=$PROJECT" \
    --filter "label=com.docker.compose.service=$SERVICE" \
    --format '{{.ID}} {{.Names}}')"

  count="$(printf '%s\n' "$containers" | sed '/^$/d' | wc -l | tr -d ' ')"
  if [ "$count" = "0" ]; then
    echo "chaos-monkey: no running containers found for $PROJECT/$SERVICE"
    continue
  fi

  rand_between 1 "$count"
  index="$RAND_BETWEEN"
  target_line="$(printf '%s\n' "$containers" | sed -n "${index}p")"
  target_id="$(printf '%s' "$target_line" | awk '{print $1}')"
  target_name="$(printf '%s' "$target_line" | awk '{print $2}')"
  rand_between "$DOWN_MIN" "$DOWN_MAX"
  down_seconds="$RAND_BETWEEN"

  echo "chaos-monkey: stopping $target_name for ${down_seconds}s"
  docker stop -t 2 "$target_id" >/dev/null
  sleep "$down_seconds"
  echo "chaos-monkey: starting $target_name"
  docker start "$target_id" >/dev/null
done

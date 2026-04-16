#!/bin/sh
set -eu

SELF_PROJECT="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' "$HOSTNAME" 2>/dev/null || true)"
PROJECT="${COMPOSE_PROJECT_NAME:-${SELF_PROJECT:-ratchet-loadtest}}"
SERVICE="${CHAOS_TARGET_SERVICE:-ratchet-node}"
INTERVAL="${CHAOS_INTERVAL_SECONDS:-30}"
INTERVAL_MIN="${CHAOS_INTERVAL_SECONDS_MIN:-$INTERVAL}"
INTERVAL_MAX="${CHAOS_INTERVAL_SECONDS_MAX:-$INTERVAL}"
DOWN_MIN="${CHAOS_DOWN_SECONDS_MIN:-5}"
DOWN_MAX="${CHAOS_DOWN_SECONDS_MAX:-20}"
PROBABILITY="${CHAOS_PROBABILITY:-1.0}"
TARGETS_MIN="${CHAOS_TARGETS_PER_CYCLE_MIN:-1}"
TARGETS_MAX="${CHAOS_TARGETS_PER_CYCLE_MAX:-2}"
MIN_RUNNING="${CHAOS_MIN_RUNNING:-1}"
ACTIONS="${CHAOS_ACTIONS:-stop,kill,pause,restart}"
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

count_lines() {
  printf '%s\n' "$1" | sed '/^$/d' | wc -l | tr -d ' '
}

pick_line() {
  value="$1"
  index="$2"
  printf '%s\n' "$value" | sed -n "${index}p"
}

remove_line() {
  value="$1"
  index="$2"
  printf '%s\n' "$value" | awk -v skip="$index" 'NF { if (++n != skip) print }'
}

pick_action() {
  action_list="$(printf '%s' "$ACTIONS" | tr ',' '\n' | sed 's/^ *//;s/ *$//' | sed '/^$/d')"
  action_count="$(count_lines "$action_list")"
  if [ "$action_count" = "0" ]; then
    PICKED_ACTION="stop"
    return
  fi
  rand_between 1 "$action_count"
  PICKED_ACTION="$(pick_line "$action_list" "$RAND_BETWEEN")"
}

disrupt_target() {
  action="$1"
  target_id="$2"
  target_name="$3"
  down_seconds="$4"

  case "$action" in
    stop)
      echo "chaos-monkey: stopping $target_name for ${down_seconds}s"
      docker stop -t 2 "$target_id" >/dev/null
      sleep "$down_seconds"
      echo "chaos-monkey: starting $target_name"
      docker start "$target_id" >/dev/null
      ;;
    kill)
      echo "chaos-monkey: killing $target_name for ${down_seconds}s"
      docker kill "$target_id" >/dev/null
      sleep "$down_seconds"
      echo "chaos-monkey: starting $target_name after kill"
      docker start "$target_id" >/dev/null
      ;;
    pause)
      echo "chaos-monkey: pausing $target_name for ${down_seconds}s"
      docker pause "$target_id" >/dev/null
      sleep "$down_seconds"
      echo "chaos-monkey: unpausing $target_name"
      docker unpause "$target_id" >/dev/null
      ;;
    restart)
      echo "chaos-monkey: restarting $target_name with ${down_seconds}s cooldown"
      docker restart -t 2 "$target_id" >/dev/null
      sleep "$down_seconds"
      ;;
    *)
      echo "chaos-monkey: unknown action '$action' for $target_name, defaulting to stop"
      docker stop -t 2 "$target_id" >/dev/null
      sleep "$down_seconds"
      docker start "$target_id" >/dev/null
      ;;
  esac
}

echo "chaos-monkey: project=$PROJECT service=$SERVICE interval=${INTERVAL_MIN}-${INTERVAL_MAX}s probability=$PROBABILITY actions=$ACTIONS targets=${TARGETS_MIN}-${TARGETS_MAX} min_running=$MIN_RUNNING"

while true; do
  rand_between "$INTERVAL_MIN" "$INTERVAL_MAX"
  sleep_seconds="$RAND_BETWEEN"
  sleep "$sleep_seconds"

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

  available_to_disrupt=$(( count - MIN_RUNNING ))
  if [ "$available_to_disrupt" -le 0 ]; then
    echo "chaos-monkey: preserving minimum running containers count=$count min_running=$MIN_RUNNING"
    continue
  fi

  rand_between "$TARGETS_MIN" "$TARGETS_MAX"
  target_count="$RAND_BETWEEN"
  if [ "$target_count" -gt "$available_to_disrupt" ]; then
    target_count="$available_to_disrupt"
  fi

  remaining="$containers"
  current_target=1
  while [ "$current_target" -le "$target_count" ]; do
    remaining_count="$(count_lines "$remaining")"
    if [ "$remaining_count" -le 0 ]; then
      break
    fi

    rand_between 1 "$remaining_count"
    index="$RAND_BETWEEN"
    target_line="$(pick_line "$remaining" "$index")"
    remaining="$(remove_line "$remaining" "$index")"
    target_id="$(printf '%s' "$target_line" | awk '{print $1}')"
    target_name="$(printf '%s' "$target_line" | awk '{print $2}')"
    rand_between "$DOWN_MIN" "$DOWN_MAX"
    down_seconds="$RAND_BETWEEN"
    pick_action
    disrupt_target "$PICKED_ACTION" "$target_id" "$target_name" "$down_seconds" &
    current_target=$(( current_target + 1 ))
  done
done

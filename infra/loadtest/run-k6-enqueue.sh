#!/bin/sh
set -eu

STORE="${1:-postgresql}"
NODES="${2:-3}"

case "$STORE" in
  postgresql|mysql|mongodb) ;;
  *)
    echo "usage: sh infra/loadtest/run-k6-enqueue.sh [postgresql|mysql|mongodb] [expected-nodes]" >&2
    exit 2
    ;;
esac

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR"

export MIN_ACCEPT_NODES="${MIN_ACCEPT_NODES:-$NODES}"
export TARGET_URL="${TARGET_URL:-http://gateway:8080}"

docker compose -f compose.yml -f "compose.${STORE}.yml" --profile k6 run --rm --no-deps k6

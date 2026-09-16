#!/bin/sh
set -eu

STORE="${1:-postgresql}"
NODES="${2:-3}"

case "$STORE" in
  postgresql|mysql|mongodb) ;;
  *)
    echo "usage: sh infra/loadtest/run.sh [postgresql|mysql|mongodb] [nodes] [chaos]" >&2
    exit 2
    ;;
esac

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR"

case "$NODES" in
  ''|*[!0-9]*|0) echo "nodes must be a positive integer" >&2; exit 2 ;;
esac

if [ "$#" -gt 0 ]; then shift; fi
if [ "$#" -gt 0 ]; then shift; fi

COMPOSE_FILES="-f compose.yml -f compose.${STORE}.yml"
COMPOSE_PROFILES=""

for extra in "$@"; do
  case "$extra" in
    chaos)
      COMPOSE_PROFILES="$COMPOSE_PROFILES --profile chaos"
      ;;
    *)
      echo "usage: sh infra/loadtest/run.sh [postgresql|mysql|mongodb] [nodes] [chaos]" >&2
      exit 2
      ;;
  esac
done

# shellcheck disable=SC2086
docker compose $COMPOSE_FILES $COMPOSE_PROFILES up --build --scale "ratchet-node=${NODES}"

#!/bin/sh
set -eu

STORE="${1:-postgresql}"
NODES="${2:-3}"

case "$STORE" in
  postgresql|mysql|oracle|sqlserver|mongodb) ;;
  *)
    echo "usage: sh infra/loadtest/run.sh [postgresql|mysql|oracle|sqlserver|mongodb] [nodes] [chaos]" >&2
    exit 2
    ;;
esac

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR"

shift 2

COMPOSE_FILES="-f compose.yml -f compose.${STORE}.yml"
COMPOSE_PROFILES=""

for extra in "$@"; do
  case "$extra" in
    chaos)
      COMPOSE_PROFILES="$COMPOSE_PROFILES --profile chaos"
      ;;
    *)
      echo "usage: sh infra/loadtest/run.sh [postgresql|mysql|oracle|sqlserver|mongodb] [nodes] [chaos]" >&2
      exit 2
      ;;
  esac
done

# shellcheck disable=SC2086
docker compose $COMPOSE_FILES $COMPOSE_PROFILES up --build --scale "ratchet-node=${NODES}"

#!/bin/sh
set -eu

STORE="${1:-postgresql}"
NODES="${2:-3}"
PROFILE="${3:-}"

case "$STORE" in
  postgresql|mysql|mongodb) ;;
  *)
    echo "usage: sh infra/loadtest/run.sh [postgresql|mysql|mongodb] [nodes] [chaos]" >&2
    exit 2
    ;;
esac

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ "$PROFILE" = "chaos" ]; then
  docker compose -f compose.yml -f "compose.${STORE}.yml" --profile chaos \
    up --build --scale "ratchet-node=${NODES}"
else
  docker compose -f compose.yml -f "compose.${STORE}.yml" \
    up --build --scale "ratchet-node=${NODES}"
fi

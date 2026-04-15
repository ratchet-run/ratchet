#!/bin/sh
set -eu

WORKLOAD="${1:-sleep}"
JOBS="${2:-1000}"
SLEEP_MS="${3:-5}"
FAILURE_RATE="${4:-0.0}"
SLEEP_JITTER_MS="${5:-${SLEEP_JITTER_MS:-0}}"
SLEEP_SPIKE_RATE="${6:-${SLEEP_SPIKE_RATE:-0.0}}"
SLEEP_SPIKE_MS="${7:-${SLEEP_SPIKE_MS:-0}}"
GATEWAY="${RATCHET_GATEWAY_URL:-http://localhost:8080}"

curl -sS -X POST "$GATEWAY/api/runs" \
  -H 'Content-Type: application/json' \
  -d "{\"workload\":\"$WORKLOAD\",\"jobs\":$JOBS,\"sleepMs\":$SLEEP_MS,\"sleepJitterMs\":$SLEEP_JITTER_MS,\"sleepSpikeRate\":$SLEEP_SPIKE_RATE,\"sleepSpikeMs\":$SLEEP_SPIKE_MS,\"failureRate\":$FAILURE_RATE}"

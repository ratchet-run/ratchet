#!/bin/sh
# Measured triage run: server-side perf_schema + client-side pool gauges + JVM JFR, all overlapping
# a 2-minute 2000/s noop enqueue load, on a shared RUN_ID. Runs entirely on deep-thought.
set -u
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR/.." || exit 1

RUN_ID="${RUN_ID:-triage-$(date +%s)}"
OUT="profiling-out/mysql-${RUN_ID}"
mkdir -p "$OUT"
echo "[run] RUN_ID=$RUN_ID start=$(date +%T) load1=$(cut -d' ' -f1 /proc/loadavg)"

# host load + container count sampler (~200s)
( for i in $(seq 1 40); do
    echo "$(date +%T) load=$(cut -d' ' -f1-3 /proc/loadavg) containers=$(docker ps -q | wc -l)"
    sleep 5
  done > "$OUT/host-load.log" 2>&1 ) &
HLPID=$!

# server-side MySQL capture (resets perf_schema, samples, post-snapshots)
DURATION=180 RUN_ID="$RUN_ID" STORE=mysql sh profiling/mysql-capture.sh > "$OUT/mysql-capture.console.log" 2>&1 &
MPID=$!

# JVM-side JFR on one node (low overhead, corroboration)
DURATION=180 RUN_ID="$RUN_ID" STORE=mysql sh profiling/jfr-capture.sh > "$OUT/jfr-capture.console.log" 2>&1 &
JPID=$!

# let perf_schema reset + JFR start settle before load
sleep 6

echo "[run] launching k6 2000/2m noop $(date +%T)"
LOAD_RATE=2000 LOAD_DURATION=2m JOB_WORKLOAD=noop RUN_ID="$RUN_ID" \
  sh run-k6-enqueue.sh mysql 10 > "$OUT/k6-summary.log" 2>&1
echo "[run] k6 exit=$? $(date +%T)"

# queue-drain snapshots after enqueue stops (execution throughput)
curl -sS --max-time 8 http://localhost:8080/api/cluster > "$OUT/cluster-post.json" 2>/dev/null
sleep 30
curl -sS --max-time 8 http://localhost:8080/api/cluster > "$OUT/cluster-post-30s.json" 2>/dev/null

wait "$MPID" 2>/dev/null
wait "$JPID" 2>/dev/null
wait "$HLPID" 2>/dev/null
echo "[run] captures complete $(date +%T)"
echo "[run] OUT=$OUT"
ls -la "$OUT"

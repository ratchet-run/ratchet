#!/bin/sh
# Pool-vs-lock triage: pull the decisive time series out of Prometheus as JSON for offline analysis.
#
# Captures the client-side pool gauges (ratchet_ds_pool_*, published by DataSourcePoolMetricsBinder)
# and the server-side MySQL lock/throughput series (from mysqld-exporter) over the run window. The
# two sides side by side are the triage verdict: pool saturation + blocking time with low DB lock
# time means connection-pool starvation; low pool wait with rising InnoDB row-lock time/waits means
# MySQL hot-table contention.
#
# Run after the measured load run (window defaults to the last DURATION seconds):
#   DURATION=120 sh infra/loadtest/profiling/prom-dump.sh
# From a workstation over the SSH tunnel, point at the forwarded port:
#   PROM_URL=http://localhost:19093 DURATION=120 sh infra/loadtest/profiling/prom-dump.sh
set -eu

PROM_URL="${PROM_URL:-http://localhost:9090}"
DURATION="${DURATION:-120}"
STEP="${STEP:-5s}"
RUN_ID="${RUN_ID:-$(date +%s)}"
STORE="${STORE:-mysql}"

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

OUT="${OUT:-profiling-out/${STORE}-${RUN_ID}}"
mkdir -p "$OUT"

END="$(date +%s)"
START="$((END - DURATION))"

dump() {
  name="$1"
  query="$2"
  curl -sS -G "$PROM_URL/api/v1/query_range" \
    --data-urlencode "query=$query" \
    --data-urlencode "start=$START" \
    --data-urlencode "end=$END" \
    --data-urlencode "step=$STEP" \
    > "$OUT/prom-$name.json"
  echo "[prom] $name -> $OUT/prom-$name.json"
}

# Client side: every pool statistic the binder discovered, per node.
dump pool '{__name__=~"ratchet_ds_pool_.*"}'

# Server side: MySQL contention and throughput.
dump mysql_threads_running 'mysql_global_status_threads_running'
dump mysql_innodb_row_lock_current_waits 'mysql_global_status_innodb_row_lock_current_waits'
dump mysql_innodb_row_lock_time_rate 'rate(mysql_global_status_innodb_row_lock_time[15s])'
dump mysql_innodb_row_lock_waits_rate 'rate(mysql_global_status_innodb_row_lock_waits[15s])'
dump mysql_queries_rate 'rate(mysql_global_status_queries[15s])'

# Scheduler side: queue drain, for correlating with the enqueue rate.
dump ratchet_jobs_by_status '{__name__="ratchet_store_jobs"}'

# Per-path commit attribution: the store-operation timer already counts every store op by
# operation + outcome, so the representative op of each path's transaction is its commit count
# (mark_succeeded = completions, claim_mark_running_batch = claims, the job insert = submits).
# Range series of the cumulative _count/_sum; take last-minus-first over the window for per-run totals.
dump store_operation_count '{__name__="ratchet_store_operation_seconds_count"}'
dump store_operation_sum '{__name__="ratchet_store_operation_seconds_sum"}'

# Execution-side job counters (started vs completed vs failed), independent cross-check.
dump jobs_started '{__name__="ratchet_jobs_started_total"}'
dump jobs_completed '{__name__="ratchet_jobs_completed_total"}'
dump jobs_failed '{__name__="ratchet_jobs_failed_total"}'

echo "[prom] window ${START}..${END} (${DURATION}s) done -> $OUT"

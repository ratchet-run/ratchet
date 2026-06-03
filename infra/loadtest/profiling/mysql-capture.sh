#!/bin/sh
# Pool-vs-lock triage: MySQL server-side capture.
#
# Resets the cumulative performance_schema summaries, samples live lock/contention state on an
# interval for the duration of a load run, then snapshots the top statement digests and wait
# events. Output is plain tabular text meant for offline analysis, not a dashboard.
#
# Run on the load-test host (deep-thought) with the MySQL cluster already up:
#   sh infra/loadtest/run.sh mysql 10            # terminal 1, leave running
#   DURATION=120 sh infra/loadtest/profiling/mysql-capture.sh   # terminal 2, start with the k6 run
set -eu

STORE="${STORE:-mysql}"
DURATION="${DURATION:-120}"
INTERVAL="${INTERVAL:-5}"
RUN_ID="${RUN_ID:-$(date +%s)}"

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR/.."

OUT="${OUT:-profiling-out/${STORE}-${RUN_ID}}"
mkdir -p "$OUT"

COMPOSE="docker compose -f compose.yml -f compose.${STORE}.yml"
mysql_sql() {
  # shellcheck disable=SC2086
  $COMPOSE exec -T mysql mysql -uroot -prootpassword ratchet "$@"
}

echo "[capture] store=$STORE duration=${DURATION}s interval=${INTERVAL}s out=$OUT"

# --- pre: reset cumulative counters so the measured window is clean -----------------------------
mysql_sql -e "
  TRUNCATE performance_schema.events_statements_summary_by_digest;
  TRUNCATE performance_schema.events_waits_summary_global_by_event_name;
" 2>&1 | tee "$OUT/pre-reset.log"

# --- during: sample live contention state -------------------------------------------------------
: > "$OUT/during-processlist.tsv"
: > "$OUT/during-lock-waits.tsv"
: > "$OUT/during-innodb-status.log"
elapsed=0
while [ "$elapsed" -lt "$DURATION" ]; do
  ts="$(date +%FT%T)"

  {
    echo "=== $ts ==="
    mysql_sql -e "
      SELECT COUNT(*) AS sessions, COALESCE(state,'') AS state
      FROM information_schema.PROCESSLIST
      WHERE COMMAND <> 'Sleep'
      GROUP BY state ORDER BY sessions DESC;"
  } >> "$OUT/during-processlist.tsv" 2>&1

  {
    echo "=== $ts ==="
    mysql_sql -e "
      SELECT
        (SELECT COUNT(*) FROM performance_schema.data_lock_waits) AS lock_wait_edges,
        (SELECT COUNT(*) FROM information_schema.INNODB_TRX WHERE trx_state='LOCK WAIT') AS trx_lock_wait;"
    mysql_sql -e "
      SELECT REQUESTING_ENGINE_TRANSACTION_ID AS waiter, BLOCKING_ENGINE_TRANSACTION_ID AS blocker
      FROM performance_schema.data_lock_waits LIMIT 20;"
  } >> "$OUT/during-lock-waits.tsv" 2>&1

  {
    echo "=== $ts ==="
    mysql_sql -e "SHOW ENGINE INNODB STATUS\G"
  } >> "$OUT/during-innodb-status.log" 2>&1

  sleep "$INTERVAL"
  elapsed=$((elapsed + INTERVAL))
done

# --- post: where did time actually go -----------------------------------------------------------
mysql_sql -e "
  SELECT
    LEFT(DIGEST_TEXT, 160)              AS statement,
    COUNT_STAR                          AS calls,
    ROUND(SUM_TIMER_WAIT/1e12, 3)       AS total_s,
    ROUND(AVG_TIMER_WAIT/1e9, 3)        AS avg_ms,
    ROUND(SUM_LOCK_TIME/1e12, 3)        AS lock_s,
    SUM_ROWS_EXAMINED                   AS rows_examined,
    SUM_ROWS_AFFECTED                   AS rows_affected
  FROM performance_schema.events_statements_summary_by_digest
  ORDER BY SUM_TIMER_WAIT DESC LIMIT 25;" 2>&1 | tee "$OUT/post-statement-digests.tsv"

mysql_sql -e "
  SELECT EVENT_NAME AS wait_event, COUNT_STAR AS calls, ROUND(SUM_TIMER_WAIT/1e12, 3) AS total_s
  FROM performance_schema.events_waits_summary_global_by_event_name
  WHERE COUNT_STAR > 0
  ORDER BY SUM_TIMER_WAIT DESC LIMIT 30;" 2>&1 | tee "$OUT/post-wait-events.tsv"

echo "[capture] done -> $OUT"

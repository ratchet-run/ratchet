# Oracle Claim EXPLAIN Plans

The executable-claim hot path is the only query whose plan is regression-guarded. `OracleExplainPlanCaptureIT`
captures it live against a Testcontainers Oracle 23ai instance (seeded with pending executable jobs and
freshly gathered stats) and writes the rendered plan to `target/explain-plans/oracle-optimized-claim.txt`.

## Optimized Executable Claim

The RI hot path calls `claimNextBatchOptimized(jobType, limit, nodeId)`. On Oracle this runs as two phases
(Oracle rejects `FETCH FIRST` combined with `FOR UPDATE SKIP LOCKED`, ORA-02014): Phase A selects the top-N
candidates without locking, Phase B re-locks the still-`PENDING` ids with `FOR UPDATE SKIP LOCKED`. The plan
that matters is Phase A — the candidate select:

```sql
EXPLAIN PLAN SET STATEMENT_ID = 'ratchet_claim' FOR
SELECT /*+ INDEX(sjq idx_claim_executable) */
       job_id, status, job_type, priority, scheduled_time,
       version, timeout_sec, picked_by, picked_at, business_key,
       attempts, max_retries, execution_target
FROM scheduler_job_queue sjq
WHERE status = 'PENDING'
  AND scheduled_time <= CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
  AND job_type = 'SINGLE'
ORDER BY (priority + FLOOR(GREATEST(0,
           (CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - CAST(scheduled_time AS DATE)) * 1440)
           / 15)) DESC,
         scheduled_time ASC,
         job_id ASC
FETCH FIRST 50 ROWS ONLY;

SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', 'ratchet_claim', 'ALL'));
```

`EXPLAIN PLAN` / `DBMS_XPLAN.DISPLAY` are granted to PUBLIC, so no extra privilege is needed. The `INDEX`
hint forces the covering index the same way MySQL's `FORCE INDEX` does — Oracle silently ignores an
un-honorable hint and falls back to a full scan, so the assertion is a structural regression guard.

A `SORT ORDER BY` step is expected: effective priority includes an age-boost expression based on the current
UTC time, so the optimizer cannot satisfy the full ordering from a static B-tree. The invariant the IT
asserts is that the plan reaches due rows through `IDX_CLAIM_EXECUTABLE` (an `INDEX RANGE SCAN`) before
sorting, and never `TABLE ACCESS FULL` on `scheduler_job_queue`.

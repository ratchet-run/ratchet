# SQL Server Claim EXPLAIN Plans

Captured on 2026-04-19 with Testcontainers `postgres:16` after seeding 600 pending executable jobs
across multiple execution types.

## Optimized Executable Claim

The RI hot path calls `claimNextBatchOptimized(jobType, limit, nodeId)`, which runs as **two
statements**, not a CTE: a `SELECT … FOR UPDATE SKIP LOCKED` that locks the due rows in
effective-priority order, then a separate `UPDATE … WHERE job_id IN (…)` that transitions exactly
those rows to RUNNING. The split is deliberate — a single `UPDATE … RETURNING` from a `WITH picked
AS (…)` CTE would emit rows in heap order rather than the claimed priority order (see the
`SqlserverJobClaimOperations` claim-select Javadoc). The plan below is for the `SELECT`, captured
with `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` in a transaction that is rolled back. Because the
fixture table is intentionally small, the test sets `enable_seqscan = off` locally to verify that
the intended claim index remains selectable.

```sql
-- Statement 1: lock the due rows in effective-priority order
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT job_id
FROM scheduler_job_queue
WHERE status = 'PENDING'
  AND scheduled_time <= statement_timestamp()
  AND job_type = 'SINGLE'
ORDER BY
  (priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - scheduled_time))) / (60.0 * 15))) DESC,
  scheduled_time ASC,
  job_id ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;

-- Statement 2: transition exactly the locked rows, preserving the order from statement 1
UPDATE scheduler_job_queue
SET status = 'RUNNING',
    picked_by = 'explain-node',
    picked_at = statement_timestamp(),
    updated_at = statement_timestamp(),
    version = version + 1
WHERE job_id IN ( /* job_ids returned by statement 1, in order */ )
  AND status = 'PENDING';
```

Relevant JSON excerpt:

```json
{
  "Node Type": "Bitmap Index Scan",
  "Index Name": "idx_claim_executable",
  "Actual Rows": 150,
  "Index Cond": "((job_type = 'SINGLE'::text) AND (scheduled_time <= statement_timestamp()))"
}
```

The selected rows feed a `Sort` node on computed effective priority. That sort is expected because
the age-boost term depends on `statement_timestamp()` and is not immutable. The index invariant is
that the plan uses `idx_claim_executable` (the partial index on `scheduler_job_queue` filtered by
`status = 'PENDING'`) to reduce the due candidate set before sorting.

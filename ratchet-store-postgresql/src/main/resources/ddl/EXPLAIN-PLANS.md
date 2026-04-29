# PostgreSQL Claim EXPLAIN Plans

Captured on 2026-04-19 with Testcontainers `postgres:16` after seeding 600 pending executable jobs
across multiple execution types.

## Optimized Executable Claim

The RI hot path calls `claimNextBatchOptimized(jobType, limit, nodeId)`, so the representative
claim shape is a single execution type. The test uses `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` in a
transaction and rolls the update back. Because the fixture table is intentionally small, the test
sets `enable_seqscan = off` locally to verify that the intended claim index remains selectable.

```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH picked AS (
  SELECT job_id
  FROM scheduler_job_queue
  WHERE status = 'PENDING'
    AND scheduled_time <= statement_timestamp()
    AND job_type = 'SINGLE'
  ORDER BY
    (priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - scheduled_time))) / (60.0 * 15))) DESC,
    scheduled_time ASC,
    job_id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 50
)
UPDATE scheduler_job_queue AS q
SET status = 'RUNNING',
    picked_by = 'explain-node',
    picked_at = statement_timestamp(),
    updated_at = statement_timestamp(),
    version = version + 1
FROM picked
WHERE q.job_id = picked.job_id
RETURNING q.job_id;
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

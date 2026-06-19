# MySQL Claim EXPLAIN Plans

Captured on 2026-04-19 with Testcontainers `mysql:8.0` after seeding 600 pending executable jobs
across multiple execution types.

## Optimized Executable Claim

The RI hot path calls `claimNextBatchOptimized(jobType, limit, nodeId)`, so the representative
claim shape is a single execution type:

```sql
EXPLAIN FORMAT=JSON
SELECT job_id, status, job_type, priority, scheduled_time,
       version, timeout_sec, picked_by, picked_at, business_key,
       attempts, max_retries
FROM scheduler_job_queue FORCE INDEX (idx_claim_executable)
WHERE status = 'PENDING'
  AND scheduled_time <= NOW(3)
  AND job_type = 'SINGLE'
ORDER BY
  (priority + FLOOR(GREATEST(0, TIMESTAMPDIFF(MINUTE, scheduled_time, NOW(3))) / 15)) DESC,
  scheduled_time ASC,
  job_id ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;
```

Relevant JSON excerpt:

```json
{
  "table_name": "scheduler_job_queue",
  "access_type": "range",
  "possible_keys": ["idx_claim_executable"],
  "key": "idx_claim_executable",
  "used_key_parts": ["status", "job_type", "scheduled_time"],
  "rows_examined_per_scan": 150,
  "index_condition": "((`ratchet_test`.`scheduler_job_queue`.`status` = 'PENDING') and (`ratchet_test`.`scheduler_job_queue`.`scheduled_time` <= <cache>(now(3))) and (`ratchet_test`.`scheduler_job_queue`.`job_type` = 'SINGLE'))"
}
```

`using_filesort=true` is expected: effective priority includes an age-boost expression based on
`NOW(3)`, so MySQL cannot satisfy the full ordering from a static B-tree. The index invariant is
that the plan filters to due rows with `idx_claim_executable` before sorting.

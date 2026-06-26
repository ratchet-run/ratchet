# SQL Server Claim EXPLAIN Plans

Captured 2026-06-24 by `SqlserverExplainPlanCaptureIT` against Testcontainers
`mcr.microsoft.com/mssql/server:2022-latest` after seeding pending executable jobs across multiple
execution types. The IT writes the full plan to `target/explain-plans/sqlserver-optimized-claim.xml`;
the summary below is the relevant excerpt.

## Optimized Executable Claim

The RI hot path calls `claimNextBatchOptimized(jobType, limit, nodeId)`, which runs as **two
statements**, not a CTE: a `SELECT … WITH (UPDLOCK, READPAST, ROWLOCK)` that locks the due rows in
effective-priority order (`READPAST` makes a concurrent claim skip rows another node already locked),
then a separate `UPDATE … WHERE job_id IN (…)` that transitions exactly those rows to RUNNING. The
split is deliberate — a single `UPDATE … OUTPUT` from a `WITH picked AS (…)` CTE would emit rows in
heap order rather than the claimed priority order (see the `SqlserverJobClaimOperations` claim-select
Javadoc).

The plan below is for the `SELECT`, captured with `SET SHOWPLAN_XML ON`, which returns the estimated
plan without executing the statement. The filtered index is forced with `WITH
(INDEX(idx_claim_executable))`: SQL Server rejects that hint outright if the filtered index can no
longer serve the claim shape, so a passing capture proves the index remains usable. (The production
claim adds an age-boost term to the `ORDER BY`; it is omitted here so the captured plan isolates index
selection, and the boost only reinforces the `Sort` that is already present.)

```sql
SELECT job_id, status, job_type, priority, scheduled_time, version, timeout_sec,
       picked_by, picked_at, business_key, attempts, max_retries, execution_target
FROM scheduler_job_queue WITH (INDEX(idx_claim_executable), UPDLOCK, READPAST, ROWLOCK)
WHERE status = 'PENDING'
  AND scheduled_time <= SYSUTCDATETIME()
  AND job_type = 'SINGLE'
ORDER BY priority DESC, scheduled_time ASC, job_id ASC
OFFSET 0 ROWS FETCH NEXT 50 ROWS ONLY;
```

Plan shape (top-down), from the captured SHOWPLAN XML:

```
Sort (TopN Sort)                          -- effective-priority order, bounded by FETCH NEXT 50
  └─ Nested Loops (Inner Join)
       ├─ Index Seek  [idx_claim_executable]      Ordered=1  ScanDirection=FORWARD  (filtered index)
       └─ Clustered Index Seek  [pk_scheduler_job_queue]   Lookup=1   -- fetch the non-covered columns
```

The seek on `idx_claim_executable` (the filtered index on `scheduler_job_queue`, predicated on
`status = 'PENDING'`) reduces the due-candidate set, and a nested-loop key lookup into the clustered
primary key fetches the remaining columns. The `TopN Sort` on top is expected: rows are ordered by
effective priority, which the index alone does not provide. The index invariant the IT asserts is
that the plan references both `scheduler_job_queue` and `idx_claim_executable` — i.e. the claim still
seeks the filtered index rather than scanning the table.

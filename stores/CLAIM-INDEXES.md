# Priority-ordered SQL claims

V008 adds `idx_claim_pending_priority` alongside `idx_claim_executable` in PostgreSQL,
MySQL, SQL Server, and Oracle. Released migrations are unchanged; consolidated schemas
also include the new index.

The optimized poller query selects one job type and orders by `priority DESC,
scheduled_time ASC, job_id ASC` when priority boosting is disabled. The new index
matches that order. PostgreSQL and SQL Server filter it to `status = 'PENDING'`;
MySQL and Oracle prepend `status` to the key. MySQL's existing `FORCE INDEX` hint now
allows both claim indexes so its optimizer can choose between them.

The due-time-first index remains available for selective due-time predicates and
boosted priorities. Dynamic priority boosting and claims spanning several job types
can still require sorting. Scheduling order, row-lock hints, ownership checks,
transaction isolation, and Oracle's two-phase claim protocol are unchanged.

## Deployment

Apply V008 before starting the updated MySQL code, which names the new index in its
hint. Follow the normal migration procedure for the other stores too. Index creation
consumes disk space and adds maintenance work on queue writes. The bundled migration
uses ordinary index creation; plan a maintenance window for large live queues.

For PostgreSQL, an operator who needs an online build can create the same index with
`CREATE INDEX CONCURRENTLY` outside a transaction before running the migration. Verify
that the completed index is valid and has the exact V008 definition first; a failed
concurrent build can leave an invalid index, and `IF NOT EXISTS` does not repair it.
The bundled migrator runs scripts in transactions, so V008 intentionally does not use
`CONCURRENTLY`.

## SQL Server migration batches

V007 is unreleased and includes the single-statement directive for its
`IF ... BEGIN ... END` block. SQL Server's migration dialect requests native JDBC
batch execution, preserving block and local-variable scope for every migration;
the other stores retain their existing splitting behavior. The migrator drains every
result and update count before recording a version, and rolls back on a late batch
error. Released migration checksum validation remains unchanged.

## Evidence and limits

On the local 32-CPU PostgreSQL 15 / ten-node load-test stack (2026-09-16), adding the
index to a draining backlog changed the actual 100-row claim plan from a sequential
scan and external merge sort to an ordered index scan. A before/after sample took
344.909 ms versus 1.426 ms. The pre-index 30-second interval completed about 450
jobs/sec; the first post-index interval completed about 2,592 jobs/sec. These are
samples on a shrinking queue, not a controlled steady-state capacity guarantee.

A subsequent requested 3,000/sec, two-minute enqueue run accepted 306,777 jobs; all
completed with zero failed jobs and zero database deadlocks. Its enqueue p95 was
1.630 seconds and k6 exited 99 on the latency threshold. PostgreSQL showed substantial
shared-buffer lock waits, and unrelated database tests were running on the same VM.
This run does not establish sustained 3,000/sec end-to-end capacity.

Regression coverage checks PostgreSQL's default planner against 100,000-row due and
future-heavy queues. Additional native plan tests check unboosted index ordering in
each other SQL dialect, with 100,000-row backlogs for Oracle and SQL Server. Existing claim contracts cover priority boosting, future-job
exclusion, routing, limits, and ownership. Live throughput evidence remains
PostgreSQL-only; plan checks in the other stores are not throughput benchmarks.

Detailed local captures: `infra/loadtest/profiling-out/claim-priority-index-20260916/`
(ignored runtime artifacts). This change concerns executable queue claims, separately
from startup recurring-cleanup retries and MySQL lease/deadlock investigations.

Final scoped verification: 651 passed, 3 existing introspection skips; formatting passed.
See the local report for per-suite counts, negative controls, warnings, and exit statuses.

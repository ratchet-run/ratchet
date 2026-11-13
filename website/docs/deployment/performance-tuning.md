---
sidebar_position: 6
title: Performance Tuning
description: Tuning Ratchet's polling intervals, thread pools, batch sizes, timeouts, and database for high-throughput job scheduling.
---

# Performance Tuning

Ratchet's performance depends on polling frequency, thread pool sizing, batch sizes, database tuning, and the nature of your jobs. This guide covers how to tune each parameter for your workload.

## Polling configuration

The polling engine periodically queries the database for jobs that are due for execution. The two key parameters are the poll interval and the batch size.

### Poll interval

```bash
RATCHET_POLLER_MIN_DELAY_MS=2000
RATCHET_POLLER_MAX_DELAY_MS=10000
```

Ratchet uses adaptive polling, so the minimum and maximum delay matter more than a single fixed interval. Lower minimums improve latency; higher maximums reduce database load during idle periods.

| Minimum Delay | Typical Latency | DB Queries/Min | Use Case |
|----------|---------|----------------|----------|
| 1000ms | ~1 second | 60 | Real-time processing, low-latency requirements |
| 2000ms (default) | ~2 seconds | 30 | General-purpose, balanced |
| 3000ms | ~3 seconds | 20 | High-throughput production workloads |
| 10000ms | ~10 seconds | 6 | Light workloads, reduce DB pressure |

Lowering the minimum below 1 second is not recommended: the overhead of frequent queries usually outweighs the latency benefit.

### Batch size

```bash
RATCHET_POLLER_BATCH_SIZE=100
```

How many jobs to fetch in a single poll. Larger batches reduce the number of queries but consume more memory and increase the time between when a job is fetched and when it starts executing.

| Batch Size | Queries | Memory | Best For |
|-----------|---------|--------|----------|
| 10 | More frequent | Low | Few jobs, short execution times |
| 50 (default) | Balanced | Moderate | General-purpose |
| 100 | Fewer queries | Moderate | High-throughput production workloads |
| 500 | Fewer queries | Higher | High-throughput with many pending jobs |
| 1000+ | Minimal | Significant | Bulk processing, batch workloads |

### Adaptive polling

Adaptive polling is enabled by default. The polling engine automatically adjusts its interval based on queue depth:
- **Queue has jobs**: Poll at the configured interval or faster
- **Queue is empty**: Gradually back off to reduce unnecessary queries
- **New work notification**: Immediately poll when a `ClusterCoordinator` signals new work

Adaptive polling works well in environments with variable load: it provides low latency during busy periods and reduces database overhead during idle periods.

The deep idle thresholds control how aggressively the engine backs off:

```bash
# Time of no work before entering deep idle
RATCHET_POLLER_DEEP_IDLE_THRESHOLD_MS=60000

# Poll interval during deep idle
RATCHET_POLLER_DEEP_IDLE_DELAY_MS=30000

# Maximum poll delay (cap for backoff)
RATCHET_POLLER_MAX_DELAY_MS=10000
```

## Thread pool sizing

### Executor threads

```bash
RATCHET_THREAD_POOL_SIZE_SINGLE=16
```

Ratchet uses per-execution-type pools. Tune the ones you actually use instead of a single global executor size.

**For CPU-bound jobs** (computation, data processing):
```bash
# Match CPU cores for one-off jobs
RATCHET_THREAD_POOL_SIZE_SINGLE=8
```

**For I/O-bound jobs** (HTTP calls, database queries, file operations):
```bash
# Start with 2-4x CPU cores
RATCHET_THREAD_POOL_SIZE_SINGLE=32
RATCHET_THREAD_POOL_SIZE_BATCH_CHILD=64
```

**For mixed workloads**, start with 2x CPU cores and adjust based on monitoring.

### Virtual threads (Java 21+)

```bash
RATCHET_WORKER_DEFAULT_THREADING_MODE=virtual
RATCHET_WORKER_VIRTUAL_EXECUTOR_JNDI=java:app/concurrent/MyVirtualExecutor
```

Virtual threads (Project Loom) eliminate the need to size thread pools for I/O-bound workloads. With the default threading mode set to virtual and a virtual-backed managed executor wired in, Ratchet routes jobs to the virtual pool, and the JVM multiplexes them across platform threads.

Benefits:
- No thread pool sizing needed (virtual threads are cheap to create)
- Blocking I/O no longer wastes platform threads
- Scales to thousands of concurrent jobs without tuning

Considerations:
- Requires Java 21 or later
- CPU-bound jobs do not benefit (still limited by platform thread count)
- `synchronized` blocks can pin virtual threads; prefer `ReentrantLock` in job code

### Custom `ExecutorProvider`

For full control over the thread pool, implement the `ExecutorProvider` SPI:

```java
@ApplicationScoped
public class CustomExecutorProvider implements ExecutorProvider {

  @Override
  public ExecutorService getJobExecutor() {
    return new ThreadPoolExecutor(
        8,                          // core pool size
        32,                         // max pool size
        60, TimeUnit.SECONDS,       // keep-alive
        new LinkedBlockingQueue<>(1000),  // work queue
        new ThreadPoolExecutor.CallerRunsPolicy()
    );
  }

  @Override
  public ScheduledExecutorService getScheduledExecutor() {
    return Executors.newScheduledThreadPool(2);
  }
}
```

## Permit-based backpressure

Ratchet supports resource-level concurrency control through the `scheduler_resource_limit` and `scheduler_resource_permit` tables. This limits how many jobs using a specific resource can execute simultaneously.

### Configure resource limits

Set each limit with `JobSchedulerService.setResourceLimit`. A job or batch that names a resource with no limit fails at `submit()` with `ResourceNotConfiguredException`, so set limits at start-up, before any code submits jobs. A `SchedulerLifecycleHook` is a good place. Its `beforeStart` runs after schema migration and before the poller starts. In Spring Boot and Micronaut, register the hook as a bean.

```java
@ApplicationScoped
public class ResourceLimitSetup implements SchedulerLifecycleHook {

  @Inject JobSchedulerService scheduler;

  @Override
  public void beforeStart() {
    scheduler.setResourceLimit(
        ResourceLimit.of("external-api", 5).withDescription("Rate-limited external API"));
    scheduler.setResourceLimit(
        ResourceLimit.of("report-generator", 2)
            .withRetryDelay(Duration.ofSeconds(10))
            .withDescription("Memory-intensive report generation"));
  }
}
```

- `setResourceLimit` creates the limit, or replaces every value of an existing one. Every node can set the same limits at start-up.
- A name is trimmed and must be 1 to 100 printable ASCII characters. The limit must be at least 1. The retry delay defaults to 5 seconds and is stored in whole milliseconds. `withResource` applies the same name rule.
- Lowering a limit does not stop running jobs. They keep their permits, and new jobs wait until enough permits are released. Raising a limit wakes waiting jobs straight away.
- On SQL stores the call joins the caller's transaction and locks the limit row until commit, which holds up permit requests for that resource. On MongoDB the change applies at once, and a rollback does not undo it.
- `findResourceLimit(name)` and `listResourceLimits()` read limits back. On a store without the `ResourcePermitStore` capability, `setResourceLimit` throws `UnsupportedOperationException` and the reads return nothing.
- A hook exception is logged and does not stop start-up. If a limit fails to save there, the first submit that names the resource fails with `ResourceNotConfiguredException`.
- MySQL, and SQL Server with a case-insensitive collation, treat `GPU` and `gpu` as one resource. PostgreSQL, Oracle and MongoDB do not.

If a limit is deleted with SQL while jobs still name it, each of those jobs fails when it is next claimed. It uses no retry and goes to the dead-letter queue with `ResourceNotConfiguredException`. Set the limit again, then re-run the job with `retryJob`.

### Assign resources to jobs

```java
scheduler.enqueue(() -> callExternalApi())
    .withResource("external-api")
    .submit();
```

When all permits for a resource are in use, jobs that need it wait in line in `scheduler_resource_waiter`. A released permit wakes the next waiter at once and sends the cluster wake-up hint. `retry_delay_ms` sets how often a waiting job re-checks when no wake-up reaches it. It also sets how long the job keeps its place in the line: `retry_delay_ms` plus the larger of `retry_delay_ms` and 10 seconds. A short delay means more re-checks. With a long one, a waiter whose wake-up was lost waits longer, up to the next orphan-recovery sweep. See [Resource Permit](../concepts/execution-model.md#step-details) for the order waiters are served in and its limits.

### Monitoring permits

```sql
-- Active permits per resource
SELECT resource_name, COUNT(*) AS active_permits
FROM scheduler_resource_permit
GROUP BY resource_name;

-- Available capacity per resource
SELECT
  rl.resource_name,
  rl.max_concurrent,
  COUNT(rp.id) AS active,
  rl.max_concurrent - COUNT(rp.id) AS available
FROM scheduler_resource_limit rl
LEFT JOIN scheduler_resource_permit rp ON rl.resource_name = rp.resource_name
GROUP BY rl.resource_name, rl.max_concurrent;

-- Jobs waiting per resource
SELECT resource_name, COUNT(*) AS waiting
FROM scheduler_resource_waiter
GROUP BY resource_name;
```

A waiter row can outlive its job for a short time. The engine skips a row whose job was canceled or has finished, and the sweep removes it.

## Timeout configuration

### Job timeout

```java
scheduler.enqueue(() -> longRunningTask())
    .withTimeout(Duration.ofMinutes(30))
    .submit();
```

Jobs without their own timeout (`timeout_sec = 0`) use `ratchet.timeout.default-sla-seconds` (1800 seconds by default). In production, set a timeout that fits each job so stuck jobs do not block the thread pool. Long jobs that work in units can stop cleanly before the timeout; see [Stopping at a safe point](../concepts/execution-model.md#stopping-at-a-safe-point).

Recommended timeouts by job type:

| Job Type | Recommended Timeout |
|----------|-------------------|
| Quick tasks (email, notification) | 30 seconds - 2 minutes |
| API calls | 1 - 5 minutes |
| Data processing | 5 - 30 minutes |
| Report generation | 30 - 120 minutes |
| Batch parents | Sum of child timeouts + overhead |

## Database optimization

### PostgreSQL tuning

**Shared buffers:** set to 25% of available RAM:
```sql
ALTER SYSTEM SET shared_buffers = '4GB';
```

**Work memory:** increase for complex queries:
```sql
ALTER SYSTEM SET work_mem = '256MB';
```

**Effective cache size:** set to 75% of available RAM:
```sql
ALTER SYSTEM SET effective_cache_size = '12GB';
```

**Autovacuum:** Ratchet performs frequent updates and deletes on the hot queue table. Tune autovacuum to keep up:
```sql
ALTER TABLE scheduler_job_queue SET (
  autovacuum_vacuum_scale_factor = 0.05,  -- vacuum after 5% of rows change
  autovacuum_analyze_scale_factor = 0.02  -- analyze after 2% of rows change
);
```

**Connection pooling:** use PgBouncer in transaction mode:
```ini
[pgbouncer]
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
```

### MySQL tuning

**InnoDB buffer pool:** set to 70-80% of available RAM:
```ini
[mysqld]
innodb_buffer_pool_size = 8G
innodb_buffer_pool_instances = 8
```

**Log file size:** larger redo logs improve write performance:
```ini
[mysqld]
innodb_log_file_size = 1G
```

**Isolation level:** required for Ratchet:
```ini
[mysqld]
transaction_isolation = READ-COMMITTED
```

### Index verification

Verify that the polling query uses indexes:

```sql
-- PostgreSQL
EXPLAIN ANALYZE
SELECT * FROM scheduler_job_queue
WHERE status = 'PENDING'
  AND scheduled_time <= statement_timestamp()
  AND job_type = 'SINGLE'
ORDER BY (priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - scheduled_time))) / (60.0 * 15))) DESC,
         scheduled_time ASC,
         job_id ASC
LIMIT 100
FOR UPDATE SKIP LOCKED;

-- MySQL
EXPLAIN
SELECT * FROM scheduler_job_queue FORCE INDEX (idx_claim_executable)
WHERE status = 'PENDING'
  AND job_type = 'SINGLE'
  AND scheduled_time <= NOW()
ORDER BY (priority + FLOOR(GREATEST(0, TIMESTAMPDIFF(MINUTE, scheduled_time, NOW(3))) / 15)) DESC,
         scheduled_time ASC,
         job_id ASC
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

The query should use `idx_claim_executable` on `scheduler_job_queue` on both PostgreSQL and MySQL: it is the hot-path claim index. On PostgreSQL it is partial on `status = 'PENDING'`; on MySQL it is a plain index with `status` as the leading column (MySQL has no partial indexes). A sort on computed effective priority is expected; a full scan of the pending queue is not. If you see a sequential scan, check that statistics are up to date:

```sql
-- PostgreSQL
ANALYZE scheduler_job_queue;

-- MySQL
ANALYZE TABLE scheduler_job_queue;
```

## Job retention and archiving

Unbounded table growth degrades polling performance. Configure retention to keep the active job table small:

```bash
# Auto-delete completed jobs after 14 days
RATCHET_JOB_RETENTION_DAYS=14

# Purge DLQ jobs after 90 days
RATCHET_DLQ_PURGE_DAYS=90
```

The `scheduler_job_archive` table stores historical data for completed and failed jobs. It has its own indexes for reporting queries, separate from the active job table's performance-critical indexes.

### Manual cleanup

```sql
-- PostgreSQL: archive old completed jobs. Terminated jobs are cold rows on
-- scheduler_job (the hot scheduler_job_queue row is deleted at the terminal
-- transition), so filter on terminal_status / terminated_at.
INSERT INTO scheduler_job_archive (archive_id, original_job_id, final_status, ...)
SELECT ...
FROM scheduler_job
WHERE terminal_status IN ('SUCCEEDED', 'FAILED', 'CANCELED')
  AND terminated_at < NOW() - INTERVAL '30 days';

-- Then delete the cold rows
DELETE FROM scheduler_job
WHERE terminal_status IN ('SUCCEEDED', 'FAILED', 'CANCELED')
  AND terminated_at < NOW() - INTERVAL '30 days';
```

## Monitoring performance

### Key metrics to watch

| Metric | What It Tells You | Action If High |
|--------|------------------|----------------|
| Poll query duration | Database under load | Add indexes, increase shared_buffers |
| Queue depth (pending jobs) | Jobs accumulating faster than processing | Increase threads, add nodes |
| Job execution duration (p95) | Slow jobs blocking the pool | Set timeouts, investigate slow jobs |
| DLQ growth rate | Rising failure rate | Check error logs, fix root cause |
| Thread pool utilization | Threads saturated | Increase threads or enable virtual threads |

### Micrometer integration

Add the `ratchet-micrometer` module instead of maintaining a lifecycle-only collector:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-micrometer</artifactId>
  <version>0.5.0</version>
</dependency>
```

The bundled adapter covers the full `MetricsCollector` surface, including store-finalization fallbacks, claim and application circuit-breaker state, and encryption rollout signals. See [Monitoring](./monitoring.md#published-metrics) for the meter catalog and setup.

### Useful dashboard queries

```sql
-- Average poll time (should be < 50ms)
SELECT AVG(duration_ms) FROM scheduler_job_execution
WHERE started_at > NOW() - INTERVAL '1 hour';

-- Jobs processed per minute. SUCCEEDED is terminal: status survives as
-- terminal_status on the cold scheduler_job row, alongside execution_end_time.
SELECT
  date_trunc('minute', execution_end_time) AS minute,
  COUNT(*) AS completed
FROM scheduler_job
WHERE terminal_status = 'SUCCEEDED'
  AND execution_end_time > NOW() - INTERVAL '1 hour'
GROUP BY minute
ORDER BY minute;

-- Thread pool pressure (jobs waiting for execution). PENDING is live state on
-- scheduler_job_queue.
SELECT COUNT(*) AS queued_jobs
FROM scheduler_job_queue
WHERE status = 'PENDING'
  AND scheduled_time <= NOW();
```

## Tuning checklist

1. **Start with defaults:** Ratchet's defaults work well for most workloads
2. **Measure first:** enable metrics before changing anything
3. **Tune one parameter at a time:** change one setting, measure the impact, then move on
4. **Watch the database:** most performance issues are database-related (missing indexes, insufficient memory, too many connections)
5. **Set timeouts:** every job should have a timeout to prevent resource leaks
6. **Configure retention:** keep the active job table small for fast polling
7. **Scale horizontally:** add nodes before over-tuning a single instance

## See also

- [Configuration](/deployment/configuration) -- Full configuration reference
- [Monitoring & Observability](/deployment/monitoring) -- Metrics and alerting
- [Cluster Configuration](/deployment/cluster-configuration) -- Multi-node tuning
- [Troubleshooting](/deployment/troubleshooting) -- Diagnosing performance issues

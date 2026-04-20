---
sidebar_position: 4
title: FAQ
description: Frequently asked questions about Ratchet job scheduling, deployment, retry behavior, and compatibility.
---

# Frequently Asked Questions

## Can I Use Ratchet Without CDI?

Ratchet is designed as a CDI-first library. The reference implementation (`ratchet`) uses CDI for dependency injection, bean resolution, and event publishing. However, the architecture separates concerns through SPI interfaces:

- `BeanResolver` -- abstracts how job target beans are obtained
- `ExecutorProvider` -- abstracts thread pool management
- `ClassPolicy`, `RetryPolicy`, `ResilienceStrategy` -- all are pluggable interfaces

In theory, you could wire these manually without CDI, but you would need to:
1. Construct all beans and their dependency graphs yourself
2. Provide a `BeanResolver` implementation that resolves beans without CDI
3. Replace the CDI event bridge with your own event dispatch

This is not a supported configuration. If you need a non-CDI scheduler, consider whether Ratchet is the right fit. The library is purpose-built for Jakarta EE environments with CDI.

## What Databases Are Supported?

Ratchet ships with two store modules:

| Module | Database | Minimum Version |
|---|---|---|
| `ratchet-store-mysql` | MySQL | 8.0+ |
| `ratchet-store-postgresql` | PostgreSQL | 12+ |

Both modules provide JPA entities and DDL scripts (`src/main/resources/ddl/`). The DDL is shipped as plain SQL files -- you apply them however you manage your schema (Flyway, Liquibase, manual scripts, etc.).

**Adding a new database:** Implement the `JobStore` SPI (which is a composite of 16 sub-interfaces like `JobClaimStore`, `JobCrudStore`, `JobStatusStore`, etc.) and provide the corresponding DDL. The `ratchet-tck` module contains a Technology Compatibility Kit that validates any store implementation against the expected behavior.

## How Does Retry Work?

When a job fails, Ratchet follows this decision path:

1. **Check for `@DoNotRetry`:** If the exception class (or any class in its hierarchy) is annotated with `@DoNotRetry`, the job skips all retry logic and moves directly to the DLQ.

2. **Consult the RetryPolicy SPI:** The `RetryPolicy.shouldRetry(attempt, cause)` method is called. The default implementation (`DefaultRetryPolicy`) always returns `true`, deferring to the attempt limit.

3. **Check attempt count:** If `attempts <= maxRetries` and the RetryPolicy says yes, the job is rescheduled for retry.

4. **Calculate backoff delay:** First, `RetryPolicy.getDelay(attempt)` is consulted. If it returns `Duration.ZERO`, the job-level backoff configuration is used instead:
   - **NONE:** Immediate retry (0ms delay)
   - **FIXED:** Constant delay equal to `backoffParamMs`
   - **EXPONENTIAL:** `backoffParamMs * 2^(attempt-1)`, capped at 24 hours

5. **Move to DLQ:** If retries are exhausted or the RetryPolicy says no, the job is marked FAILED and moved to the dead letter queue.

**Example configuration:**

```java
scheduler.enqueue(paymentService::processRefund)
    .withMaxRetries(3)
    .withBackoffPolicy(BackoffPolicy.EXPONENTIAL)
    .withBackoffParam(1000)  // 1 second base
    .submit();
// Retry delays: 1s, 2s, 4s (then DLQ)
```

**Manual retry from the DLQ:**

```java
// Reset a failed job back to PENDING with attempt counter cleared
scheduler.retryJob(failedJobId);
```

This sets the job status to PENDING, clears the error, resets attempts to 0, and sets `scheduled_time` to now.

## Can Jobs Be Distributed Across Nodes?

Yes. Ratchet is designed for multi-node deployment. Each node runs its own poller that atomically claims jobs from the shared database using optimistic locking (`claimNextBatchOptimized`). This ensures:

- **No duplicate execution:** A job is claimed by exactly one node via an atomic compare-and-swap on the `status` and `picked_by` columns.
- **Automatic failover:** If a node crashes, the `OrphanRecoveryTimer` on surviving nodes detects stale heartbeats and resets orphaned RUNNING jobs back to PENDING.
- **Node identity:** Each node registers in `scheduler_node` with a unique ID and periodic heartbeat (default every 10 seconds).

There is no built-in job routing or affinity -- any node can execute any job. If you need affinity (e.g., "only node X should run this type of job"), implement a custom `ClusterCoordinator` SPI.

**Key multi-node settings:**

| Variable | Default | Purpose |
|---|---|---|
| `RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS` | `10` | How often nodes write their heartbeat |
| `RATCHET_NODE_ORPHAN_GRACE_SECONDS` | `60` | Time before a silent node's jobs are recovered |
| `RATCHET_ORPHAN_SCAN_INTERVAL_MINUTES` | `5` | How often to scan for orphaned jobs |

## What Happens If the Server Crashes Mid-Job?

When a node crashes while jobs are RUNNING:

1. **The heartbeat stops.** The crashed node's `heartbeat_ts` in `scheduler_node` goes stale.

2. **Orphan recovery kicks in.** Surviving nodes run the `OrphanRecoveryTimer` (default every 5 minutes). It finds RUNNING jobs assigned to nodes whose heartbeat is older than the grace period (default 60 seconds).

3. **Jobs are reset to PENDING.** The orphaned jobs are atomically moved from RUNNING back to PENDING, making them eligible for re-claim by any healthy node.

4. **Resource permits are cleaned up.** Any permits held by the dead node in `scheduler_resource_permit` are released.

5. **The stale node entry is deleted.** The `scheduler_node` row for the crashed node is removed.

**Important:** The recovered job starts from scratch -- it does not resume from where it left off. If your job performs work that is not idempotent, you should design it to check for partial completion before proceeding.

**Worst-case recovery time:** `orphan_scan_interval + orphan_grace_seconds`. With defaults, this is 5 minutes + 60 seconds = ~6 minutes.

## How Do I Migrate from Quartz?

Ratchet and Quartz have fundamentally different architectures. Quartz uses trigger-based scheduling with XML or programmatic job definitions. Ratchet uses lambda serialization and a pull-based poller.

**Key differences to address:**

| Quartz Concept | Ratchet Equivalent |
|---|---|
| `Job` interface + `execute(JobExecutionContext)` | Any CDI bean with public methods |
| `@DisallowConcurrentExecution` | Resource permits (`withResource()`) |
| `CronTrigger` | `recurring()` or `@Recurring` annotation |
| `SimpleTrigger` with repeat | `recurring()` with cron expression |
| `JobDataMap` | `JobBuilder.withParam()` / `JobContext.param()` |
| `JobStore` (RAM/JDBC) | `ratchet-store-mysql` or `ratchet-store-postgresql` |
| Clustering via database locks | Built-in atomic claim with optimistic locking |
| `@PersistJobDataAfterExecution` | Job results stored in `job_result` column |

**Migration steps:**

1. Replace `Job` implementations with CDI beans containing public methods
2. Replace `CronTrigger` definitions with `@Recurring` annotations or `recurring()` calls
3. Replace `JobDataMap` usage with `withParam()` at submission and `JobContext.param()` at execution
4. Replace Quartz tables with Ratchet DDL (the schemas are incompatible)
5. Configure your `ClassPolicy` to allow your application packages

## How Do I Migrate from jBeret?

jBeret implements the JSR 352 (Java Batch) specification with XML-based job definitions. Ratchet takes a code-first approach.

| jBeret / JSR 352 Concept | Ratchet Equivalent |
|---|---|
| `job.xml` with steps | Code-based chains and workflows |
| `ItemReader` / `ItemProcessor` / `ItemWriter` | Streaming batches (`streamingBatch()`) |
| `@BatchProperty` | `JobBuilder.withParam()` |
| `JobOperator.start()` | `scheduler.enqueue()` |
| Partition mapping | Batch builder with parallel children |

**Note:** jBeret and Ratchet solve overlapping but different problems. jBeret focuses on ETL-style chunk processing with reader/processor/writer pipelines. Ratchet focuses on task scheduling with retry, circuit breaking, and workflow orchestration. For pure ETL workloads, jBeret may still be the better fit.

## What Is the Overhead of the Circuit Breaker?

The built-in circuit breaker is lightweight by design:

- **CLOSED state (normal operation):** One lock acquisition per call to update the sliding window ring buffer. This is a `ReentrantLock` held for microseconds -- negligible compared to any real job execution.
- **OPEN state:** A single `System.currentTimeMillis()` comparison. No lock needed for the fast path.
- **Memory:** One `int[]` array per circuit breaker instance (size = window size, default 100). Each unique service name gets its own instance.

The circuit breaker is resolved per job by inspecting the `@CircuitBreakerProtected` annotation on the target method or class. Service names are cached after first resolution, so reflection only happens once.

**Disabling it:** If you do not need circuit breaking, set `RATCHET_CIRCUIT_BREAKER_ENABLED=false`. This replaces the circuit breaker with a passthrough that adds zero overhead.

## Can I Use Virtual Threads?

Yes. Set `RATCHET_WORKER_USE_VIRTUAL_THREADS=true` to switch from platform thread pools to virtual threads.

When enabled:
- The `ThreadPoolManager` creates virtual threads via `Thread.ofVirtual()` instead of using `ExecutorService` thread pools
- Semaphore-based concurrency limits are replaced with `AtomicInteger` counters
- Each job type still has a configurable concurrency limit (default 1000) to prevent unbounded growth

**Requirements:**
- Java 21+ (virtual threads are a preview feature in Java 19-20 and GA in 21)
- Your jobs must not perform long-duration `synchronized` blocks or call native methods that pin the carrier thread

**Configuration:**

```bash
export RATCHET_WORKER_USE_VIRTUAL_THREADS=true
# Optional: adjust per-type limits (default 1000)
export RATCHET_VIRTUAL_THREAD_LIMIT_SINGLE=500
export RATCHET_VIRTUAL_THREAD_LIMIT_BATCH_CHILD=2000
```

**When to use virtual threads:** They are most beneficial when your jobs spend the majority of their time waiting on I/O (database queries, HTTP calls, file operations). For CPU-bound workloads, platform threads with appropriate pool sizes are usually sufficient.

## How Does Job Priority Work?

Ratchet supports five priority levels:

| Priority | Enum Value | Numeric Value |
|---|---|---|
| `LOWEST` | `JobPriority.LOWEST` | 0 |
| `LOW` | `JobPriority.LOW` | 1 |
| `NORMAL` | `JobPriority.NORMAL` | 2 (default) |
| `HIGH` | `JobPriority.HIGH` | 3 |
| `CRITICAL` | `JobPriority.CRITICAL` | 4 |

The poller claims jobs ordered by effective priority descending, then due time. Effective priority starts with the numeric priority and adds `floor(wait_minutes / RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES)`.

With the default 15-minute interval, a long-waiting low-priority job can overtake newer high-priority work. This boost is computed during claim ordering; it does not rewrite the stored priority. Set `RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES=0` to disable boosting.

## How Are Job Results Stored?

When a job method returns a non-null value, Ratchet serializes it to JSON and stores it:

```sql
SELECT job_result, result_type
FROM scheduler_job
WHERE job_id = 12345;
```

- `job_result` contains the JSON representation
- `result_type` contains the fully qualified class name of the return value

If serialization fails (e.g., the return type is not JSON-serializable), a warning is logged but the job still counts as succeeded. The result is best-effort metadata, not a critical path.

## What Is the Maximum Payload Size?

The default maximum payload size is 100 KB, controlled by `RATCHET_MAX_PAYLOAD_KB`. The payload includes the serialized lambda descriptor (target class, method name, method descriptor, arguments).

If you need to pass large data to a job, pass a reference (e.g., a database ID or S3 key) rather than the data itself:

```java
// Do this
scheduler.enqueue(() -> importService.processFile(fileId));

// Don't do this
scheduler.enqueue(() -> importService.processData(hugeByteArray));
```

## How Does Job Archiving Work?

Completed jobs are automatically archived based on retention settings:

| Variable | Default | Purpose |
|---|---|---|
| `RATCHET_JOB_ARCHIVE_ENABLED` | `true` | Enable/disable archiving |
| `RATCHET_JOB_RETENTION_DAYS` | `90` | Days before completed jobs are archived |
| `RATCHET_JOB_ARCHIVER_CRON` | `0 0 1 * * ?` | When the archiver runs (1 AM daily) |
| `RATCHET_JOB_ARCHIVE_BATCH_SIZE` | `1000` | Jobs archived per run |

The archiver moves jobs from `scheduler_job` to `scheduler_job_archive`, preserving all metadata. This keeps the active job table small for poller performance while retaining history for auditing.

Job logs have separate retention controlled by `RATCHET_LOG_RETENTION_DAYS` (default 30 days).

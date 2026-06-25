---
sidebar_position: 4
title: FAQ
description: Frequently asked questions about Ratchet job scheduling, deployment, retry behavior, and compatibility.
---

# Frequently Asked Questions

## Can I use Ratchet without CDI?

Ratchet is designed as a CDI-first library. The reference implementation (`ratchet`) uses CDI for dependency injection, bean resolution, and event publishing. However, the architecture separates concerns through SPI interfaces:

- `BeanResolver`: abstracts how job target beans are obtained
- `ExecutorProvider`: abstracts thread pool management
- `ClassPolicy`, `RetryPolicy`, `ResilienceStrategy`: all are pluggable interfaces

You could wire these manually without CDI, but you would need to:
1. Construct all beans and their dependency graphs yourself
2. Provide a `BeanResolver` implementation that resolves beans without CDI
3. Replace the CDI event bridge with your own event dispatch

This is not a supported configuration. If you need a non-CDI scheduler, consider whether Ratchet is the right fit. The library is purpose-built for Jakarta EE environments with CDI.

## What databases are supported?

Ratchet ships with four store modules:

| Module | Database | Minimum Version |
|---|---|---|
| `ratchet-store-mysql` | MySQL | 8.0+ |
| `ratchet-store-postgresql` | PostgreSQL | 14+ |
| `ratchet-store-oracle` | Oracle | 23ai+ |
| `ratchet-store-mongodb` | MongoDB | 6.0+ |

The SQL modules provide DDL scripts (`src/main/resources/ddl/`) as plain SQL files. Apply them however you manage your schema (Flyway, Liquibase, manual scripts, etc.). The MongoDB module initializes its collections and indexes at startup.

**Adding a new database:** Implement the composed `JobStore` SPI and provide the corresponding schema/bootstrap logic for your backend. The `ratchet-tck-store` submodule contains contract tests that validate any store implementation against the expected behavior; passing them earns the "Ratchet Store Compatible" label.

## How does retry work?

When a job fails, Ratchet follows this decision path:

1. **Check for `@DoNotRetry`:** If the exception class (or any exception in its cause chain) is annotated with `@DoNotRetry`, the job skips all retry logic and moves directly to the DLQ.

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
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(1))  // 1 second base
    .submit();
// Retry delays: 1s, 2s, 4s (then DLQ)
```

**Manual retry from the DLQ:**

```java
// Reset a failed job back to PENDING with attempt counter cleared
scheduler.retryJob(failedJobId);
```

This sets the job status to PENDING, clears the error, resets attempts to 0, and sets `scheduled_time` to now.

## Can jobs be distributed across nodes?

Yes. Ratchet supports multi-node deployment. Each node runs its own poller that atomically claims jobs from the shared database using optimistic locking (`claimNextBatchOptimized`). This ensures:

- **No duplicate execution:** A job is claimed by exactly one node via an atomic compare-and-swap on the `status` and `picked_by` columns.
- **Automatic failover:** If a node crashes, the `OrphanRecoveryTimer` on surviving nodes detects stale heartbeats and resets orphaned RUNNING jobs to PENDING.
- **Node identity:** Each node registers in `scheduler_node` with a unique ID and periodic heartbeat (default every 10 seconds).

Ratchet supports worker tag affinity: tag jobs with `withTags(...)` and constrain which jobs a node claims by providing a `NodeTagAffinityProvider` (the default is `DefaultNodeTagAffinityProvider`, consumed by the poller). For fully custom routing or cross-node wakeups, implement the `ClusterCoordinator` SPI.

**Multi-node settings:**

| Variable | Default | Purpose |
|---|---|---|
| `RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS` | `10` | How often nodes write their heartbeat |
| `RATCHET_NODE_ORPHAN_GRACE_SECONDS` | `60` | Time before a silent node's jobs are recovered |
| `RATCHET_NODE_ORPHAN_SCAN_INTERVAL_MINUTES` | `5` | How often to scan for orphaned jobs |

## What happens if the server crashes mid-job?

When a node crashes while jobs are RUNNING:

1. **The heartbeat stops.** The crashed node's `heartbeat_ts` in `scheduler_node` goes stale.

2. **Orphan recovery kicks in.** Surviving nodes run the `OrphanRecoveryTimer` (default every 5 minutes). It finds RUNNING jobs assigned to nodes whose heartbeat is older than the grace period (default 60 seconds).

3. **Jobs are reset to PENDING.** The orphaned jobs are atomically moved from RUNNING back to PENDING, making them eligible for re-claim by any healthy node.

4. **Resource permits are cleaned up.** Any permits held by the dead node in `scheduler_resource_permit` are released.

5. **The stale node entry is deleted.** The `scheduler_node` row for the crashed node is removed.

**Important:** The recovered job starts from scratch and does not resume from where it left off. If your job performs work that is not idempotent, design it to check for partial completion before proceeding.

**Worst-case recovery time:** `orphan_scan_interval + orphan_grace_seconds`. With defaults, this is 5 minutes + 60 seconds = ~6 minutes.

## How do I migrate from Quartz?

Ratchet and Quartz have different architectures. Quartz uses trigger-based scheduling with XML or programmatic job definitions. Ratchet uses lambda serialization and a pull-based poller.

**Differences to address:**

| Quartz Concept | Ratchet Equivalent |
|---|---|
| `Job` interface + `execute(JobExecutionContext)` | Any CDI bean with public methods |
| `@DisallowConcurrentExecution` | Resource permits (`withResource()`) |
| `CronTrigger` | `scheduleRecurring()` or `@Recurring` annotation |
| `SimpleTrigger` with repeat | `scheduleRecurring()` with cron expression |
| `JobDataMap` | `JobBuilder.withParam()` / `JobContext.param()` |
| `JobStore` (RAM/JDBC) | `ratchet-store-mysql`, `ratchet-store-postgresql`, `ratchet-store-oracle`, or `ratchet-store-mongodb` |
| Clustering via database locks | Built-in atomic claim with optimistic locking |
| `@PersistJobDataAfterExecution` | Job results stored in `job_result` column |

**Migration steps:**

1. Replace `Job` implementations with CDI beans containing public methods
2. Replace `CronTrigger` definitions with `@Recurring` annotations or `scheduleRecurring()` calls
3. Replace `JobDataMap` usage with `withParam()` at submission and `JobContext.param()` at execution
4. Replace Quartz tables with Ratchet DDL (the schemas are incompatible)
5. Configure your `ClassPolicy` to allow your application packages

## How do I migrate from jBeret?

jBeret implements the JSR 352 (Java Batch) specification with XML-based job definitions. Ratchet takes a code-first approach.

| jBeret / JSR 352 Concept | Ratchet Equivalent |
|---|---|
| `job.xml` with steps | Code-based chains and workflows |
| `ItemReader` / `ItemProcessor` / `ItemWriter` | Streaming batches (`streamingBatch()`) |
| `@BatchProperty` | `JobBuilder.withParam()` |
| `JobOperator.start()` | `scheduler.enqueue()` |
| Partition mapping | Batch builder with parallel children |

**Note:** jBeret and Ratchet solve overlapping but different problems. jBeret focuses on ETL-style chunk processing with reader/processor/writer pipelines. Ratchet focuses on task scheduling with retry, circuit breaking, and workflow orchestration. For pure ETL workloads, jBeret may still be the better fit.

## What is the overhead of the circuit breaker?

The built-in circuit breaker adds minimal overhead:

- **CLOSED state (normal operation):** One lock acquisition per call to update the sliding window ring buffer. This is a `ReentrantLock` held for microseconds, negligible against any real job execution cost.
- **OPEN state:** A single `System.currentTimeMillis()` comparison, with no lock on the fast path.
- **Memory:** One `int[]` array per circuit breaker instance (size = window size, default 100). Each unique service name gets its own instance.

The circuit breaker is resolved per job by inspecting the `@CircuitBreakerProtected` annotation on the target method or class. Service names are cached after first resolution, so reflection runs only once.

**Disabling it:** To skip circuit breaking entirely, set `RatchetOptions.builder().circuitBreaker(cb -> cb.enabled(false))`. This replaces the circuit breaker with a passthrough that adds zero overhead.

## Can I use virtual threads?

Yes, on a Jakarta EE 11 container. Virtual-thread support has two parts:

1. **Where jobs run.** Ratchet runs each job on the `ManagedExecutorService` resolved from `ratchet.worker.job-executor-jndi` (default `java:comp/DefaultManagedExecutorService`). Point that at a virtual-thread-backed managed executor and jobs run on virtual threads, with the container's context propagation (CDI, transaction, security) intact. A hand-rolled `Thread.ofVirtual()` executor would lose that context.
2. **Backpressure accounting.** `execution.virtualCounterAccounting(true)` swaps the semaphore-based concurrency limits for `AtomicInteger` counters, since virtual threads are cheap and a fixed pool no longer bounds concurrency. Each job type keeps a configurable limit (default 1000) to prevent unbounded growth.

Ratchet does not ship its own executor definition (resource-definition scanning of library jars is container-specific, so a bundled definition would not bind portably). Declare one in your application on EE 11 (Jakarta Concurrency 3.1) and point Ratchet at it:

```java
@ManagedExecutorDefinition(name = "java:app/concurrent/MyVirtualExecutor", virtual = true)
@ApplicationScoped
public class VirtualExecutorConfig {}
```

```bash
export RATCHET_WORKER_DEFAULT_THREADING_MODE=virtual
export RATCHET_WORKER_JOB_EXECUTOR_JNDI=java:app/concurrent/MyVirtualExecutor
export RATCHET_WORKER_VIRTUAL_COUNTER_ACCOUNTING=true
# Optional: adjust per-type limits (default 1000)
export RATCHET_VIRTUAL_THREAD_LIMIT_SINGLE=500
export RATCHET_VIRTUAL_THREAD_LIMIT_BATCH_CHILD=2000
```

Or programmatically:

```java
RatchetOptions.builder()
    .execution(e -> e
        .defaultThreadingMode(RatchetOptions.ThreadingMode.VIRTUAL)
        .virtualCounterAccounting(true)
        .jobExecutorJndi("java:app/concurrent/MyVirtualExecutor"));
```

**Requirements:**
- A Jakarta EE 11 container whose Jakarta Concurrency 3.1 implementation honors `virtual = true`, on Java 21+. Verified on Eclipse GlassFish 8 (the EE 11 reference implementation). Note: WildFly 40.0.0.Final accepts the definition and runs jobs on the configured executor, but does not yet create virtual threads for managed executors; jobs run on platform threads there until a later release implements it.
- Jakarta EE 10 (Jakarta Concurrency 3.0) has no standard `virtual = true` attribute on `@ManagedExecutorDefinition`, so an application cannot portably declare a virtual-thread executor. `virtualCounterAccounting(true)` still switches the backpressure model, but jobs run on virtual threads only if you point the JNDI name at an executor the container itself configures as virtual through a vendor-specific mechanism.
- Your jobs must not hold long `synchronized` blocks or call native methods that pin the carrier thread. Prefer `ReentrantLock`.

**When to use virtual threads:** They are most useful when jobs spend most of their time waiting on I/O (database queries, HTTP calls, file operations). For CPU-bound workloads, platform threads with appropriate pool sizes are usually sufficient.

## How does job priority work?

Ratchet supports five priority levels:

| Priority | Enum Value | Numeric Value |
|---|---|---|
| `LOWEST` | `JobPriority.LOWEST` | 0 |
| `LOW` | `JobPriority.LOW` | 1 |
| `NORMAL` | `JobPriority.NORMAL` | 2 (default) |
| `HIGH` | `JobPriority.HIGH` | 3 |
| `CRITICAL` | `JobPriority.CRITICAL` | 4 |

The poller claims jobs ordered by effective priority descending, then due time. Effective priority starts with the numeric priority and adds `floor(wait_minutes / priorityBoostIntervalMinutes)`.

With the default 15-minute interval, a long-waiting low-priority job can overtake newer high-priority work. This boost is computed during claim ordering; it does not rewrite the stored priority. Set `RatchetOptions.builder().store(s -> s.priorityBoostIntervalMinutes(0))` to disable boosting.

## How are job results stored?

When a job method returns a non-null value, Ratchet serializes it to JSON and stores it:

```sql
SELECT job_result, result_type
FROM scheduler_job
WHERE job_id = '01902c4e-c4f3-7b8a-9d3e-fedcba987654';
```

- `job_result` contains the JSON representation
- `result_type` contains the fully qualified class name of the return value

If serialization fails (e.g., the return type is not JSON-serializable), a warning is logged but the job still counts as succeeded. The result is best-effort metadata, not a critical path.

## What is the maximum payload size?

The default maximum payload size is 100 KB, controlled by `RatchetOptions.builder().payload(p -> p.maxPayloadKb(...))`. The payload includes the serialized lambda descriptor (target class, method name, method descriptor, arguments).

If you need to pass large data to a job, pass a reference (e.g., a database ID or S3 key) rather than the data itself:

```java
// Do this
scheduler.enqueue(() -> importService.processFile(fileId));

// Don't do this
scheduler.enqueue(() -> importService.processData(hugeByteArray));
```

## How does job archiving work?

Completed jobs are automatically archived based on retention settings:

| Variable | Default | Purpose |
|---|---|---|
| `RATCHET_JOB_ARCHIVE_ENABLED` | `true` | Enable/disable archiving |
| `RATCHET_JOB_RETENTION_DAYS` | `90` | Days before completed jobs are archived |
| `RATCHET_JOB_ARCHIVE_CRON` | `0 0 1 * * ?` | When the archiver runs (1 AM daily) |
| `RATCHET_JOB_ARCHIVE_BATCH_SIZE` | `1000` | Jobs archived per run |

The archiver moves jobs from `scheduler_job` to `scheduler_job_archive`, preserving all metadata. This keeps the active job table small for poller performance while retaining history for auditing.

Job logs have separate retention controlled by `RATCHET_LOG_RETENTION_DAYS` (default 30 days).

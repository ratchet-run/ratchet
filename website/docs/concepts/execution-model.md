---
sidebar_position: 5
title: Execution Model
description: How jobs are polled, claimed, validated, executed, and post-processed
---

# Execution Model

This page explains how Ratchet moves a job from "sitting in the database" to "running on a worker thread" and back. The key components are the Poller, the JobExecutionCoordinator, and the JobTask.

## Execution Pipeline

<div class="docs-diagram" role="img" aria-label="Ratchet execution pipeline from the hot queue table through the Poller, coordinator, worker task, and post-processing.">
  <div class="docs-diagram-flow">
    <div class="docs-diagram-card docs-diagram-card--store">
      <strong>Hot Queue</strong>
      <small>`scheduler_job_queue` stores live PENDING/RUNNING/PAUSED/WAITING state.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--primary">
      <strong>Poller</strong>
      <small>Claims due PENDING rows with `FOR UPDATE SKIP LOCKED`.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--active">
      <strong>JobExecutionCoordinator</strong>
      <small>Dispatches lightweight claim DTOs to the configured executor.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--active">
      <strong>JobTask</strong>
      <small>Loads the full job, validates policy, executes, and records the outcome.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--muted">
      <strong>Post-processing</strong>
      <small>Triggers chains, workflow branches, batch updates, events, and archival paths.</small>
    </div>
  </div>
</div>

The flow for each poll cycle:

1. **Poll:** The Poller queries for eligible jobs using `SELECT ... FOR UPDATE SKIP LOCKED`
2. **Claim:** Matching jobs are atomically transitioned from PENDING to RUNNING
3. **Dispatch:** Each claimed job is submitted to the `JobExecutionCoordinator` thread pool
4. **Execute:** A `JobTask` validates, runs, and handles the result on a worker thread
5. **Post-process:** Based on outcome and job type, the engine triggers chains, workflows, or batch updates

## The Poller

The `Poller` is the heart of Ratchet's pull-based architecture. It periodically queries the database for jobs that are ready to execute.

### Claim Query

The core query selects PENDING jobs whose scheduled time has passed, ordered by effective priority
and due time:

```sql
SELECT job_id FROM scheduler_job_queue
WHERE status = 'PENDING'
  AND scheduled_time <= CURRENT_TIMESTAMP
ORDER BY (priority + age_boost) DESC, scheduled_time ASC
FOR UPDATE SKIP LOCKED
LIMIT :batchSize
```

Key properties of this query:

- **`FOR UPDATE SKIP LOCKED`** -- Two nodes running this query concurrently will claim disjoint sets of jobs. Neither blocks the other. This is the foundation of Ratchet's cluster-safe execution.
- **Priority ordering** -- Raw priority is the base priority.
- **Age boosting** -- `age_boost` is computed as `floor(wait_minutes / priorityBoostIntervalMinutes)`, so sufficiently old low-priority work can move ahead of newer high-priority work. Configure the interval with `RatchetOptions.store(...)`.
- **Age ordering** -- Within the same effective priority, older jobs are claimed first (fairness).
- **Batch claiming** -- Up to `batchSize` jobs are claimed in a single query to reduce database round trips.

After the query, claimed jobs are updated:

- `status` = RUNNING
- `picked_by` = current node ID
- `picked_at` = current timestamp

### Adaptive Polling

The Poller doesn't use a fixed interval. The `PollingStrategy` dynamically adjusts the delay between polls:

| Mode | Delay | Trigger |
|------|-------|---------|
| **Burst** | 500ms | Wakeup signal received |
| **Normal** | 2-30s | Steady-state operation |
| **Deep Idle** | 60s | No jobs found for 5+ minutes |

The algorithm considers:

- **Rolling window** of the last 10 poll results to identify trends
- **Full batch detection** -- when 3+ consecutive polls return a full batch (all `batchSize` slots filled), polling becomes very aggressive (delay / 4)
- **System load factor** -- thread pool utilization adjusts the delay. High utilization slows polling; low utilization allows faster polling
- **Idle count** -- consecutive empty polls trigger exponential backoff
- **Deep idle threshold** -- after 5 minutes without finding any jobs, the poller enters deep idle (60s delay)

When a wakeup signal arrives (from `ClusterCoordinator.notifyNewWork()` or a local job submission), the poller immediately exits deep idle and enters burst mode.

### Drain Mode

During CDI shutdown, an internal `DrainController` makes subsequent poll cycles skip new claims
before the Poller stops. The execution coordinator then requests cancellation of active work;
unfinished RUNNING rows remain durable and are handled by normal shutdown reset or orphan recovery.

`DrainController` is not a public health or lifecycle API. An application's readiness endpoint
must own its traffic-acceptance state rather than inject an RI implementation type. See the
[Kubernetes rolling-termination pattern](../deployment/kubernetes.md#rolling-termination).

## Job Execution Coordinator

The `JobExecutionCoordinator` manages the thread pools that execute jobs. When the Poller claims a batch of jobs, each job is submitted to the coordinator for asynchronous execution.

### Thread Pool Management

The `ThreadPoolManager` maintains one or more thread pools for job execution. The `ExecutorProvider` SPI controls how these pools are created:

- **Default (CDI):** Uses `@Resource`-based container-managed executors (e.g., WildFly's `ManagedExecutorService`)
- **Custom:** Implement `ExecutorProvider` to use virtual threads, custom pool sizes, or application-specific executors

The coordinator tracks pool utilization metrics, which feed back into the `PollingStrategy` to adjust polling aggressiveness.

### Lazy Entity Loading

For efficiency, the Poller passes lightweight `JobClaimDto` objects (containing only the job ID and essential metadata) to the coordinator rather than full `JobEntity` objects. The `JobTask` loads the full entity from the database only when execution begins. This avoids holding large entity graphs in memory between claim and execution.

## JobTask: The Worker

`JobTask` implements `Callable<Void>` and encapsulates the complete execution lifecycle for a single job. Each instance is dedicated to one job and discarded after completion.

### Execution Steps

<div class="docs-diagram docs-step-list" role="list" aria-label="JobTask execution steps">
  <div class="docs-diagram-step" role="listitem">
    <strong>Setup MDC context</strong>
    <small>Adds job ID and node ID so logs can be correlated.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Load `JobEntity`</strong>
    <small>Loads the full entity lazily from the lightweight `JobClaimDto`.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Bind `JobContext`</strong>
    <small>Attaches logger, params, and any delivered signal payload to the worker thread.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Check cancellation</strong>
    <small>Re-reads status and exits cleanly if the job was canceled after claim.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Check circuit breaker</strong>
    <small>Asks `ResilienceStrategy` whether the target service is available.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Acquire resource permit</strong>
    <small>Reschedules without consuming a retry if the configured resource is saturated.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Validate class policy</strong>
    <small>Applies `ClassPolicy` before resolving the target invocation.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Resolve bean and method</strong>
    <small>Uses CDI lookup and reflection from the persisted payload.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Execute target</strong>
    <small>Runs through `ResilienceStrategy`, including circuit-breaker wrapping.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Handle result</strong>
    <small>Routes success to `handleSuccess()` or failure to `handleFailure()`.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Release resource permit</strong>
    <small>Frees capacity for other queued work.</small>
  </div>
  <div class="docs-diagram-step" role="listitem">
    <strong>Clear MDC and context</strong>
    <small>Removes thread-local state before the worker returns to the pool.</small>
  </div>
</div>

### Step Details

**1-3. Context Setup**

The `JobMdcContext` sets up SLF4J/JUL MDC entries (job ID, node ID) so all log output during execution is automatically correlated. `JobContext.bind()` attaches the thread-local context that jobs can access via `JobContext.current()`.

**4. Cancellation Check**

Before doing any work, the task re-reads the job's status from the database. If the job was canceled between claim and execution, results are discarded and the execution ends cleanly.

**5. Circuit Breaker Gate**

The `ResilienceStrategy` SPI is consulted to check if the target service is available. If the circuit breaker is OPEN, the job is rescheduled with a delay (matching the circuit breaker's OPEN-to-HALF_OPEN transition window) without counting as a retry attempt.

**6. Resource Permit**

If the job specifies a `resourceName`, the `ResourcePermitService` attempts to acquire a permit. If the resource is at capacity, the job is rescheduled with a configurable delay. This is not a failure; no retry is consumed.

**7. Security Validation**

The `PreExecutionValidator` invokes the `ClassPolicy` SPI to verify the target class is allowed. The default `PackagePrefixClassPolicy` is deny-all until you provide an `@Alternative` bean with your application's package prefixes, and `RatchetProducer` fails deployment by default so this misconfiguration is caught at startup.

**8. Bean Resolution**

For instance methods, the `BeanResolver` SPI locates the CDI bean for the target class. For static methods, no bean is needed. Method resolution uses ASM descriptors to match the exact method signature, with results cached in a `ConcurrentHashMap`.

**9. Execution**

The actual method invocation happens through `ResilienceStrategy.execute()`, which wraps the call in circuit breaker protection. The method is invoked reflectively with the deserialized arguments.

**10. Result Handling**

On **success:**
- Execution timing is recorded
- Return value is serialized to JSON
- Status transitions RUNNING -> SUCCEEDED
- `JobCompletedEvent` is published
- `PostExecutionHandler.handleJobSuccess()` routes to batch/chain/workflow handlers

On **failure:**
- Attempt counter is incremented atomically
- `@DoNotRetry` is checked on the exception
- `RetryPolicy.shouldRetry()` is consulted
- Either schedules a retry or moves to DLQ

### Mid-Execution Cancellation

The executor checks `wasJobCanceledDuringExecution()` both before starting work and after the method returns. If the job was canceled during execution, the result is discarded and the job stays in CANCELED state. This handles the race condition where `cancelJob()` is called while a job is actively running.

## Transactions and Database Connections

`JobTask.call()` does **not** run inside a transaction. This is deliberate: holding a database connection open for the entire duration of a long-running job would exhaust connection pools.

Instead, each database operation (status update, retry scheduling, success marking) is a separate short transaction. The `@Transactional` annotation is on the individual service methods (`PostExecutionHandler`, `DeadLetterService`, etc.), not on `JobTask` itself.

## Timeout Handling

If a job exceeds its configured timeout, the `JobTimeoutHandler` interrupts the executing thread. The resulting `InterruptedException` flows through the normal failure path. The task logs a specific warning when it detects a timeout-caused interruption.

```java
scheduler.enqueue(() -> longRunningTask())
    .withTimeout(Duration.ofMinutes(5))
    .submit();
```

## Performance Characteristics

### Method Caching

`JobTask` maintains static `ConcurrentHashMap` caches for:
- **Class lookups** (`Class.forName` results)
- **Method resolution** (reflected `Method` objects)
- **Service name resolution** (circuit breaker service names)

These caches are cleared on application shutdown via `JobTask.clearCaches()` to prevent classloader leaks in redeployable containers.

### Heartbeat

The `DynamicHeartbeatCalculator` adjusts the heartbeat interval based on system load. Heartbeats update the node's `heartbeat_ts` value in the `scheduler_node` table, so orphan recovery can detect crashed nodes.

## Related

- [Job Lifecycle](./job-lifecycle.md) -- State transitions during execution
- [Scheduling](./scheduling.md) -- Adaptive polling details
- [Error Handling](./error-handling.md) -- What happens when execution fails
- [Clustering](./clustering.md) -- Multi-node polling coordination

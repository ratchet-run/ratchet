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

### Release retries

When an attempt ends, Ratchet releases what it held: its business-key hold, its resource permit and, when it gives the job back, its claim. A release that fails, or that starts on a thread that is still interrupted, is retried on a separate release pool sized by `ratchet.worker.release-threads` (default 2, minimum 1). The heartbeat and the timeout watchdog run on the scheduled executor, so a release that blocks in the store cannot delay them.

Retries run through one drain for each kind of write: business-key hold releases, resource permit releases, outcome writes and claim reschedules. Each drain runs its writes in the order they fall due. A tick handles at most 100 writes or about one second of work, then yields to other work queued on the pool. The budget is checked after each write, so it cannot cut short a store call that hangs. The first try of a hard-timeout outcome write and each state-flush drain go straight to the pool and never wait in a drain.

A second hand-off of a hold release, permit release or claim reschedule that is already pending is dropped, and the first keeps its retry timing and callbacks. Outcome writes are never merged. Each drain warns, at most once every ten minutes, when more than 1000 writes are pending or its oldest write is more than a minute past due. No write is dropped to save memory, because nothing frees a permit or ends a RUNNING job while this node runs. A hold release still gives up after about five minutes and leaves the hold to expire. When the node stops, pending writes are left to hold expiry, node clean-up, shutdown reset or orphan recovery, and the log says which.

A finished run that asks to continue gives its claim back through the same retries. A transient store failure during that release does not charge an attempt or rerun the body.

A store call that fails transiently while the worker charges an attempt, checks its batch or reschedules a retry does not fail the job.
The worker retries the rest of its failure handling until it lands.
It reuses a committed charge and keeps its retry decision and retry time.
It checks the batch again before each reschedule, and a batch that has closed ends the job FAILED.
The retrying event is published at most once and may be missing when a failed try had in fact committed.
The failure metric is recorded when the reschedule lands, when it misses without a cancel, or when the job ends FAILED.
It may be missing if the node stops before then.

The release pool also runs the store writes of a hard timeout. The watchdog claims the timeout and interrupts the worker on the scheduled executor. It then saves the job's state and writes the retry or final state on the release pool, so a timeout write that blocks in the store cannot delay the heartbeat or other jobs' timeouts. The job's `onFailure` callback after a hard timeout runs on that release thread. While the node stops, a timeout write that the release pool refuses is left to shutdown reset or orphan recovery. If the release pool is unavailable for another reason, the watchdog writes on the scheduled executor instead, so the job is not left RUNNING. A write that blocks there can delay the heartbeat.

Periodic checkpoint and progress flushes run on the release pool too, and only their timer stays on the scheduled executor. If the release pool refuses a flush, the state stays queued and Ratchet tries again later. It never flushes on the scheduled executor.

The release pool also retries an attempt's outcome write that a hard timeout or a failed failure write left RUNNING. This includes the watchdog's timeout transition and the forced FAILED write. It retries until the write lands or the node stops. A retry the release executor refuses while the node runs is resubmitted from the scheduled executor. This applies to outcome writes, permit releases and claim reschedules.

In Jakarta EE, Ratchet builds the release pool from the managed thread factory named by `ratchet.worker.thread-factory-jndi` (default `java:comp/DefaultManagedThreadFactory`). The container's default managed scheduled executor can be shared with other applications on the server. Ratchet keeps its own release retries off it, but other applications' work can still delay the timers that run there.

A custom `ExecutorProvider` must return a release executor that runs on threads separate from its scheduled executor's. The runtime refuses to start if both are the same instance, but it cannot tell when two different executors share the same threads. Size the release executor for this store work.

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
    <small>Puts the job in the resource's waiting line, without using a retry, if the resource is full.</small>
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

The `ResilienceStrategy` SPI is consulted to check if the target service is available. If the circuit breaker is OPEN, the job is rescheduled with a delay (matching the circuit breaker's OPEN-to-HALF_OPEN transition window) without counting as a retry attempt. The worker uses `JobRetryStore.releaseClaim`, which requires RUNNING status, the same claim sequence and the attempt count read with the claim. It never writes attempts, so a timeout charge committed under the same claim makes the release miss. The same release handles transient or superseded permits, envelope upgrades and failed batch checks; children of a paused batch remain PAUSED until resume.

**6. Resource Permit**

If the job names a resource, the `ResourcePermitService` asks the store for a permit. The job gets one when a slot is still free after the live waiters ahead of it are counted: with three free slots and one waiter ahead, it gets a permit. Otherwise the store parks the job in the same transaction. The job goes back to PENDING (PAUSED if its batch is paused), due again after the resource's `retry_delay_ms`, and it gets a row in `scheduler_resource_waiter`. Parking is not a failure and uses no retry. While the job waits, its `last_error` reads `Waiting for resource: <name>`.

If the resource has no limit, the store throws `ResourceNotConfiguredException`. The attempt fails without using a retry, and the job goes to the dead-letter queue. `submit()` already rejects a resource with no limit, so this happens only when a limit was deleted after the job was submitted. Set the limit again and re-run the job with `retryJob`.

Waiting jobs are served in order: higher priority first, then the job that has waited longest, then job id. A job starts waiting at the due time of the attempt that first missed, and later misses keep that time. When a permit is released, the engine wakes as many of the first waiters as there are free slots. A raised limit wakes that resource's waiters the same way. Orphan recovery and the start-up permit clean-up do not know which resources they freed, so they run the waiter sweep, which wakes the waiters of up to 64 resources and moves on to the next 64 the following time. A woken job's `scheduled_time` goes back to the time it started waiting, so a job query can show an earlier due time than the one set when it last parked. The wake-up also sends the cluster wake-up hint, so a node that can run the job polls straight away. Orphan recovery also runs a sweep that wakes waiters whose wake-up was lost.

The order has limits:

- A waiter holds its place for a lease of `retry_delay_ms` plus the larger of `retry_delay_ms` and 10 seconds, and each re-check renews it. A waiter that no node can claim, for example because of tags, a named queue, a full pool or an open circuit breaker, stops counting when its lease ends, and later jobs can pass it. It gets its place back when it next re-checks.
- A paused waiter does not count.
- Priority comes first, and waiting does not raise it. A steady stream of higher-priority jobs can keep a lower-priority waiter waiting for as long as the stream lasts.
- Jobs that have never waited and reach a free slot at the same moment race for it. In a burst submitted while the resource is idle, any of them can run first. The ones that miss then wait in order.
- On the SQL stores, if another transaction holds a waiter's job row when the wake-up runs, the wake-up skips that waiter rather than wait for the lock. The waiter then becomes due at its own retry time, at the next wake-up or at the sweep, and its slot can stay unused until then. Waiters behind it are not woken in its place. On MongoDB the wake-up retries instead.
- A job whose resource changes leaves the old resource's line. It joins the new resource's line at its next miss, with its due time at that attempt as its place. On the SQL stores, a park that finds the job row held by another transaction, such as a save in progress, parks the job without a place in line. The job becomes due at its retry time and gets its place at its next miss.

A permit belongs to the attempt's claim. An attempt whose job is no longer RUNNING under its claim takes no permit and stops; this happens when orphan recovery or a timeout has rescheduled the job, or when the job has ended. The store checks the claim once, just before it writes the permit, so an attempt whose job is rescheduled after that check still gets its permit and keeps it until it releases it. A retry never reuses the permit of an earlier attempt of the same job. That permit counts against capacity like any other until the earlier attempt releases it or clean-up removes it. With a limit of 1, the retry waits for it and is rescheduled each time it finds the resource full. With a higher limit, the retry starts as soon as a slot is free. An earlier attempt releases its permit when its body ends, so if the body ignores its timeout interrupt, the retry waits as long as the body runs. If the release fails, the node tries it again after 5 seconds and doubles the wait before each later try, up to 1 minute. A permit has no expiry, so the node keeps trying until the release succeeds or the node stops. It logs a warning at the first failure and again after every 10 minutes of failed tries, and logs when the release succeeds. Each try names the attempt's claim, so it never removes a permit that a later attempt of the job holds. A worker thread that is still interrupted when the body ends does not call the store; the node makes the release from its scheduler thread instead. Orphan recovery removes the permits of a node whose heartbeat has stopped. A starting node deletes the permits left under its own node id before it claims work. A permit still held when the node stops stays until one of these clean-ups removes it.

**Recording the attempt**

Steps 4 to 6 run before the attempt is recorded. When all three pass, the task records the job-start metric, writes the attempt's row in `scheduler_job_execution`, publishes `JobStartedEvent` and opens the tracing span. A job that stops at one of these steps records none of them. A job that waits a long time for a permit, or for an open circuit breaker, adds no rows to its execution history and publishes no start events. An attempt that fails at these steps, for example because its resource is not configured, has no execution row either; the job's `last_error` holds the error.

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

`cancelJob()` and `replace()` move a running job to `CANCELED` in the store and ask its body to stop (see [Canceling a running job](#cancelling-a-running-job)). The executor also re-reads the status before it starts work and after the method returns. If the job was canceled, the result is discarded and the job stays `CANCELED`.

## Transactions and Database Connections

`JobTask.call()` does **not** run inside a transaction. This is deliberate: holding a database connection open for the entire duration of a long-running job would exhaust connection pools.

Instead, each database operation (status update, retry scheduling, success marking) is a separate short transaction. The `@Transactional` annotation is on the individual service methods (`PostExecutionHandler`, `DeadLetterService`, etc.), not on `JobTask` itself.

Two features open a transaction around job code on purpose. [`@TransactionalJob`](./transactional-jobs.md) runs the whole body in the transaction that records success. [`JobContext.runInTransaction`](../api-reference/job-context.md#saving-the-checkpoint-with-the-work) runs one unit of work at a time and saves the unit's checkpoint in its transaction. Its connection is held only for that unit. Before the commit, Ratchet locks the job's queue row and checks that the attempt still owns the job, as completion does.

## Timeout Handling

A cancel is recorded for an attempt only when it ended that attempt's claim.
A cancel after orphan recovery, or of a newer claim, is not recorded for the old attempt.
A deleted job is not a cancel.
When the row is gone before the body starts, the worker does not run the body and records nothing.
The same holds when a cancel ended another claim before the body starts.
Otherwise the worker finishes through its normal fenced paths.

Each execution attempt has a deadline: the time the attempt started plus the job's timeout. A job without its own timeout uses `ratchet.timeout.default-sla-seconds` (30 minutes by default). A watchdog on the executing node does up to three things for each attempt. When the grace is longer than the soft-warning margin, step 2 happens before step 1.

1. At `ratchet.timeout.soft-timeout-percent` of the timeout (80% by default), it logs a warning.
2. At the deadline minus `ratchet.timeout.cancellation-grace-seconds`, it sets the attempt's cancellation request. This step is skipped when the grace is `0` (the default) or not shorter than the timeout.
3. At the deadline, if the attempt is still running, it sets the cancellation request (if it is not set yet) and cancels the attempt. If the job body is running, the worker thread is interrupted with `Future.cancel(true)`. A body that has not started does not start, and a worker that has returned from the body is not interrupted while it records the outcome. The attempt then counts as a timeout: it is retried if it has retries left, otherwise it fails and goes to the DLQ. An attempt that already stopped after the request is not interrupted (see [Stopping at a safe point](#stopping-at-a-safe-point)).

```java
scheduler.enqueue(() -> longRunningTask())
    .withTimeout(Duration.ofMinutes(5))
    .submit();
```

Once the watchdog has counted the timeout against the attempt, the run can no longer finish the
job, even before the watchdog schedules the retry. If the body returns successfully after that
point, Ratchet discards the result, and the job runs again under the watchdog's retry or ends
FAILED when no retries are left. If the run finishes first, its result stands and the watchdog's
timeout handling is skipped.

### Stopping at a safe point

An interrupt lands wherever the job body happens to be blocked: waiting for a pooled bean, a JDBC connection, or a cache lock. The code it interrupts may report the timeout as an unrelated error and leave container state half-changed. A job that works in units, such as pages of a table, can stop between units instead. `JobContext` tells it when to stop:

| Method | Returns |
|---|---|
| `deadline()` | `Optional<Instant>`: when the watchdog will cancel this attempt. Empty when the context was not bound for a watched execution, for example inside an `onFailure` callback that the watchdog runs after it interrupts a job. |
| `isCancellationRequested()` | `true` once Ratchet has asked this attempt to stop. Cheap to call; it does no I/O. |
| `throwIfCancellationRequested()` | Throws `CancellationRequestedException` if a stop was requested. |

```java
public void migrate() {
  JobContext ctx = JobContext.current();
  while (hasMorePages()) {
    ctx.throwIfCancellationRequested(); // stop between pages, not in the middle of one
    copyNextPage();
  }
}
```

Set a grace period that is longer than one unit of work, so the job sees the request and finishes its current unit before the deadline:

```properties
ratchet.timeout.cancellation-grace-seconds=60
```

What happens next depends on how the job ends:

- **It throws `CancellationRequestedException`** (directly, or as the cause of another exception) and the exception was created after the request: the attempt is a timeout, exactly as if the watchdog had interrupted it. The attempt count, retry backoff, `JobExecutionTimedOutEvent`, DLQ routing and `onFailure` callback are the same. The worker thread is not interrupted. The exception records the request state when it is created, so one created before the request stays an ordinary failure, even if the request arrives while it unwinds through a `finally` block or a wrapper. The exception also records the attempt it was created in, so one kept from an earlier attempt and rethrown by a retry is an ordinary failure. `throwIfCancellationRequested()` only throws after a request, so its exception counts for that attempt. If you create the exception yourself on another thread, build it with `CancellationRequestedException.forContext(message, context)` and pass the job's `JobContext`, because the context is only bound on the job thread.
- **It returns normally**: the attempt succeeds. Ratchet cannot tell a job that stopped early from one that finished just before its deadline, so a job that stops before its work is done must throw.
- **It keeps running**: the watchdog interrupts it at the deadline, as before.

Each attempt has its own request flag, so a retry starts with the flag cleared. The flag is held in memory on the node running the attempt, where the watchdog also runs.

### Canceling a running job {#cancelling-a-running-job}

`JobSchedulerService.cancelJob` and `replace` also set the request for a running job. When the cancel runs on the node that runs the job, the flag is set right after the cancel commits. Every node also checks the store every 5 seconds for its attempts that have run for at least 5 seconds, so a cancel made on another node reaches the job within about 10 seconds. Ratchet does not interrupt the worker thread for a cancel. A job that never checks the flag runs until it returns or the watchdog interrupts it at its deadline, and its result is discarded.

When a canceled job stops by throwing any exception, the attempt ends as `CANCELED`. Its execution record is `CANCELED`, and there is no retry, DLQ entry, `JobExecutionTimedOutEvent` or `onFailure` callback. The cancel publishes the one `JobCancelledEvent`. It also cancels the chain and workflow steps that wait on the job, so they never start, whatever the body does next. A resource permit held by the job is released when the body ends.

A cancel can also land close to the attempt's deadline, after the worker has stopped but before any outcome for the attempt is written. For example, the worker saved its execution as failed with the timeout and left the timeout outcome to the watchdog, or the watchdog stopped a worker whose success write was stuck on a store error. The watchdog then finds the job `CANCELED` and writes no retry or failure. The attempt still ends as `CANCELED`. If the worker has ended, the watchdog marks its execution record `CANCELED`. If the worker is still running, the worker does this when it ends. Either way the execution record ends `CANCELED`, and only one of them changes it. An attempt that already wrote its own outcome, such as a retry, keeps that outcome.

A step that waits on a canceled or failed job can start just before Ratchet cancels it. Ratchet then cancels that step as a running job, and holds its business key as described below. The step gets its stop request from the store check every 5 seconds, so the request arrives within about 10 seconds. No `JobCancelledEvent` is published for the step. The `ChainFailedEvent` for a canceled job has the error message `Job was canceled`. If the job was retried before the cancel, it keeps the error of its earlier failed attempt, and the event carries that error instead.

If the job has a business key, the key stays held until the body ends, so a new job with the same key cannot run alongside the old body. While the key is held, `submit()` throws `DuplicateBusinessKeyException` and `findBusinessKeyHolder` returns the canceled job with status `CANCELED`. `cancelJob` and `replace` of a running job hold the key. So do the cancels Ratchet makes itself, such as the cancel of a running step described above. A hard timeout does not. The hold ends in one of four ways:

- The worker releases it when the body ends. Ratchet's timeout and shutdown interrupts reach a worker only while its body runs, and the worker clears that interrupt before it makes the release. If the store call fails, or if the worker thread is interrupted again after the body ends, a scheduler thread makes the release instead and tries again every 5 seconds for up to 5 minutes. A late interrupt can come from the application, from the container, or from the standalone executor when shutdown has to force it to stop.
- The watchdog releases it when it finds the job canceled and no body of the attempt can still run. Either the worker has ended, which can happen when the cancel lands close to the deadline, or no worker has started the attempt yet. In the second case the watchdog has already cancelled execution, so a worker that starts late cannot run the body. The watchdog never releases the key while the body still runs. A failed release is retried in the same way.
- The orphan scan on another node releases it when it cleans up the node that ran the body. A node also releases its own holds when it starts.
- The orphan scan deletes it on its first run after its limit has passed. The limit is the cancel time plus the job's timeout (or `ratchet.timeout.default-sla-seconds`), plus `ratchet.node.orphan-grace-seconds`, plus `ratchet.node.orphan-scan-interval-seconds`. The body's deadline comes first, so the limit only ends the hold of a body that ignored both the stop request and the deadline interrupt.

The node release and the expiry cleanup never wait for a hold that another transaction has locked, such as a release on a node that has stopped responding. They skip that hold. The expiry cleanup tries it again on every scan. When the release for a dead node skips a hold, the scan keeps that node's `scheduler_node` row, so the next scan finds the node again and tries the release again. The row is deleted once none of the node's holds is left. A hold that a node skipped when it released its own holds at start-up stays until its limit has passed.

The node that cancels the job sets the limit, and the node that runs the body sets the deadline. Give every node the same `default-sla-seconds`, `orphan-grace-seconds` and `orphan-scan-interval-seconds`, and keep their clocks in sync. Otherwise the limit can pass before the body's deadline.

If the worker thread is interrupted again after the body ends, the worker does not check for the cancel, so the attempt's execution record can show a failure.

Deleting or archiving a job does not end its hold. The store's `delete` of a held job fails, its `deleteJobsByIds` skips the job, and job retention archives the job only after the hold ends.

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

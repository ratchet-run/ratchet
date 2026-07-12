---
sidebar_position: 2
title: Job Lifecycle
description: Complete job state machine with transitions, guards, and edge cases
---

# Job Lifecycle

Every job in Ratchet follows a defined state machine from creation to terminal state. The states and transitions below determine how jobs are claimed, retried, paused, and archived.

## State Machine

<div class="docs-diagram" role="img" aria-label="Ratchet job state machine: PENDING jobs can run, pause, cancel, or retry; RUNNING jobs succeed, fail, or cancel; WAITING jobs unblock to PENDING when a signal arrives or fail on timeout.">
  <div class="docs-diagram-row docs-diagram-row--tight">
    <div class="docs-diagram-state docs-diagram-state--primary">
      <strong>PENDING</strong>
      <small>Queued and eligible when `scheduled_time <= now`.</small>
    </div>
    <div class="docs-diagram-state docs-diagram-state--active">
      <strong>RUNNING</strong>
      <small>Claimed by one node and executing on a worker.</small>
    </div>
    <div class="docs-diagram-state docs-diagram-state--warning">
      <strong>WAITING</strong>
      <small>Blocked for an external signal; hidden from polling.</small>
    </div>
    <div class="docs-diagram-state docs-diagram-state--muted">
      <strong>PAUSED</strong>
      <small>Temporarily hidden; resumes to the stored previous state.</small>
    </div>
    <div class="docs-diagram-state docs-diagram-state--success">
      <strong>SUCCEEDED</strong>
      <small>Terminal success; eligible for archival after retention.</small>
    </div>
    <div class="docs-diagram-state docs-diagram-state--danger">
      <strong>FAILED</strong>
      <small>Terminal when retries are exhausted; otherwise retried.</small>
    </div>
    <div class="docs-diagram-state docs-diagram-state--danger">
      <strong>CANCELED</strong>
      <small>Terminal cancellation from queued, running, paused, or waiting work.</small>
    </div>
  </div>

  <div class="docs-diagram-connector">
    <span>Main transitions</span>
  </div>

  <div class="docs-diagram-row">
    <div class="docs-diagram-card">
      <strong>PENDING -> RUNNING</strong>
      <small>Poller claims the row atomically with `SKIP LOCKED`.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>RUNNING -> SUCCEEDED</strong>
      <small>The task completes and result handling succeeds.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>RUNNING -> FAILED</strong>
      <small>The task throws, times out, or exhausts retry handling.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>FAILED -> PENDING</strong>
      <small>Automatic retry with backoff, or manual `retryJob()` reset.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>PENDING -> PAUSED</strong>
      <small>`pauseJob()` records `paused_from_status` for accurate resume.</small>
    </div>
    <div class="docs-diagram-card">
      <strong>WAITING -> PENDING</strong>
      <small>`deliverSignal()` unblocks the job and attaches the signal payload.</small>
    </div>
  </div>
</div>

## States

### PENDING

The job is queued and waiting for execution. A PENDING job becomes visible to the Poller when its `scheduled_time <= now`. Most jobs start in this state when submitted.

- **Visible to Poller:** Yes, when scheduled time has passed
- **Transitions to:** RUNNING (claimed by worker), PAUSED (via `pauseJob()`), CANCELED (via `cancelJob()`)

### RUNNING

A worker has claimed the job and is actively executing it. The `picked_by` field records which node owns the job, and optimistic locking (`@Version`) prevents duplicate execution.

- **Visible to Poller:** No
- **Transitions to:** SUCCEEDED (execution completes), FAILED (exception thrown or timeout), CANCELED (via `cancelJob()` -- checked mid-execution)
- **Guard:** Only one node can hold a RUNNING job at a time

### SUCCEEDED

The job completed without throwing an exception. This is a terminal state. Succeeded jobs may trigger dependent workflow branches or chain steps.

- **Terminal:** Yes
- **Eligible for archival:** Yes, after retention period

### FAILED

The job threw an exception during execution. A FAILED job may or may not have retries remaining:

- **If retries remain:** The engine schedules a retry (back to PENDING with a backoff delay). The job entity stays in FAILED only momentarily during the transition.
- **If retries exhausted:** The job is permanently FAILED and moved to the Dead Letter Queue. This is a terminal state.
- **If `@DoNotRetry`:** Skips retries entirely, moves directly to DLQ.

Transitions:
- **Back to PENDING:** Automatic retry (retries remain) or manual `retryJob()` call

A FAILED job cannot be paused -- it is terminal, so `pauseJob()` returns `false`.

### PAUSED

The job is temporarily suspended and invisible to the Poller. The `paused_from_status` column records the state the job had before pausing, so it can be accurately restored.

- **Visible to Poller:** No
- **Transitions to:** Previous state via `resumeJob()` -- restores PENDING
- **Idempotent:** Pausing an already-paused job returns `true` without error

### WAITING

The job is blocked until an external signal is delivered. WAITING jobs are not visible to the Poller. Signal delivery transitions the job to PENDING and stores the payload, which the running job reads via `JobContext.signalPayload(Class)`.

- **Visible to Poller:** No
- **Transitions to:** PENDING via `deliverSignal()`, FAILED on signal timeout, CANCELED via `cancelJob()`
- **Guard:** WAITING jobs cannot be paused

### CANCELED

The job was explicitly canceled and will not execute. This is a terminal state. Canceling a RUNNING job sets the status; the executor checks status mid-flight and discards results.

- **Terminal:** Yes
- **Cascading:** Canceling a chain step cancels all downstream dependents

## Transition Details

### Submission to PENDING

When you call `submit()` on a builder, the engine:

1. Analyzes the lambda to extract target class, method, and arguments
2. Converts that metadata into a persisted job payload via the active `JobInvocationResolver`
3. Checks the idempotency key for duplicates (globally unique, forever)
4. Checks the business key for active conflicts (unique among PENDING/RUNNING jobs)
5. Persists the `JobEntity` with status PENDING
6. For immediate or CRITICAL-priority jobs, publishes a wakeup notification via `ClusterCoordinator`

```java
JobHandle handle = scheduler.enqueue(() -> service.process(id))
    .withIdempotencyKey(requestId)  // prevents duplicate submission
    .withBusinessKey("process-" + id)  // prevents concurrent processing
    .submit();
```

### PENDING to RUNNING (Claim)

The Poller executes a query like:

```sql
SELECT job_id FROM scheduler_job_queue
WHERE status = 'PENDING'
  AND scheduled_time <= NOW()
ORDER BY (priority + age_boost) DESC, scheduled_time ASC
FOR UPDATE SKIP LOCKED
LIMIT :batchSize
```

`age_boost` is computed from the configured priority-boost interval, so old low-priority work can outrank newer high-priority work. `SKIP LOCKED` lets multiple nodes poll concurrently without blocking each other. Each node claims a non-overlapping set of jobs. The claimed jobs are atomically updated:

- `status` = RUNNING
- `picked_by` = node ID
- `picked_at` = current timestamp

### RUNNING to SUCCEEDED

When the job method returns normally:

1. Execution timing is recorded (start, end, duration, queue wait)
2. Return value is serialized to JSON (if non-void)
3. Status atomically transitions RUNNING -> SUCCEEDED via `markJobSucceeded()`
4. `JobCompletedEvent` is published
5. Post-execution handler triggers:
   - For batch children: updates parent batch progress
   - For chain steps: schedules next step
   - For workflow branches: evaluates conditions and schedules the first matching branch
6. Success callback (`onSuccess`) is invoked if configured

### RUNNING to FAILED (with Retry)

When the job throws an exception and retries remain:

1. Attempt counter is atomically incremented
2. `@DoNotRetry` check on the exception class
3. `RetryPolicy.shouldRetry()` is consulted
4. Backoff delay is calculated (see [Retry Strategies](./retry-strategies.md))
5. Job is rescheduled: `scheduled_time = now + backoff`, status back to PENDING
6. `JobRetryingEvent` is published

### RUNNING to FAILED (Terminal -- DLQ)

When retries are exhausted or `@DoNotRetry` applies:

1. Status transitions RUNNING -> FAILED via compare-and-swap
2. Error message is sanitized via `ErrorSanitizer` SPI
3. `DeadLetterService.moveToDlq()` records the alert with deduplication
4. `JobDlqEvent` is published
5. For batch children: parent batch progress is updated (failure)
6. For chain/workflow: downstream evaluation occurs (FAILURE branches may fire)
7. Failure callback (`onFailure`) is invoked if configured

### Pause and Resume

Pausing suspends a job without losing its state:

```java
scheduler.pauseJob(jobId);   // PENDING -> PAUSED
scheduler.resumeJob(jobId);  // PAUSED -> original state
```

The `paused_from_status` field preserves context:
- A paused PENDING job resumes to PENDING (eligible for polling again)

Only PENDING jobs can be paused. FAILED is terminal, so `pauseJob()` returns `false` for it; RUNNING jobs cannot be paused -- cancel them instead.

### Manual Retry

For jobs in the Dead Letter Queue, `retryJob()` provides manual recovery:

```java
scheduler.retryJob(jobId);
```

This:
1. Resets the attempt counter to 0
2. Clears error information
3. Sets `scheduled_time` to now
4. Transitions FAILED -> PENDING

Only FAILED jobs can be retried. The job becomes immediately eligible for polling.

### Cancellation

```java
scheduler.cancelJob(jobId);
```

Behavior depends on current state:
- **PENDING:** Immediately transitions to CANCELED
- **RUNNING:** Sets status to CANCELED. The executor periodically checks `wasJobCanceledDuringExecution()` and discards results if true
- **WAITING:** Cancels the signal wait and prevents future delivery from releasing the job
- **Terminal states:** Returns `false` (cannot cancel completed jobs)

For chain steps, cancellation cascades to all downstream dependents using depth-first traversal.

## Optimistic Locking

The `JobEntity` uses JPA `@Version` for optimistic locking. When two nodes attempt to modify the same job concurrently, one will get an `OptimisticLockException`. Combined with `SKIP LOCKED` during claiming, this prevents two nodes from running the same job at the same time:

- `SKIP LOCKED` prevents two nodes from claiming the same job
- `@Version` prevents stale updates if a race occurs during status transitions

## Orphan Recovery

If a node crashes while executing a job, the job remains in RUNNING state with no node to complete it. The `OrphanRecoveryTimer` periodically scans for stale RUNNING jobs (based on `picked_at` timestamp) and resets them to PENDING for re-execution.

## Archival

Completed jobs (SUCCEEDED and FAILED) are eligible for archival after a configurable retention period. The `JobArchivingService` moves old jobs from `scheduler_job` to `scheduler_job_archive`, keeping the active table lean for efficient polling.

## Related

- [Execution Model](./execution-model.md) -- How the Poller and executor work together
- [Error Handling](./error-handling.md) -- Detailed retry and DLQ mechanics
- [Retry Strategies](./retry-strategies.md) -- Backoff policies and custom retry logic

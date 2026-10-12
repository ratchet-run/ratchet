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

A worker has claimed the job and is actively executing it. The `picked_by` field records which node owns the job, and each claim advances a claim sequence. Owner writes must match that sequence, so a recovered job rejects outcomes and retry updates from an older execution. Delivery remains at least once: job bodies can overlap after recovery, so external side effects need idempotency.

- **Visible to Poller:** No
- **Transitions to:** SUCCEEDED (execution completes), FAILED (exception thrown or timeout), CANCELED (via `cancelJob()` -- checked mid-execution)
- **Guard:** Only the current claim can persist owner writes

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
4. Checks the business key for active conflicts (unique among PENDING/RUNNING/PAUSED/WAITING jobs)
5. Persists the `JobEntity` with status PENDING
6. For immediate or CRITICAL-priority jobs, publishes a wakeup notification via `ClusterCoordinator`

A conflicting business key causes `submit()` to throw `DuplicateBusinessKeyException`. Retrying does not help until the active job reaches a terminal state. A racing submission with the same idempotency key either returns the original job's handle or throws `DuplicateIdempotencyKeyException`. Retrying that submission in a fresh transaction returns the original handle.

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

When retries are exhausted, `@DoNotRetry` applies, poison data is detected, or a protective
runtime limit forces terminal handling:

1. Status transitions RUNNING -> FAILED via compare-and-swap
2. Error message is sanitized via `ErrorSanitizer` SPI
3. The durable job row records terminal FAILED status, the sanitized error, and the final retry
   count; there is no separate alert or deduplication ledger
4. `JobFailedEvent` is followed by one centrally published `JobDlqEvent` after the terminal commit.
   `JobDlqEvent` is a non-replayable notification; the durable FAILED row is the source of truth
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

To recover multiple failures from one incident, use a bounded `JobFilter` operation:

```java
int retried = scheduler.retryJobs(
    JobFilter.builder().tags("billing").build(),
    250);
```

The selected jobs move from FAILED to PENDING atomically. The 1 to 1000 limit keeps each recovery
transaction bounded; repeat the call for larger DLQs.

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

## Claim fencing

Each claim increments a persisted `claim_seq`. The worker keeps the sequence it received and
includes it when completing, retrying, failing, or releasing that claim. The store rejects a write
from an older claim before changing the outcome, attempts, or dependent jobs. This also protects
against an older execution on the same node after that node reclaims the job.

SQL stores lock queue rows when claiming; MongoDB uses atomic filter-and-update operations.
The separate `version` column guards optimistic state transitions. Neither mechanism prevents a
stalled worker's body from overlapping a recovered execution. Delivery remains at least once;
job code must make external side effects idempotent.

## Orphan Recovery

If a node crashes while executing a job, orphan recovery checks its heartbeat and claim age. It returns the job to PENDING while its crash count is below `ratchet.node.max-crash-redeliveries`. Each recovery adds one to the count. The default allows three redeliveries. The next crash fails the job and sends it to the DLQ. A limit of zero fails it after the first crash. This budget is separate from application retries.

A committed crash failure runs `onFailure`. If the job payload cannot be loaded, Ratchet fails it from stored metadata and skips the callback. A manual DLQ retry starts with a fresh crash budget. Graceful shutdown releases claims without charging the budget, even when a worker outlives the drain timeout.

The next claim advances the claim sequence. A stalled worker cannot overwrite the new owner's outcome or retry count. External side effects still require idempotency. Execution-history rows for crashed runs can still remain RUNNING; cleaning up those rows is a separate follow-up.

## Archival

Completed jobs (SUCCEEDED and FAILED) are eligible for archival after a configurable retention period. The `JobArchivingService` moves old jobs from `scheduler_job` to `scheduler_job_archive`, keeping the active table lean for efficient polling.

## Related

- [Execution Model](./execution-model.md) -- How the Poller and executor work together
- [Error Handling](./error-handling.md) -- Detailed retry and DLQ mechanics
- [Retry Strategies](./retry-strategies.md) -- Backoff policies and custom retry logic

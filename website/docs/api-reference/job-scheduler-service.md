---
sidebar_position: 2
title: JobSchedulerService Reference
description: Complete method reference for JobSchedulerService, the primary entry point for scheduling background jobs.
---

# JobSchedulerService

The primary entry point for all job scheduling operations. Inject this interface to enqueue jobs, create batches, schedule recurring work, and manage the job lifecycle.

```java
@Inject
JobSchedulerService scheduler;
```

**Package:** `run.ratchet.api`
**Type:** Interface

## Scheduling methods

### enqueue

```java
JobBuilder enqueue(SerializableCheckedRunnable task)
```

Enqueues a task for immediate execution, returning a [`JobBuilder`](./job-builder) for further configuration. The task is not persisted until `submit()` is called on the returned builder.

**Parameters:**
- `task` -- the job task to execute. Must be a single method reference or single method call (see [method reference constraint](./functional-interfaces#serializablecheckedrunnable)).

**Returns:** a `JobBuilder` for configuring retries, priority, timeout, workflows, tags, parameters, and callbacks.

```java
// Simple enqueue with configuration
JobHandle handle = scheduler.enqueue(() -> emailService.send(userId))
    .withPriority(JobPriority.HIGH)
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(5))
    .withTimeout(Duration.ofMinutes(10))
    .withTags("email", "notifications")
    .withParam("userId", userId)
    .submit();
```

### enqueueNow

```java
JobHandle enqueueNow(SerializableCheckedRunnable task)
```

Enqueues a task for immediate execution with default configuration. This is a convenience method equivalent to `enqueue(task).immediate().submit()`.

**Parameters:**
- `task` -- the job task to execute.

**Returns:** a `JobHandle` containing the assigned UUIDv7 job ID.

```java
// Fire-and-forget with default settings
JobHandle handle = scheduler.enqueueNow(() -> auditService.log(event));
UUID jobId = handle.id();
```

### schedule

```java
JobBuilder schedule(Duration delay, SerializableCheckedRunnable task)
```

Schedules a task to execute after the specified delay. Returns a `JobBuilder` for further configuration.

**Parameters:**
- `delay` -- how long to wait before the job becomes eligible for execution. Must not be null.
- `task` -- the job task to execute.

**Returns:** a `JobBuilder` for further configuration.

```java
// Send a reminder email in 24 hours
scheduler.schedule(Duration.ofHours(24), () -> reminderService.send(orderId))
    .withParam("orderId", String.valueOf(orderId))
    .withTags("reminder")
    .submit();
```

### scheduleRecurring

```java
RecurringJobBuilder scheduleRecurring(
    String cron, ZoneId zone, SerializableCheckedRunnable task)
```

Schedules a recurring job based on a cron expression. Returns a `RecurringJobBuilder` for configuring options, tags, identity, and downtime behavior.

**Parameters:**
- `cron` -- Quartz cron expression (6-7 fields: `second minute hour day-of-month month day-of-week [year]`).
- `zone` -- timezone for evaluating the cron expression.
- `task` -- the job task to execute on each occurrence.

**Returns:** a `RecurringJobBuilder` for configuring options, tags, business key, and a persisted `RecurringMisfirePolicy`.

```java
// Every weekday at 9 AM Eastern
scheduler.scheduleRecurring(
        "0 0 9 ? * MON-FRI",
        ZoneId.of("America/New_York"),
        () -> reportService.generateDailyReport())
    .withOptions(JobOptions.defaults()
        .withMaxRetries(2)
        .withBackoff(BackoffPolicy.FIXED, Duration.ofMinutes(5)))
    .withTags(List.of("reports", "daily"))
    .withBusinessKey("daily-report")
    .withMisfirePolicy(RecurringMisfirePolicy.fireOnce())
    .submit();
```

`withMisfirePolicy` accepts `skip()`, `fireOnce()`, or `catchUp(maxExecutions)`. The default is `catchUp(11)`, which preserves the previous bounded catch-up behavior. Policies apply only when at least two occurrences are overdue; a lone due occurrence runs normally.

## Batch methods

### enqueueBatch

```java
BatchBuilder enqueueBatch(String name)
```

Creates a [`BatchBuilder`](./batch-builder) for parallel execution of multiple tasks. The batch is submitted as a single unit and provides aggregate progress tracking.

**Parameters:**
- `name` -- human-readable name for the batch, used in logs and monitoring.

**Returns:** a `BatchBuilder` for adding items, configuring workflows, and submitting.

```java
List<Long> orderIds = orderRepository.findPending();

scheduler.enqueueBatch("Process Pending Orders")
    .forEach(orderIds, orderId -> orderService.process(orderId))
    .onProgress(ctx -> log.info("Batch {} is {}% complete", ctx.batchId(), ctx.percentDone()))
    .thenOnBatchSuccess(() -> notificationService.sendBatchComplete())
    .thenOnBatchFailure(() -> alertService.sendBatchFailed())
    .submit();
```

### streamingBatch

```java
<T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name)
```

Creates a `StreamingBatchBuilder` for memory-efficient processing of large datasets. Items are read from a stream and inserted in chunks, avoiding loading the entire dataset into memory.

**Type Parameters:**
- `T` -- the type of items to process; must implement `Serializable`.

**Parameters:**
- `name` -- human-readable name for the batch.

**Returns:** a `StreamingBatchBuilder` for configuring the stream source, processing action, chunk size, and callbacks.

```java
scheduler.<Long>streamingBatch("Migrate Users")
    .fromStream(userRepository.streamAllUserIds())
    .process(userId -> migrationService.migrateUser(userId))
    .withChunkSize(1000)
    .onProgress(ctx -> log.info("Streamed {} items in {} chunks",
        ctx.processedItems(), ctx.chunksInserted()))
    .thenOnBatchSuccess(() -> log.info("Migration complete"))
    .start();
```

## Job replacement

### replace

```java
JobHandle replace(UUID jobId, Duration delay,
                  SerializableCheckedRunnable newTask, JobOptions opts)
```

Replaces an existing job with a new one. The original job is canceled and a new job is created with the specified delay and options.

**Parameters:**
- `jobId` -- the ID of the existing job to replace.
- `delay` -- delay before the replacement job becomes eligible for execution.
- `newTask` -- the new task to execute.
- `opts` -- execution options for the replacement job.

**Returns:** a `JobHandle` for the newly created replacement job.

```java
// Replace a pending job with updated logic
JobHandle replacement = scheduler.replace(
    originalHandle.id(),
    Duration.ofMinutes(5),
    () -> updatedProcessingService.process(data),
    JobOptions.defaults().withPriority(JobPriority.HIGH));
```

## Job control methods

### cancelJob

```java
boolean cancelJob(UUID jobId)
```

Cancels a job by its ID. For a batch parent, also cancels PENDING, PAUSED and WAITING
children and unstarted callbacks with their chain steps. A RUNNING child finishes its
current attempt. If that attempt fails, it is not retried. No child starts a new
attempt after the cancel.
Cancelling a batch child counts as a failure. The batch completes
as soon as its last child ends.

- **PENDING** jobs transition directly to CANCELED.
- **RUNNING** jobs transition to CANCELED; the executor should check status before committing results.
- Jobs in terminal states (SUCCEEDED, FAILED, CANCELED) cannot be canceled.

**Parameters:**
- `jobId` -- the ID of the job to cancel.

**Returns:** `true` if the job was successfully canceled; `false` if the job was not found or is already in a terminal state.

```java
boolean canceled = scheduler.cancelJob(handle.id());
if (!canceled) {
    log.warn("Job {} could not be canceled", handle.id());
}
```

### pauseJob

```java
boolean pauseJob(UUID jobId)
```

Pauses a job, preventing it from being picked up for execution.
For a batch parent, also pauses its PENDING children. RUNNING children finish.
Callbacks stay locked, and batch completion waits for resume.
On MongoDB, child updates run in separate transactions. If a batch pause or resume is
interrupted, call it again to finish the remaining children.

- Only **PENDING** jobs can be paused.
- The job's previous status is recorded so it can be restored on resume.
- Jobs in **RUNNING**, **WAITING**, or terminal states cannot be paused.
- **Idempotent:** pausing an already-PAUSED job returns `true` without error.

**Parameters:**
- `jobId` -- the ID of the job to pause.

**Returns:** `true` if the job was paused or was already paused; `false` if the job was not found or in an incompatible state.

```java
scheduler.pauseJob(jobId);
// Later...
scheduler.resumeJob(jobId);
```

### resumeJob

```java
boolean resumeJob(UUID jobId)
```

Resumes a paused job, making it eligible for execution again.
For a batch parent, also resumes every PAUSED child, including children paused
individually. Checks for batch completion after the transaction commits.
On MongoDB, child updates run in separate transactions. If a batch pause or resume is
interrupted, call it again to finish the remaining children.

- The job returns to the status it had before being paused.
- Resuming a previously **PENDING** job makes it eligible for polling again.
- **Idempotent:** resuming a non-paused job returns `false` without error.

**Parameters:**
- `jobId` -- the ID of the job to resume.

**Returns:** `true` if the job was resumed; `false` if the job was not found or not in PAUSED state.
For an open batch parent that is already PENDING, returns `true` if PAUSED children were resumed.

```java
boolean resumed = scheduler.resumeJob(jobId);
```

### retryJob

```java
boolean retryJob(UUID jobId)
```

Resets a failed job for retry. This is the primary mechanism for manual retry of jobs in the Dead Letter Queue.

- The attempt counter is reset to 0.
- Error information is cleared.
- Scheduled time is set to now. Retried children of a paused batch come back PAUSED
  and run when the batch resumes. Other retried jobs become PENDING.
- Executable jobs must be **FAILED** to be retried.

**Parameters:**
- `jobId` -- the ID of the failed job to retry.

Failed batch children can be retried while their batch is running. The retry updates the batch's
failure counter. Passing an open batch parent retries all FAILED children and returns
`true` if any were reset. The parent must be PENDING or PAUSED with completion unprocessed.
Finished batches and their children cannot be retried.

**Returns:** `true` if the job was successfully reset for retry; `false` if not found, not FAILED,
or a child of a finished batch. For a batch parent, returns `true` if any child was reset;
returns `false` for a finished batch or when no FAILED child was reset.

```java
// Manual DLQ retry
boolean retried = scheduler.retryJob(failedJobId);
if (retried) {
    log.info("Job {} re-queued for execution", failedJobId);
}
```

### retryJobs

```java
int retryJobs(JobFilter filter, int limit)
```

Recovers a bounded set of failed jobs in one atomic store operation. Ratchet intersects the filter
with `FAILED`, ignores archived rows, and resets retry metadata.
Retried children of a paused batch come back PAUSED and run when the batch resumes.
Other retried jobs become PENDING and eligible again. The limit must be from 1 through 1000; repeat the call to drain a larger incident in
bounded transactions.

```java
JobFilter billingFailures = JobFilter.builder()
    .tags("billing")
    .createdAfter(incidentStarted)
    .sortField(JobQuerySortField.CREATED_AT)
    .sortAscending(true)
    .build();

int recovered = scheduler.retryJobs(billingFailures, 250);
```

- All `JobFilter` criteria and ordering apply, but only current `FAILED` jobs are eligible.
- Batch parents and children of finished batches are skipped. They do not count toward the limit.
  Retrying a child of a running batch updates its failure counter.
- The batch commits or rolls back as a unit. A business-key reservation conflict rolls back the
  whole selected batch instead of partially recovering it.
- This administrative operation does not run per-job authorization checks. Use `retryJob(UUID)`
  when each job needs an authorization decision.
- When the count is positive, Ratchet issues one scheduler wakeup and publishes at most one
  `JobsBulkRetriedEvent`. It does not emit one `JobRetryingEvent` per recovered job.

## Signal delivery methods

Signal-waiting jobs are created through [`JobBuilder.awaitSignal()`](./job-builder#awaitsignal). They remain in `WAITING` status until a matching signal is delivered, or until their signal timeout fails the job.

### deliverSignal by job ID

```java
int deliverSignal(UUID jobId, Serializable payload)
int deliverSignal(UUID jobId, SignalDecision decision)
```

Delivers a signal to a specific `WAITING` job. The job transitions to `PENDING`, and the payload is made available through [`JobContext.signalPayload()`](./job-context#signalpayload).

**Returns:** `1` when a waiting job was unblocked, `0` when the job was not found, was not waiting, or signal support is not configured.

```java
JobHandle approval = scheduler.enqueue(() -> approvalJob.continueOrder(orderId))
    .awaitSignal("order:" + orderId + ":approved", Duration.ofHours(24))
    .submit();

int delivered = scheduler.deliverSignal(
    approval.id(),
    SignalDecision.approved("approved-by-manager"));
```

### deliverSignal by signal key

```java
int deliverSignal(String signalKey, Serializable payload)
int deliverSignal(String signalKey, SignalDecision decision)
```

Delivers a signal to every `WAITING` job with the matching signal key. Store implementations perform this as an atomic bulk update so already-unblocked jobs are not counted twice.

```java
int unblocked = scheduler.deliverSignal(
    "billing-cycle-2026-05-ready",
    SignalDecision.approved("ledger-closed"));

log.info("Released {} waiting billing jobs", unblocked);
```

`SignalDecision` is scheduler-visible metadata for audit, metrics, and events. A rejected decision still unblocks the job; the job code decides how to handle it:

```java
SignalDecision decision =
    JobContext.current().signalPayload(SignalDecision.class);

if (decision != null && decision.isRejected()) {
    throw new RejectedOrderException(decision.rejectionReason());
}
```

## Recurring job management

### cancelRecurringJobsByTag

```java
int cancelRecurringJobsByTag(String tag)
```

Cancels all recurring jobs associated with the specified tag.

**Parameters:**
- `tag` -- the tag identifying the recurring jobs to cancel.

**Returns:** the number of jobs canceled.

```java
int canceled = scheduler.cancelRecurringJobsByTag("maintenance");
log.info("Canceled {} recurring maintenance jobs", canceled);
```

### cancelRecurringJobByBusinessKey

```java
int cancelRecurringJobByBusinessKey(String businessKey)
```

Cancels the active recurring job with the specified business key. This is the primary mechanism for replacing a recurring job definition during redeployment. Only jobs in active states (PENDING, RUNNING, PAUSED) with a matching business key and recurring job type are affected.

**Parameters:**
- `businessKey` -- the business key identifying the recurring job to cancel.

**Returns:** the number of jobs canceled (0 or 1, since business keys are active-unique).

```java
// Replace a recurring job by canceling the old one first
scheduler.cancelRecurringJobByBusinessKey("daily-report");
scheduler.scheduleRecurring("0 0 10 * * ?", ZoneId.of("UTC"),
        () -> reportService.generateDailyReport())
    .withBusinessKey("daily-report")
    .submit();
```

## Business key lookup

### findBusinessKeyHolder

```java
Optional<BusinessKeyHolder> findBusinessKeyHolder(String businessKey)
```

Returns the active holder of a business key from the same reservation that `JobBuilder.submit()` checks. A holder can be a job or a recurring master. `BusinessKeyHolder` contains `id()`, `type()` and `status()`. The id is the job id or recurring master id. `cancelJob(id)` cancels either.

For a job, the type is its public type (`SINGLE` for a job from `JobBuilder`) and the status is its live `PENDING`, `RUNNING`, `PAUSED` or `WAITING` status. For a recurring master, the type is `RECURRING` and the status is `PAUSED` when paused, or `PENDING` otherwise. This matches `JobQueryService.getRecurringMasters()`. Status is never null or terminal.

Returns empty when no reservation exists, including after the holder finishes, is canceled, or is deleted. It also returns empty if a reservation exists but its owner row is not visible to the read. This can happen during a store's own write or between MongoDB's reservation and owner reads.

Key handling matches `JobBuilder.withBusinessKey`: null or blank returns empty, and the key is trimmed. More than 255 characters after trimming, or a character outside printable ASCII (U+0020 to U+007E), throws `IllegalArgumentException`.

This is a point-in-time read. A later `submit()` can still lose a race. Treat the answer as a hint or pre-check, not a lock.

The call never starts a transaction that outlives it. Finding a holder, finding none, or rejecting an invalid key never marks the caller transaction rollback-only. A store failure throws and, as with `submit()`, can leave it rollback-only.

Whether the read joins a caller transaction depends on the store. The bundled SQL stores join it, so the read sees the caller's own uncommitted submissions. The MongoDB store does not. A MongoDB submission commits in its own session and is not rolled back with the caller transaction, and the lookup sees only committed data.

On MySQL under the default REPEATABLE READ, the transaction's first plain read fixes its snapshot. The lookup does not see a holder that commits after that point. It can still return a holder that ends after that point, because the end is not visible until a fresh snapshot. Call outside a transaction or use READ COMMITTED for the latest state.

There is no per-job authorization check, matching `submit()`, which reports a held key to any caller.

The method is inherited from the stable `BusinessKeyLookup` interface. Inject that interface when only the lookup is needed:

```java
@Inject
BusinessKeyLookup businessKeys;

void inspect(String key) {
    businessKeys.findBusinessKeyHolder(key).ifPresent(holder -> {
        System.out.println(holder.id() + " " + holder.type() + " " + holder.status());
    });
}
```

`JobQueryService.findJobs` searches job rows, including finished jobs. It sees recurring masters only when the filter's `types` is exactly `RECURRING`, which reads the recurring store. Other searches need the query capability and find nothing without it. Use this lookup to ask whether a key is held now.

## Idempotency key lookup

### findIdempotencyKeyHolder

```java
Optional<IdempotencyKeyHolder> findIdempotencyKeyHolder(String idempotencyKey)
```

Reads the same ledger that `submit()` checks. The stable `IdempotencyKeyLookup` interface can also be injected directly. An unused key returns empty. Null or blank keys return empty; keys are trimmed, and more than 36 characters throws `IllegalArgumentException`, matching `withIdempotencyKey`.

The holder contains `id()`, `retention()`, `type()` and `status()`:

| Retention | What the holder reports |
| --- | --- |
| `LIVE` | The job still has a live row. `type()` is its public type and `status()` its current status. |
| `ARCHIVED` | Only an archive row remains. `type()` is its public type and `status()` its terminal final status. |
| `PURGED` | No row remains, only the ledger entry. `type()` and `status()` are null. |

`PURGED` also covers a deleted job on a store without archive support. In every state, a resubmit under the same key returns the original id and runs nothing. `findJobs` with `JobFilter.idempotencyKey` searches live jobs only.

For a failed job, submit a replacement under a new key derived from the holder's id. A UUID hash keeps the replacement key within 36 characters:

```java
scheduler.findIdempotencyKeyHolder(key).ifPresent(holder -> {
    if (holder.retention() == IdempotencyKeyHolder.Retention.PURGED) {
        // The key is spent. Only holder.id() remains.
        return;
    }
    if (holder.status() == JobStatus.FAILED) {
        String replacementKey = UUID.nameUUIDFromBytes(
            ("retry:" + holder.id()).getBytes(StandardCharsets.UTF_8)).toString();
        scheduler.enqueue(() -> reportService.generateDailyReport())
            .withIdempotencyKey(replacementKey)
            .submit();
    }
});
```

This is a point-in-time read, not a lock; a later submit can still race. No per-job authorization check is performed, matching `submit()`. Found, missing, and invalid-key results do not mark a caller transaction rollback-only. Store failures can.

SQL stores join the caller transaction and see its own uncommitted submissions. MongoDB reads committed data and does not join it. On MySQL REPEATABLE READ, the first plain read fixes the snapshot; later commits and state changes may be invisible. Read outside a transaction or use READ COMMITTED for the latest state.

## Event listener management

### addEventListener

```java
void addEventListener(Consumer<Object> listener)
```

Registers a programmatic event listener that receives all scheduler events in any runtime. For type-specific event observation, use CDI `@Observes` on Jakarta EE, Spring's `@EventListener` in Spring Boot, or Micronaut's `@EventListener` in Micronaut.

**Parameters:**
- `listener` -- a consumer that receives all scheduler events. Events are instances of classes in the `run.ratchet.api.event` package.

```java
scheduler.addEventListener(event -> {
    if (event instanceof JobFailedEvent failed) {
        metrics.counter("jobs.failed").increment();
        log.error("Job {} failed: {}", failed.getJobId(), failed.getErrorMessage());
    } else if (event instanceof JobCompletedEvent completed) {
        metrics.timer("jobs.duration")
            .record(completed.getExecutionTimeMs(), TimeUnit.MILLISECONDS);
    }
});
```

### removeEventListener

```java
void removeEventListener(Consumer<Object> listener)
```

Removes a previously registered event listener.

**Parameters:**
- `listener` -- the listener to remove (must be the same instance passed to `addEventListener`).

```java
Consumer<Object> listener = event -> { /* ... */ };
scheduler.addEventListener(listener);
// Later...
scheduler.removeEventListener(listener);
```

## JobHandle {#jobhandle}

`JobHandle` is the return type of all submission methods. It is a `@FunctionalInterface` with a single method:

```java
@FunctionalInterface
public interface JobHandle {
    UUID id();
}
```

The ID is a `java.util.UUID` UUIDv7 value, globally unique within the scheduler, and remains valid throughout the job's lifecycle.

```java
JobHandle handle = scheduler.enqueue(() -> processData()).submit();
UUID jobId = handle.id();

// Use the ID for lifecycle operations
scheduler.cancelJob(jobId);
scheduler.pauseJob(jobId);
scheduler.retryJob(jobId);
```

## RecurringJobBuilder

Returned by `scheduleRecurring()`. The configuration methods documented below set options, tags, and the business key:

### withOptions

```java
RecurringJobBuilder withOptions(JobOptions options)
```

Configures execution options (retry, timeout, priority, backoff) for the recurring job.

### withTags

```java
RecurringJobBuilder withTags(List<String> tags)
```

Associates tags with the job for filtering and categorization.

### withBusinessKey

```java
RecurringJobBuilder withBusinessKey(String key)
```

Sets the business key for active-unique identity. While the job is active (PENDING, RUNNING, PAUSED), no other job may share the same business key.

After trimming, a business key may contain up to 255 printable ASCII characters (`U+0020` through
`U+007E`). Ratchet rejects longer or non-ASCII keys instead of leaving a store to truncate or
convert them.

### submit

```java
JobHandle submit()
```

Finalizes configuration and submits the recurring job to the scheduler.

## See also

- [JobBuilder Reference](./job-builder)
- [BatchBuilder Reference](./batch-builder)
- [Event System](./event-system)
- [Job Lifecycle](/concepts/job-lifecycle)

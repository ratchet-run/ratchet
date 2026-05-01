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

## Scheduling Methods

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

Enqueues a task for immediate execution with default configuration. This is a convenience method equivalent to `enqueue(task).submit()`.

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

### recurring

```java
RecurringJobBuilder scheduleRecurring(
    String cron, ZoneId zone, SerializableCheckedRunnable task)
```

Schedules a recurring job based on a cron expression. Returns a `RecurringJobBuilder` for configuring options and tags.

**Parameters:**
- `cron` -- Quartz cron expression (6-7 fields: `second minute hour day-of-month month day-of-week [year]`).
- `zone` -- timezone for evaluating the cron expression.
- `task` -- the job task to execute on each occurrence.

**Returns:** a `RecurringJobBuilder` for configuring options, tags, and business key.

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
    .submit();
```

## Batch Methods

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

## Job Replacement

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

## Job Control Methods

### cancelJob

```java
boolean cancelJob(UUID jobId)
```

Cancels a job by its ID.

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

- Only **PENDING** or **FAILED** jobs can be paused.
- The job's previous status is recorded so it can be restored on resume.
- **RUNNING** jobs cannot be paused (cancel them instead).
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

- The job returns to the status it had before being paused.
- Resuming a previously **PENDING** job makes it eligible for polling again.
- Resuming a previously **FAILED** job restores it to FAILED without retrying.
- **Idempotent:** resuming a non-paused job returns `false` without error.

**Parameters:**
- `jobId` -- the ID of the job to resume.

**Returns:** `true` if the job was resumed; `false` if the job was not found or not in PAUSED state.

```java
boolean resumed = scheduler.resumeJob(jobId);
```

### retryJob

```java
boolean retryJob(UUID jobId)
```

Retries a failed job by resetting it to PENDING status. This is the primary mechanism for manual retry of jobs in the Dead Letter Queue.

- The attempt counter is reset to 0.
- Error information is cleared.
- Scheduled time is set to now, making the job immediately eligible for execution.
- Only **FAILED** jobs can be retried.

**Parameters:**
- `jobId` -- the ID of the failed job to retry.

**Returns:** `true` if the job was successfully reset to PENDING; `false` if not found or not FAILED.

```java
// Manual DLQ retry
boolean retried = scheduler.retryJob(failedJobId);
if (retried) {
    log.info("Job {} re-queued for execution", failedJobId);
}
```

## Recurring Job Management

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

## Event Listener Management

### addEventListener

```java
void addEventListener(Consumer<Object> listener)
```

Registers a programmatic event listener that receives all scheduler events. For type-safe event observation, use CDI `@Observes` with specific event types instead. This method is intended for non-CDI contexts or when receiving all events is desired.

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

Returned by `recurring()`, provides three configuration methods:

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

### submit

```java
JobHandle submit()
```

Finalizes configuration and submits the recurring job to the scheduler.

## See Also

- [JobBuilder Reference](./job-builder)
- [BatchBuilder Reference](./batch-builder)
- [Event System](./event-system)
- [Job Lifecycle](/docs/concepts/job-lifecycle)

---
sidebar_position: 6
title: BatchBuilder Reference
description: Complete reference for BatchBuilder and StreamingBatchBuilder for parallel and streaming batch job processing.
---

# BatchBuilder

Builders for processing collections of items as coordinated batch jobs. Ratchet provides two batch builders:

- **`BatchBuilder`** -- for in-memory collections where the total size is known up front.
- **`StreamingBatchBuilder<T>`** -- for large datasets read from a `Stream`, processed in chunks without loading everything into memory.

**Package:** `run.ratchet.api`

## BatchBuilder

Obtained from [`JobSchedulerService.enqueueBatch()`](./job-scheduler-service#enqueuebatch).

### forEach

```java
<T extends Serializable> BatchBuilder forEach(
    Collection<T> items, SerializableConsumer<T> action)
```

Applies an action to each item in the collection. Each item becomes a child job in the batch.

**Type Parameters:**
- `T` -- the type of elements; must implement `Serializable`.

**Parameters:**
- `items` -- the collection of items to process.
- `action` -- the operation to perform on each item.

**Returns:** the `BatchBuilder` for chaining.

```java
List<Long> orderIds = List.of(1L, 2L, 3L, 4L, 5L);

scheduler.enqueueBatch("Process Orders")
    .forEach(orderIds, orderId -> orderService.process(orderId))
    .submit();
```

### onProgress

```java
BatchBuilder onProgress(SerializableConsumer<BatchContext> hook)
```

Registers a progress hook invoked as child jobs complete. The hook receives a [`BatchContext`](./batch-context) snapshot with current progress metrics.

**Parameters:**
- `hook` -- a consumer receiving `BatchContext` updates.

**Returns:** the `BatchBuilder` for chaining.

```java
scheduler.enqueueBatch("Import Records")
    .forEach(records, record -> importService.importRecord(record))
    .onProgress(ctx -> {
        log.info("Batch {}: {}% complete ({}/{} items, {} failed)",
            ctx.batchId(), ctx.percentDone(),
            ctx.completedItems(), ctx.totalItems(), ctx.failedItems());
    })
    .submit();
```

### thenOnBatchSuccess

```java
BatchBuilder thenOnBatchSuccess(SerializableCheckedRunnable next)
```

Schedules a job to execute when **all** child jobs complete successfully (zero failures).

**Parameters:**
- `next` -- the task to execute on batch success.

```java
scheduler.enqueueBatch("Nightly Sync")
    .forEach(accounts, acct -> syncService.sync(acct))
    .thenOnBatchSuccess(() -> notificationService.sendSyncComplete())
    .submit();
```

### thenOnBatchFailure

```java
BatchBuilder thenOnBatchFailure(SerializableCheckedRunnable next)
```

Schedules a job to execute when one or more child jobs fail.

**Parameters:**
- `next` -- the task to execute on batch failure.

```java
scheduler.enqueueBatch("Data Migration")
    .forEach(rows, row -> migrationService.migrate(row))
    .thenOnBatchFailure(() -> alertService.migrationPartiallyFailed())
    .submit();
```

### thenWhenBatch

```java
BatchBuilder thenWhenBatch(
    SerializablePredicate<BatchContext> condition,
    SerializableCheckedRunnable next)
```

Schedules a job when a custom condition on the `BatchContext` is met.

**Parameters:**
- `condition` -- predicate evaluating the `BatchContext`.
- `next` -- the task to execute when the condition is true.

```java
public final class BatchWorkflowConditions {
    public static boolean isCompleteWithoutFailures(BatchContext ctx) {
        return ctx.failedItems() == 0 && ctx.isComplete();
    }

    public static boolean shouldRollback(BatchContext ctx) {
        return ctx.failedItems() > ctx.totalItems() / 2;
    }
}

scheduler.enqueueBatch("Process Items")
    .forEach(items, item -> processItem(item))
    .thenWhenBatch(BatchWorkflowConditions::isCompleteWithoutFailures,
                   () -> archiveResults())
    .thenWhenBatch(BatchWorkflowConditions::shouldRollback,
                   () -> rollbackProcessing())
    .submit();
```

Custom batch predicates are analyzed into `JobPayload` JSON at submission time. Put compound logic in a public helper or CDI bean method, then pass a method reference.

### thenWhenSuccessRate

```java
BatchBuilder thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next)
```

Schedules a job when the success rate meets or exceeds the specified threshold.

**Parameters:**
- `minRate` -- minimum success rate (0.0 to 1.0).
- `next` -- the task to execute when the success rate condition is met.

```java
scheduler.enqueueBatch("Email Campaign")
    .forEach(recipients, r -> emailService.sendCampaign(r))
    .thenWhenSuccessRate(0.95, () -> log.info("Campaign delivered successfully"))
    .thenWhenSuccessRate(0.50, () -> alertService.campaignPartialFailure())
    .submit();
```

### thenWhenFailureCount

```java
BatchBuilder thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next)
```

Schedules a job when the number of failures reaches the specified threshold.

**Parameters:**
- `maxFailures` -- the failure count that triggers the action.
- `next` -- the task to execute when the failure count is reached.

```java
scheduler.enqueueBatch("Import Data")
    .forEach(rows, row -> importRow(row))
    .thenWhenFailureCount(10, () -> alertService.tooManyImportFailures())
    .submit();
```

### thenBranch

```java
BatchBuilder thenBranch(WorkflowCondition condition,
                        SerializableCheckedRunnable next,
                        String description)
```

Adds a workflow branch with an explicit `WorkflowCondition` and description.

**Parameters:**
- `condition` -- the `WorkflowCondition` determining when this branch fires.
- `next` -- the task to execute.
- `description` -- human-readable description for monitoring.

```java
public final class BatchWorkflowConditions {
    public static boolean hasMoreThanFiveFailures(BatchContext ctx) {
        return ctx.failedItems() > 5 && ctx.isComplete();
    }
}

scheduler.enqueueBatch("Complex Batch")
    .forEach(items, item -> processItem(item))
    .thenBranch(
        WorkflowCondition.batchCustom(BatchWorkflowConditions::hasMoreThanFiveFailures),
        () -> escalateToOps(),
        "Escalate when more than 5 items fail")
    .submit();
```

### submit

```java
JobHandle submit()
```

Submits the configured batch for execution.

**Returns:** a `JobHandle` for the batch parent job.

```java
JobHandle handle = scheduler.enqueueBatch("My Batch")
    .forEach(items, item -> processItem(item))
    .submit();

log.info("Batch submitted with ID {}", handle.id());
```

## StreamingBatchBuilder {/* #streamingbatchbuilder */}

Obtained from [`JobSchedulerService.streamingBatch()`](./job-scheduler-service#streamingbatch). Designed for large datasets where items are read from a `Stream` and inserted in configurable chunks.

### fromStream

```java
<U extends Serializable> StreamingBatchBuilder<U> fromStream(Stream<U> stream)
```

Sets the input data source for the batch.

**Type Parameters:**
- `U` -- the type of items; must implement `Serializable`.

**Parameters:**
- `stream` -- the stream of items to process.

```java
scheduler.<Long>streamingBatch("Process Users")
    .fromStream(userRepository.streamAllIds())
    // ...
```

### process

```java
StreamingBatchBuilder<T> process(SerializableCheckedConsumer<T> action)
```

Configures the processing logic applied to each item. The action can throw checked exceptions.

**Parameters:**
- `action` -- the processing action for each item.

```java
scheduler.<Long>streamingBatch("Migrate Users")
    .fromStream(userIds.stream())
    .process(userId -> migrationService.migrateUser(userId))
    // ...
```

### withChunkSize

```java
StreamingBatchBuilder<T> withChunkSize(int size)
```

Sets the number of items per database insert chunk. Default is 500. Tune this based on your database's bulk insert performance.

**Parameters:**
- `size` -- items per chunk. Must be positive.

```java
scheduler.<Record>streamingBatch("Bulk Import")
    .fromStream(records.stream())
    .process(record -> importRecord(record))
    .withChunkSize(2000)
    // ...
```

### onProgress

```java
StreamingBatchBuilder<T> onProgress(Consumer<StreamingBatchContext> hook)
```

Registers a progress hook called during the **stream consumption phase** (not during job execution). Receives a [`StreamingBatchContext`](./batch-context#streamingbatchcontext) with streaming progress.

**Parameters:**
- `hook` -- a consumer receiving `StreamingBatchContext` updates.

```java
scheduler.<Long>streamingBatch("Stream Import")
    .fromStream(dataStream)
    .process(item -> processItem(item))
    .onProgress(ctx -> log.info("Streamed {} items in {} chunks",
        ctx.processedItems(), ctx.chunksInserted()))
    // ...
```

### onBatchProgress

```java
StreamingBatchBuilder<T> onBatchProgress(SerializableConsumer<BatchContext> hook)
```

Registers a progress hook called during **job execution** (after streaming is complete). Receives a `BatchContext` with execution progress.

**Parameters:**
- `hook` -- a consumer receiving `BatchContext` updates.

```java
scheduler.<Long>streamingBatch("Process Stream")
    .fromStream(ids.stream())
    .process(id -> processId(id))
    .onBatchProgress(ctx -> log.info("Execution: {}% complete", ctx.percentDone()))
    // ...
```

### Workflow Methods

`StreamingBatchBuilder` supports the same workflow methods as `BatchBuilder`:

```java
StreamingBatchBuilder<T> thenOnBatchSuccess(SerializableCheckedRunnable next)
StreamingBatchBuilder<T> thenOnBatchFailure(SerializableCheckedRunnable next)
StreamingBatchBuilder<T> thenWhenBatch(
    SerializablePredicate<BatchContext> condition, SerializableCheckedRunnable next)
StreamingBatchBuilder<T> thenWhenFailureCount(int maxFailures, SerializableCheckedRunnable next)
StreamingBatchBuilder<T> thenWhenSuccessRate(double minRate, SerializableCheckedRunnable next)
```

### start

```java
JobHandle start()
```

Starts the streaming batch operation. The stream is consumed, items are inserted in chunks, and child jobs are created.

**Returns:** a `JobHandle` for the batch parent job.

```java
JobHandle handle = scheduler.<Long>streamingBatch("Full Migration")
    .fromStream(repository.streamAll())
    .process(id -> migrationService.migrate(id))
    .withChunkSize(1000)
    .onProgress(ctx -> log.info("Streamed {} items", ctx.processedItems()))
    .thenOnBatchSuccess(() -> log.info("Migration complete"))
    .thenOnBatchFailure(() -> alertService.migrationFailed())
    .start();
```

## BatchBuilder vs StreamingBatchBuilder

| Aspect | BatchBuilder | StreamingBatchBuilder |
|---|---|---|
| Input | `Collection<T>` (in memory) | `Stream<T>` (lazy) |
| Memory | Entire collection loaded | Chunked, constant memory |
| Total known at start | Yes | No (stream not exhausted) |
| Progress during creation | N/A | `StreamingBatchContext` |
| Progress during execution | `BatchContext` | `BatchContext` |
| Best for | Small-medium collections | Large datasets, database cursors |
| Submit method | `submit()` | `start()` |

## See Also

- [BatchContext Reference](./batch-context)
- [WorkflowCondition Reference](./workflow-condition)
- [JobSchedulerService Reference](./job-scheduler-service)

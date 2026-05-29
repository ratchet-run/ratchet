---
sidebar_position: 7
title: BatchContext Reference
description: Reference for BatchContext and StreamingBatchContext records used for batch progress monitoring.
---

# BatchContext Reference

Immutable context records providing progress information during batch operations. Ratchet provides two context types:

- **`BatchContext`** -- progress during batch **job execution** (how many items have completed/failed).
- **`StreamingBatchContext`** -- progress during the **stream consumption phase** (how many items have been read and chunked).

**Package:** `run.ratchet.api`

## BatchContext

A Java record representing the state and progress of a batch operation. Passed to progress hooks and workflow conditions during batch execution.

```java
public record BatchContext(
    UUID batchId,
    int totalItems,
    int completedItems,
    int failedItems
)
```

### Record Components

| Component | Type | Description |
|---|---|---|
| `batchId` | `UUID` | UUIDv7 identifier of the batch parent job |
| `totalItems` | `int` | Total number of child jobs in the batch |
| `completedItems` | `int` | Number of child jobs completed successfully |
| `failedItems` | `int` | Number of child jobs that have failed |

### Computed Methods

#### isComplete

```java
public boolean isComplete()
```

Returns `true` when all items have been processed (completed + failed >= total).

```java
if (ctx.isComplete()) {
    log.info("Batch {} is done", ctx.batchId());
}
```

#### percentDone

```java
public int percentDone()
```

Calculates the completion percentage as `(completedItems * 100) / totalItems`. Returns 100 for empty batches.

**Returns:** an integer between 0 and 100.

```java
int pct = ctx.percentDone();
log.info("Batch progress: {}%", pct);
```

#### successRate

```java
public double successRate()
```

Calculates the success rate as `completedItems / (completedItems + failedItems)`. Returns 1.0 if no items have been processed yet.

**Returns:** a double between 0.0 and 1.0.

```java
if (ctx.successRate() < 0.9) {
    log.warn("Batch {} success rate below 90%", ctx.batchId());
}
```

### Usage in Progress Hooks

```java
scheduler.enqueueBatch("Process Orders")
    .forEach(orders, order -> processOrder(order))
    .onProgress(ctx -> {
        log.info("Batch {} progress: {}/{} done, {} failed, {}% complete",
            ctx.batchId(),
            ctx.completedItems(),
            ctx.totalItems(),
            ctx.failedItems(),
            ctx.percentDone());

        if (ctx.successRate() < 0.5 && ctx.completedItems() + ctx.failedItems() > 10) {
            log.warn("Success rate dropped below 50%");
        }
    })
    .submit();
```

### Usage in Workflow Conditions

```java
public final class ImportBatchConditions {
    public static boolean isPerfectBatch(BatchContext ctx) {
        return ctx.failedItems() == 0 && ctx.isComplete();
    }

    public static boolean hasAcceptableSuccessRate(BatchContext ctx) {
        return ctx.successRate() >= 0.95 && ctx.isComplete();
    }

    public static boolean hasMajorityFailures(BatchContext ctx) {
        return ctx.failedItems() > ctx.totalItems() / 2;
    }
}

scheduler.enqueueBatch("Data Import")
    .forEach(rows, row -> importRow(row))
    // Perfect batch -- zero failures
    .thenWhenBatch(ImportBatchConditions::isPerfectBatch,
                   () -> markImportComplete())
    // Acceptable -- at least 95% success
    .thenWhenBatch(ImportBatchConditions::hasAcceptableSuccessRate,
                   () -> acceptWithWarnings())
    // Unacceptable -- more than half failed
    .thenWhenBatch(ImportBatchConditions::hasMajorityFailures,
                   () -> rollbackImport())
    .submit();
```

## StreamingBatchContext {#streamingbatchcontext}

A Java record providing progress information during the **streaming phase** -- when items are being read from the source stream and inserted as child jobs in chunks.

```java
public record StreamingBatchContext(
    UUID batchId,
    int processedItems,
    int chunksInserted
)
```

### Record Components

| Component | Type | Description |
|---|---|---|
| `batchId` | `UUID` | UUIDv7 identifier of the batch parent job being created |
| `processedItems` | `int` | Cumulative number of items read from the stream |
| `chunksInserted` | `int` | Number of bulk insert operations performed |

`StreamingBatchContext` is a bare record. Read its progress values directly through the canonical accessors `processedItems()` and `chunksInserted()`.

### Usage

```java
scheduler.<Long>streamingBatch("Migrate Users")
    .fromStream(userRepository.streamAllUserIds())
    .process(userId -> migrationService.migrateUser(userId))
    .withChunkSize(500)
    .onProgress(ctx -> {
        log.info("Streaming: {} items read, {} chunks inserted",
            ctx.processedItems(), ctx.chunksInserted());

        // Estimate database load
        int estimatedRows = ctx.chunksInserted() * 500;
        log.debug("Approximately {} child jobs created", estimatedRows);
    })
    .start();
```

## BatchContext vs StreamingBatchContext

| Aspect | BatchContext | StreamingBatchContext |
|---|---|---|
| **Phase** | Job execution | Stream consumption (job creation) |
| **Total known** | Yes (`totalItems`) | No (stream not yet exhausted) |
| **Tracks** | Completed + failed items | Items streamed + chunks inserted |
| **Success rate** | Available via `successRate()` | N/A (jobs haven't run yet) |
| **Completion** | `isComplete()` available | N/A |
| **Used in** | `onProgress()`, workflow conditions | `onProgress()` on `StreamingBatchBuilder` |

### Lifecycle

1. **Stream consumption** -- `StreamingBatchContext` is passed to `StreamingBatchBuilder.onProgress()` as items are read and chunked.
2. **Job execution** -- `BatchContext` is passed to `BatchBuilder.onProgress()` (or `StreamingBatchBuilder.onBatchProgress()`) as child jobs complete.

```java
scheduler.<Long>streamingBatch("Full Pipeline")
    .fromStream(largeDataset.stream())
    .process(item -> processItem(item))
    .withChunkSize(1000)
    // Phase 1: Stream consumption progress
    .onProgress(ctx -> log.info("Reading: {} items in {} chunks",
        ctx.processedItems(), ctx.chunksInserted()))
    // Phase 2: Execution progress
    .onBatchProgress(ctx -> log.info("Executing: {}% complete, {} failed",
        ctx.percentDone(), ctx.failedItems()))
    .start();
```

## See Also

- [BatchBuilder Reference](./batch-builder)
- [WorkflowCondition Reference](./workflow-condition)
- [JobSchedulerService Reference](./job-scheduler-service)

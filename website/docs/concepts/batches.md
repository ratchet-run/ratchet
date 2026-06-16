---
sidebar_position: 8
title: Batches
description: BatchBuilder for in-memory collections and StreamingBatchBuilder for large datasets
---

# Batches

Ratchet provides two batch processing APIs: `BatchBuilder` for in-memory collections and `StreamingBatchBuilder` for large datasets that shouldn't be loaded entirely into memory.

## How Batches Work

A batch consists of a **parent job** (BATCH_PARENT) and many **child jobs** (BATCH_CHILD). The parent tracks overall progress but performs no work itself. Each child executes independently and in parallel, following the normal job lifecycle with its own retry and failure handling.

<div class="docs-diagram" role="img" aria-label="Batch execution flow: submit creates a parent job, the parent creates child jobs, children execute independently, then parent evaluates success or failure callbacks.">
  <div class="docs-diagram-flow">
    <div class="docs-diagram-card docs-diagram-card--primary">
      <strong>`BatchBuilder.submit()`</strong>
      <small>Persists the batch parent and child job definitions.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--muted">
      <strong>BATCH_PARENT</strong>
      <small>Tracks progress and owns callbacks, but performs no user work.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--active">
      <strong>N child jobs</strong>
      <small>Each child is a normal job with independent retry and failure handling.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--store">
      <strong>Progress aggregation</strong>
      <small>Child completions update batch metrics and parent progress.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--success">
      <strong>Batch callbacks</strong>
      <small>`onBatchSuccess` runs if all pass; `onBatchFailure` runs if any fail.</small>
    </div>
  </div>
</div>

## BatchBuilder -- In-Memory Collections

Use `BatchBuilder` when the entire collection fits in memory:

```java
List<User> users = userRepository.findAllPending();

scheduler.enqueueBatch("Import Users")
    .forEach(users, user -> userService.importUser(user))
    .submit();
```

### Progress Monitoring

Track batch progress with a callback:

```java
scheduler.enqueueBatch("Process Orders")
    .forEach(orders, order -> orderService.process(order))
    .onProgress(ctx -> {
        log.info("Batch {} is {}% complete ({}/{} items, {} failures)",
            ctx.batchId(),
            ctx.percentDone(),
            ctx.completedItems(),
            ctx.totalItems(),
            ctx.failedItems());
    })
    .submit();
```

The `BatchContext` record provides:

| Method | Description |
|--------|-------------|
| `batchId()` | Unique identifier of the batch |
| `totalItems()` | Total number of child jobs |
| `completedItems()` | Successfully completed children |
| `failedItems()` | Failed children |
| `percentDone()` | Completion percentage (0-100) |
| `isComplete()` | Whether all items have been processed |
| `successRate()` | Ratio of successes to total processed (0.0-1.0) |

### Batch Completion Handlers

React to the batch finishing:

```java
scheduler.enqueueBatch("Nightly Sync")
    .forEach(accounts, account -> syncService.sync(account))
    .thenOnBatchSuccess(() -> notifyService.sendSyncComplete())
    .thenOnBatchFailure(() -> alertService.sendSyncFailed())
    .submit();
```

### Conditional Workflows on Batches

Use batch-level conditions for more nuanced reactions:

```java
public final class MigrationBatchConditions {
    public static boolean needsPartialRecovery(BatchContext ctx) {
        return ctx.failedItems() > 0 && ctx.successRate() > 0.8;
    }

    public static boolean isLargeBatch(BatchContext ctx) {
        return ctx.completedItems() > 1_000;
    }
}

scheduler.enqueueBatch("Data Migration")
    .forEach(records, record -> migrationService.migrate(record))

    // Fire when success rate is above 95%
    .thenWhenSuccessRate(0.95, () -> reportService.generateSuccessReport())

    // Fire when more than 10 items fail
    .thenWhenFailureCount(10, () -> alertService.escalateToOps())

    // Custom condition based on BatchContext
    .thenWhenBatch(
        MigrationBatchConditions::needsPartialRecovery,
        () -> partialRecoveryService.run())

    // Full workflow branch with description
    .thenBranch(
        WorkflowCondition.batchCustom(MigrationBatchConditions::isLargeBatch),
        () -> analyticsService.recordLargeBatch(),
        "Track large batch completions")

    .submit();
```

### Batch API Summary

| Method | Description |
|--------|-------------|
| `forEach(items, action)` | Process each item in the collection |
| `onProgress(hook)` | Track progress during execution |
| `thenOnBatchSuccess(task)` | Execute on 100% success |
| `thenOnBatchFailure(task)` | Execute on any failure |
| `thenWhenSuccessRate(rate, task)` | Execute when success rate meets threshold |
| `thenWhenFailureCount(max, task)` | Execute when failure count reaches threshold |
| `thenWhenBatch(predicate, task)` | Execute when custom BatchContext condition is met |
| `thenBranch(condition, task, desc)` | Full workflow branch with WorkflowCondition |
| `submit()` | Submit the batch for execution |

## StreamingBatchBuilder -- Large Datasets

When your dataset is too large to load into memory, use `StreamingBatchBuilder`. It consumes a `Stream<T>` in chunks, creating child jobs as it goes:

```java
scheduler.<User>streamingBatch("Process All Users")
    .fromStream(userRepository.streamAll())
    .process(user -> userService.processUser(user))
    .withChunkSize(500)
    .start();
```

### How Streaming Works

1. The stream is consumed in chunks of `chunkSize` items (default: 100)
2. Each chunk creates a batch of child jobs via a bulk insert
3. The parent BATCH_PARENT job is created to track overall progress
4. Streaming continues until the stream is exhausted
5. Child jobs execute in parallel as they're created (no need to wait for streaming to finish)

This means a million-row result set never needs to be held in memory -- it's processed in 100-item chunks.

### Streaming Progress

`StreamingBatchContext` tracks progress during the stream consumption phase (not during job execution):

```java
scheduler.<Transaction>streamingBatch("Reconcile Transactions")
    .fromStream(transactionDao.streamUnreconciled())
    .process(tx -> reconciliationService.reconcile(tx))
    .withChunkSize(1000)
    .onProgress(ctx -> {
        log.info("Streamed {} items in {} chunks",
            ctx.processedItems(), ctx.chunksInserted());
    })
    .onBatchProgress(ctx -> {
        log.info("Batch {} is {}% complete",
            ctx.batchId(), ctx.percentDone());
    })
    .start();
```

| Context | Phase | Total Known? |
|---------|-------|-------------|
| `StreamingBatchContext` | Stream consumption (job creation) | No |
| `BatchContext` | Job execution | Yes |

### Streaming Completion Handlers

The same batch-level handlers work with streaming batches:

```java
scheduler.<Record>streamingBatch("ETL Pipeline")
    .fromStream(sourceDb.streamRecords())
    .process(record -> etlService.transform(record))
    .withChunkSize(200)
    .thenOnBatchSuccess(() -> etlService.markComplete())
    .thenOnBatchFailure(() -> etlService.rollback())
    .thenWhenSuccessRate(0.99, () -> qualityService.certify())
    .start();
```

### Streaming API Summary

| Method | Description |
|--------|-------------|
| `fromStream(stream)` | Set the input stream |
| `process(action)` | Define per-item processing logic |
| `withChunkSize(size)` | Items per bulk insert (default: 100) |
| `onProgress(hook)` | Track streaming progress |
| `onBatchProgress(hook)` | Track batch execution progress |
| `thenOnBatchSuccess(task)` | Execute on 100% success |
| `thenOnBatchFailure(task)` | Execute on any failure |
| `thenWhenSuccessRate(rate, task)` | Execute when success rate meets threshold |
| `thenWhenFailureCount(max, task)` | Execute when failure count reaches threshold |
| `thenWhenBatch(predicate, task)` | Execute when custom condition is met |
| `start()` | Begin streaming and job creation |

## Batch vs Streaming: When to Use Which

| Factor | BatchBuilder | StreamingBatchBuilder |
|--------|-------------|----------------------|
| Dataset size | Fits in memory | Any size |
| Input type | `Collection<T>` | `Stream<T>` |
| Total known upfront | Yes | No (stream not exhausted) |
| Memory usage | Proportional to collection size | Constant (chunk size) |
| Child creation | All at once | In chunks |
| Submit method | `submit()` | `start()` |

**Rule of thumb:** If you can call `findAll()` without worrying about memory, use `BatchBuilder`. If you need `streamAll()` or are reading from a file/external source, use `StreamingBatchBuilder`.

## Batch Recovery

Two timers keep batches moving after a crash. The general crash-recovery scan resets orphaned batch children: any child stuck in RUNNING on a node whose heartbeat has gone stale is reset to PENDING and re-executed.

The `BatchRecoveryTimer` handles a different failure mode. A batch can finish all of its children, yet the parent's completion flag is never set because the marking transaction was lost to a crash, network partition, or rollback. The timer scans every 15 minutes for these stuck-but-complete batches, sets the completion flag, and fires the parent's completion handlers and any conditional branches.

The parent job is never "stuck" indefinitely: it tracks progress based on child status counts, so when orphaned children are recovered and re-executed, progress catches up, and the recovery timer closes out batches whose completion was never recorded.

## Related

- [Job Types](./job-types.md) -- BATCH_PARENT and BATCH_CHILD execution types
- [Workflows](./workflows.md) -- Workflow conditions including batch-specific conditions
- [Error Handling](./error-handling.md) -- How individual child failures are handled

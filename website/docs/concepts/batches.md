---
sidebar_position: 8
title: Batches
description: BatchBuilder for in-memory collections and StreamingBatchBuilder for large datasets
---

# Batches

Ratchet provides two batch processing APIs: `BatchBuilder` for in-memory collections and `StreamingBatchBuilder` for large datasets that shouldn't be loaded entirely into memory.

## Retrying failed children

You can retry a failed child while its batch is still running. The retry updates the batch's
failure counter so later completion counts the child once. Parents and children of a
finished batch cannot be retried. `retryJob` returns `false` for them. `retryJobs` skips them.
Cancelling a child counts as a failure. The batch completes as soon as its last child ends.

## How Batches Work

A batch consists of a **parent job** (BATCH_PARENT) and many **child jobs** (BATCH_CHILD). The parent tracks overall progress but performs no work itself. Each child executes independently and in parallel, following the normal job lifecycle. Retries are configured for all children on the builder; the default remains no retries.

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
      <small>Each child has independent failure handling and the retry settings from the builder.</small>
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

An empty collection or stream completes as succeeded during submission. Its workflow branches are routed as for a batch with a 100% success rate and zero failures. Routing is exclusive, so the first matching branch runs and every other branch is canceled.

## Deduplicating Batch Submissions

Use a stable key when retrying a batch submission:

```java
JobHandle handle = scheduler.enqueueBatch("Import Users")
    .withIdempotencyKey("users-import-2026-11-03")
    .forEach(users, user -> userService.importUser(user))
    .submit();
```

The key belongs to the batch parent. A later submission with the same key returns the original handle. It writes no parent, children, progress hook or branches, even after job history is purged. Children and branch jobs keep generated keys. Single jobs and batches share the key namespace.

Keys are trimmed. A key longer than 36 characters after trimming throws `IllegalArgumentException` from `withIdempotencyKey`. Null, or a key that is empty after trimming, keeps the generated key.

Streaming batches support the same method before `start()`. On a duplicate, the stream is closed without being read. `start()` closes the stream whether it returns or throws. Both invocation builders support it too.

If two submissions race on the same key, the loser may throw. Retry in a new transaction to get the original handle. Execution remains at least once.

With the MongoDB store, the writes of a submission do not join the caller's transaction. If `submit()` or `start()` throws after the parent was written, the parent and its key stay. A retry with the same key then returns that parent's handle and writes nothing, so a batch that was only partly written stays that way. A single job keeps its key the same way when its tags, chain steps or branches fail to save.

## BatchBuilder -- In-Memory Collections

Use `BatchBuilder` when the entire collection fits in memory:

```java
List<User> users = userRepository.findAllPending();

scheduler.enqueueBatch("Import Users")
    .forEach(users, user -> userService.importUser(user))
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
    .submit();
```

### Child Retries

Retries are opt-in and apply to every child in the batch. `withMaxRetries(3)` allows three attempts after the initial failure. `withTimeout(duration)` sets an execution timeout for every child in the same way; without it, children use the global default SLA. The parent and batch-level workflow branches do not inherit these settings.

Builder order does not change the result. You can set the retry options before or after `forEach`; they apply to the whole batch.

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
| `withIdempotencyKey(key)` | Deduplicate submissions using the batch parent key |
| `withMaxRetries(retries)` | Set child retry attempts (default: 0) |
| `withBackoff(policy, delay)` | Set the child retry backoff policy |
| `withTimeout(duration)` | Set the child execution timeout (default: global default SLA) |
| `withResource(name)` | Share named resource permits across all children and other jobs using that name |
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
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
    .start();
```

### How Streaming Works

1. The stream is consumed in chunks of `chunkSize` items (default: 100)
2. Each chunk creates a batch of child jobs via a bulk insert
3. The parent BATCH_PARENT job is created to track overall progress
4. Streaming continues until the stream is exhausted
5. Child jobs execute in parallel as they're created (no need to wait for streaming to finish)

This means a million-row result set is never held in memory; it's processed in 100-item chunks.

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
| `withIdempotencyKey(key)` | Deduplicate submissions using the batch parent key |
| `withMaxRetries(retries)` | Set child retry attempts (default: 0) |
| `withBackoff(policy, delay)` | Set the child retry backoff policy |
| `withTimeout(duration)` | Set the child execution timeout (default: global default SLA) |
| `withResource(name)` | Share named resource permits across all children and other jobs using that name |
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

## Control a running batch

Pass the batch parent id to the existing scheduler methods:

```java
scheduler.cancelJob(batch.id());
scheduler.pauseJob(batch.id());
scheduler.resumeJob(batch.id());
scheduler.retryJob(batch.id());
```

`cancelJob` cancels PENDING, PAUSED and WAITING children and unstarted callbacks,
including the chain steps below those callbacks. A RUNNING child finishes its current
attempt. If that attempt fails, it is not retried. No child starts a new attempt after
the cancel. No batch callback runs after cancellation.

`pauseJob` pauses PENDING children. RUNNING children finish and count toward the batch.
Callbacks stay locked, and the parent cannot complete while paused. This pauses the
children that are pending at that moment. A running child's automatic retry, or a
retry that races the pause, may become PENDING and run during the pause.

`resumeJob` restores all PAUSED children to their previous status. This includes
children paused one by one before the batch pause. It also checks whether the batch
finished while paused, so completion and callbacks can proceed immediately.

`retryJob` on an open parent retries all FAILED children. It returns `true` if at
least one child was reset. The parent must be PENDING or PAUSED and completion must
not have been processed. Finished batches cannot be reopened. Retried children of
a paused batch come back PAUSED and run when the batch resumes.

Use `retryJobs` to select children by additional filters. Each call resets at most
1000 children:

```java
scheduler.retryJobs(
    JobFilter.builder().parentJobId(batch.id()).types(JobType.BATCH).build(), 1000);
```

On MongoDB, batch submission and control update children in separate transactions,
one per chunk. Children submitted after a control operation has passed their chunk may
be missed. If a batch pause or resume is interrupted, call it again to finish the
remaining children. Recovery finishes an interrupted cancellation of an open batch with
a CANCELED parent.

## Batch Recovery

Two timers keep batches moving after a crash. The general crash-recovery scan resets orphaned batch children: any child stuck in RUNNING on a node whose heartbeat has gone stale is reset to PENDING and re-executed.

The `BatchRecoveryTimer` handles a different failure mode. A batch can finish all of its children, yet the parent's completion flag is never set because the marking transaction was lost to a crash, network partition, or rollback. The timer scans every 15 minutes for these stuck-but-complete batches, sets the completion flag, and fires the parent's completion handlers and any conditional branches.

The parent job is never "stuck" indefinitely: it tracks progress based on child status counts, so when orphaned children are recovered and re-executed, progress catches up, and the recovery timer closes out batches whose completion was never recorded.

## Related

- [Job Types](./job-types.md) -- BATCH_PARENT and BATCH_CHILD execution types
- [Workflows](./workflows.md) -- Workflow conditions including batch-specific conditions
- [Error Handling](./error-handling.md) -- How individual child failures are handled

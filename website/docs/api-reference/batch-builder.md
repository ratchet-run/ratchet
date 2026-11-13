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

### withIdempotencyKey

```java
BatchBuilder withIdempotencyKey(String key)
```

Sets the batch parent's idempotency key. The key is trimmed and must be at most 36 characters. A longer trimmed key throws `IllegalArgumentException` from this method. Null or blank keeps the generated key.

A later `submit()` with the same key returns the original handle and writes no parent, children, progress hook or branches. This also works after job history is purged. Single jobs and batches share the key namespace. Children and branch jobs keep generated keys.

If two submissions race on the same key, the loser may throw. Retry in a new transaction to get the original handle. Execution remains at least once.

**Returns:** the `BatchBuilder` for chaining.

```java
scheduler.enqueueBatch("Import Users")
    .withIdempotencyKey("users-import-2026-11-03")
    .forEach(users, user -> userService.importUser(user))
    .submit();
```

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
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
    .submit();
```

### withMaxRetries

```java
BatchBuilder withMaxRetries(int retries)
```

Sets the number of retry attempts for every child job. The default is `0`, so a child runs once unless retries are explicitly enabled. A negative value throws `IllegalArgumentException`.

The setting belongs to the whole builder and applies even when called after `forEach`. It does not apply to the no-op batch parent or workflow branches.

### withBackoff

```java
BatchBuilder withBackoff(BackoffPolicy policy, Duration param)
```

Sets the backoff policy and base delay used between child retry attempts. Both arguments are required. This does not enable retries by itself; set `withMaxRetries` to a positive value as well. The setting applies to the whole builder, regardless of call order.

```java
scheduler.enqueueBatch("Process Orders")
    .forEach(orderIds, orderId -> orderService.process(orderId))
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
    .submit();
```

### withTimeout

```java
BatchBuilder withTimeout(Duration timeout)
```

Sets the execution timeout for every child job. Null is the default and clears the override, leaving children on the global default SLA. The setting applies to the whole builder, including children already added. It does not apply to the batch parent or to workflow branch and callback jobs.

Timeouts are stored in whole seconds. Negative durations, positive durations under one second, and values above `Integer.MAX_VALUE` whole seconds throw `IllegalArgumentException`.

```java
scheduler.enqueueBatch("Process Orders")
    .forEach(orderIds, orderId -> orderService.process(orderId))
    .withTimeout(Duration.ofMinutes(45))
    .submit();
```

### withCallbackOptions

```java
BatchBuilder withCallbackOptions(JobOptions options)
```

Sets priority, retries, backoff and timeout for jobs created by `thenOnBatchSuccess`, `thenOnBatchFailure`, `thenWhenSuccessRate`, `thenWhenFailureCount`, `thenWhenBatch` and `thenBranch`.

The default is `JobOptions.defaults()`: NORMAL priority, no retries, no backoff and the global default SLA. Child settings from `withMaxRetries`, `withBackoff` and `withTimeout` do not apply to callbacks. The setting applies to the whole builder regardless of call order. Callbacks use these options for both retries and backoff.

```java
batch.withCallbackOptions(JobOptions.defaults()
    .withMaxRetries(2)
    .withTimeout(Duration.ofHours(1))
    .withPriority(JobPriority.HIGH));
```

### withResource

```java
BatchBuilder withResource(String resourceName)
```

Every child acquires a permit from the named resource pool before it runs. At capacity, children are rescheduled without counting as retries. The configured permit count caps how many children run at once. That count is shared with all other jobs using the same resource name, inside or outside the batch.

The setting applies to every child regardless of call order. The no-op parent, progress hooks, and workflow branch jobs do not inherit it. Null or blank disables resource limiting. Other names are trimmed.

`submit()` throws `UnsupportedOperationException` if a resource is set and the store does not advertise the `ResourcePermitStore` capability. Streaming batches have the same behavior at `start()`.

Configure permit counts and retry delays in `scheduler_resource_limit`, using `max_concurrent` and `retry_delay_ms`. See [Performance Tuning](../deployment/performance-tuning.md).

```java
scheduler.enqueueBatch("Payments")
    .forEach(paymentIds, id -> paymentService.charge(id))
    .withResource("payment-api")
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

Schedules a job when the number of failures stays at or below the specified threshold (failures less than or equal to `maxFailures`).

**Parameters:**
- `maxFailures` -- the maximum number of failures (inclusive) for which the action still fires.
- `next` -- the task to execute when the failure count is within the threshold.

```java
scheduler.enqueueBatch("Import Data")
    .forEach(rows, row -> importRow(row))
    .thenWhenFailureCount(10, () -> alertService.importWithinFailureBudget())
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

## StreamingBatchBuilder {#streamingbatchbuilder}

Obtained from [`JobSchedulerService.streamingBatch()`](./job-scheduler-service#streamingbatch). Designed for large datasets where items are read from a `Stream` and inserted in configurable chunks.

Framework extensions that build invocations directly (rather than serializing lambdas) have mirrored builders — `InvocationBatchBuilder` and `InvocationStreamingBatchBuilder<T>` — on the [`InvocationSubmissionService`](./spi-interfaces#invocationsubmissionservice) SPI.

`StreamingBatchBuilder` exposes the same child retry, timeout, resource, and callback option methods as `BatchBuilder`:

```java
StreamingBatchBuilder<T> withMaxRetries(int retries)
StreamingBatchBuilder<T> withBackoff(BackoffPolicy policy, Duration param)
StreamingBatchBuilder<T> withTimeout(Duration timeout)
StreamingBatchBuilder<T> withCallbackOptions(JobOptions options)
StreamingBatchBuilder<T> withResource(String resourceName)
```

The invocation-typed mirrors expose the same methods and persist the same child retry options and resource name. Both also expose `withIdempotencyKey(String key)` with the same parent-key semantics.

### withIdempotencyKey {#streaming-withidempotencykey}

```java
StreamingBatchBuilder<T> withIdempotencyKey(String key)
```

Uses the same key validation, shared namespace and deduplication rules as [`BatchBuilder.withIdempotencyKey`](#withidempotencykey). A duplicate `start()` returns the original handle and closes the stream without reading it. No parent, children, progress hook or branches are written.

**Returns:** the `StreamingBatchBuilder<T>` for chaining.

### fromStream

```java
StreamingBatchBuilder<T> fromStream(Stream<T> stream)
```

Sets the input data source for the batch. The stream item type `T` is fixed when the builder is created via [`streamingBatch()`](./job-scheduler-service#streamingbatch) and must implement `Serializable`.

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

Sets the number of items per database insert chunk. Default is 100. Tune this based on your database's bulk insert performance.

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

### Workflow methods

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
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
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

## See also

- [BatchContext Reference](./batch-context)
- [WorkflowCondition Reference](./workflow-condition)
- [JobSchedulerService Reference](./job-scheduler-service)

## Reducing builders

`BatchBuilder.reducing(R identity, SerializableBinaryOperator<R> combiner)` returns
`ReducingBatchBuilder<R>`. Its `forEach(Collection<T>, SerializableFunction<T, R>)` captures each
child's partial return value; `thenOnBatchSuccess(SerializableConsumer<R>)` registers at most one
callback receiving the combined value. All ordinary child and callback options remain available.

`StreamingBatchBuilder<T>.reducing(...)` returns `ReducingStreamingBatchBuilder<T, R>`. Use
`process(SerializableFunction<T, R>)`, or `processChecked(SerializableCheckedFunction<T, R>)` for a
checked exception, followed by `start()`. Configure reduction before `fromStream` or `process`.
Both builder families also accept `reducing(TypeRef<R>, R, SerializableBinaryOperator<R>)`.
Null identity, combiner, or type tokens are rejected. Selecting reduction after items, processing,
or a success callback throws `IllegalStateException`.

`R` is the combiner's declared generic return type, boxed for primitives; explicit `TypeRef` supports
generic combiner methods. Combiner parameters and success callback parameters must match `R`;
callback supertypes are rejected. Child return types must match, with assignable raw return types
accepted for plain-class `R`. Type variables, wildcards, and arrays are rejected. Identity must
round-trip through the configured serializer, including its generic `deserialize(String, Type)`
overload. Result classes recursively require both class-policy permissions, except the JDK values
and container raw types listed in [Batches](../concepts/batches.md#folding-partial-results).

Combiners must be deterministic and free of side effects because folding may run more than once.
Return the identity for an empty partial; null fails the batch. Folding failures run the failure
callback and suppress batch rate/count/custom conditions. Empty batches deliver the identity.
Configure stale-claim recovery with `ratchet.batch.fold-timeout=PT5M`; combined values respect
`ratchet.jobs.max-result-bytes`. The parent result is also available through job detail and result
workflow conditions.

The trusted invocation SPI accepts a per-item invocation factory. A reducing invocation stream
stages its items and resolved invocations in memory so all target return types can be checked
before any rows are written. Lambda-based reducing streams retain chunked input consumption.


## forEachPartition and PartitionedBatchBuilder

`forEachPartition(String partitionKey, List<KeyRange> partitions,
SerializableConsumer<KeyRange> action)` selects durable partition processing. It returns a
`PartitionedBatchBuilder`, a narrowed view of the same builder. It supports child options,
resources, executor selection, progress hooks and the ordinary batch callbacks. It cannot mix
with `forEach`, invocation items, reduction or streaming. After trimming, the partition key must
contain 1 to 36 printable ASCII characters (`U+0020` through `U+007E`).

`KeyRange` describes `[from, to)`. `KeyRange.split(from, to, size)` creates consecutive ranges.
Invalid bounds or nonpositive sizes throw `IllegalArgumentException`. Addition overflow throws
`ArithmeticException`. List position defines the partition index, even when ranges are unsorted.

`reopen(Collection<Integer> indexes)` reruns those partitions with no checkpoint. Duplicate
indexes are ignored. Invalid indexes throw `IllegalArgumentException`. Reopening requires a
submission idempotency key and a sealed previous generation. It cannot reopen a new set.

A later submit with the same partition key resumes unfinished partitions and keeps successful
ones. Callbacks and progress apply to the submitted generation. Query with
`JobQueryService.getPartitionSet(key)` and `getBatchPartitions(key, limit, offset)`.
`JobSchedulerService.deletePartitionSet(key)` rejects an open generation and returns false for
an unknown key. The latest batch id may refer to an archived job.

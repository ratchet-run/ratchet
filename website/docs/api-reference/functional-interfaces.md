---
sidebar_position: 11
title: Functional Interfaces
description: Reference for all serializable functional interfaces used in job definitions, callbacks, and workflow conditions.
---

# Functional Interfaces

Ratchet defines six serializable functional interfaces for job tasks, callbacks, and workflow conditions. All extend both their `java.util.function` counterpart (where applicable) and `Serializable`, enabling lambda expressions to be persisted in the job store.

**Package:** `run.ratchet.api`

## Why Serializable?

Ratchet persists job definitions in a database. When you write:

```java
scheduler.enqueue(() -> orderService.process(orderId)).submit();
```

The lambda `() -> orderService.process(orderId)` must be serialized so it can be stored, deserialized on another node, and executed later. All functional interfaces in Ratchet extend `Serializable` to make this possible.

:::warning Method Reference Constraint
All job lambdas must contain **exactly one method invocation** (a single method reference or method call). Multi-statement lambdas fail at submission time with `IllegalArgumentException`. This constraint applies to `SerializableCheckedRunnable` and `SerializableCheckedConsumer` -- the task-defining interfaces.

For complex logic, create a dedicated method in a CDI bean and reference it.
:::

## SerializableCheckedRunnable

The primary functional interface for defining job tasks. Extends `Serializable` and allows checked exceptions.

```java
@FunctionalInterface
public interface SerializableCheckedRunnable extends Serializable {
    void run() throws Exception;
}
```

**Used in:** `JobSchedulerService.enqueue()`, `schedule()`, `enqueueNow()`, `JobBuilder.then()`, all workflow branch tasks.

### Correct Usage

```java
// Method reference
scheduler.enqueue(myService::processData).submit();

// Instance method reference
scheduler.enqueue(myService::sendEmail).submit();

// Single method call with parameters
scheduler.enqueue(() -> myService.process(userId)).submit();

// Single method call with multiple parameters
scheduler.enqueue(() -> reportService.generate(userId, reportType, startDate)).submit();
```

### Incorrect Usage

```java
// WRONG -- multi-statement lambda
scheduler.enqueue(() -> {
    processData();
    updateDatabase();
}).submit();  // throws IllegalArgumentException

// WRONG -- multiple invocations
scheduler.enqueue(() -> {
    User user = userService.findById(userId);
    notificationService.send(user, message);
}).submit();  // throws IllegalArgumentException
```

### Workaround for Complex Logic

Wrap multi-step logic in a single method on a CDI bean:

```java
@ApplicationScoped
public class UserWorkflow {
    @Inject UserService userService;
    @Inject NotificationService notifications;

    public void processAndNotify(String userId) {
        User user = userService.findById(userId);
        userService.processData(user);
        notifications.send(user, "Processing complete");
    }
}

// Single method reference
scheduler.enqueue(() -> userWorkflow.processAndNotify(userId)).submit();
```

### Exception Handling

Exceptions thrown from `run()` are caught by the executor and trigger:
1. Retry attempts based on job configuration
2. Backoff delays between retries
3. Dead letter queue processing after max retries exhausted
4. Failure callbacks if configured via `onFailure()`

## SerializableConsumer\<T>

Serializable variant of `java.util.function.Consumer<T>`. Accepts a single argument and returns no result.

```java
@FunctionalInterface
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {
    void accept(T t);
}
```

**Used in:** `JobBuilder.onSuccess()` (as `SerializableConsumer<JobContext>`), `BatchBuilder.onProgress()` (as `SerializableConsumer<BatchContext>`), `BatchBuilder.forEach()`.

### Examples

```java
// Success callback
scheduler.enqueue(() -> processOrder(orderId))
    .onSuccess(ctx -> log.info("Job {} succeeded", ctx.jobId()))
    .submit();

// Batch progress monitoring
scheduler.enqueueBatch("Import")
    .forEach(records, record -> importRecord(record))
    .onProgress(ctx -> log.info("{}% complete", ctx.percentDone()))
    .submit();

// Method reference
scheduler.enqueue(() -> processOrder(orderId))
    .onSuccess(this::handleSuccess)
    .submit();

private void handleSuccess(JobContext ctx) {
    updateOrderStatus(ctx.param("orderId"), "COMPLETE");
}
```

## SerializableBiConsumer\<T, U>

Serializable variant of `java.util.function.BiConsumer<T, U>`. Accepts two arguments and returns no result.

```java
@FunctionalInterface
public interface SerializableBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {
    void accept(T t, U u);
}
```

**Used in:** `JobBuilder.onFailure()` (as `SerializableBiConsumer<JobContext, Throwable>`).

### Examples

```java
// Failure callback with context and error
scheduler.enqueue(() -> riskyOperation())
    .onFailure((ctx, error) -> {
        log.error("Job {} failed: {}", ctx.jobId(), error.getMessage());
        alertService.sendAlert(ctx.jobId(), error);
    })
    .submit();

// Method reference
scheduler.enqueue(() -> riskyOperation())
    .onFailure(this::handleFailure)
    .submit();

private void handleFailure(JobContext ctx, Throwable error) {
    log.error("Job {} failed", ctx.jobId(), error);
}
```

## SerializableFunction\<T, R>

Serializable variant of `java.util.function.Function<T, R>`. Accepts one argument and produces a result.

```java
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
    R apply(T t);
}
```

**Used in:** `JobBuilder.whenResult()` (as `SerializableFunction<T, Boolean>`), `WorkflowCondition.result()`.

### Examples

```java
// Value-based workflow branching
scheduler.enqueue(() -> analyzeData())
    .whenResult(score -> score > 0.8, () -> triggerHighPriority())
    .whenResult(score -> score <= 0.5, () -> triggerLowPriority())
    .submit();

// Complex value-based condition
scheduler.enqueue(() -> fetchUserData())
    .whenResult(user -> user.getSubscription() == Premium.GOLD,
                () -> sendPremiumFeatures())
    .submit();

// Method reference for conditions
scheduler.enqueue(() -> checkInventory())
    .whenResult(this::isLowStock, () -> reorder())
    .submit();

private Boolean isLowStock(Integer stockLevel) {
    return stockLevel < 10;
}
```

## SerializablePredicate\<T>

Serializable variant of `java.util.function.Predicate<T>`. Accepts one argument and returns a boolean.

```java
@FunctionalInterface
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {
    boolean test(T t);
}
```

**Used in:** `JobBuilder.when()` (as `SerializablePredicate<JobResult<T>>`), `BatchBuilder.thenWhenBatch()` (as `SerializablePredicate<BatchContext>`), `WorkflowCondition.custom()`, `WorkflowCondition.batchCustom()`.

### Examples

```java
// Job result condition
scheduler.enqueue(() -> processData())
    .when(result -> result.isSuccess() && result.getExecutionTimeMsOrZero() < 5000,
          () -> log.info("Fast execution"))
    .when(result -> result.isFailure() && result.getError().contains("timeout"),
          () -> increaseTimeoutAndRetry())
    .submit();

// Batch context condition
scheduler.enqueueBatch("Import")
    .forEach(records, r -> importRecord(r))
    .thenWhenBatch(ctx -> ctx.failedItems() == 0, () -> markComplete())
    .thenWhenBatch(ctx -> ctx.successRate() < 0.5, () -> rollback())
    .submit();

// Combining conditions
SerializablePredicate<BatchContext> criticalCondition =
    ctx -> ctx.failedItems() > 10 || ctx.successRate() < 0.5;

scheduler.enqueueBatch("Critical Process")
    .forEach(items, item -> processItem(item))
    .thenWhenBatch(criticalCondition, () -> escalateToOps())
    .submit();
```

## SerializableCheckedConsumer\<T>

Serializable consumer that can throw checked exceptions. Unlike `SerializableConsumer`, this does **not** extend `java.util.function.Consumer` because Consumer does not support checked exceptions.

```java
@FunctionalInterface
public interface SerializableCheckedConsumer<T> extends Serializable {
    void accept(T t) throws Exception;
}
```

**Used in:** `StreamingBatchBuilder.process()` for streaming batch item processing where the action may throw checked exceptions (e.g., `SQLException`, `IOException`).

### Examples

```java
// Streaming batch with checked exception handling
scheduler.<Long>streamingBatch("Process Users")
    .fromStream(userRepository.streamAllUserIds())
    .process(userId -> {
        // This can throw checked exceptions
        userService.processUser(userId);
    })
    .start();

// Method reference
scheduler.<Long>streamingBatch("Migrate Users")
    .fromStream(userIds.stream())
    .process(migrationService::migrateUser)
    .start();
```

### Comparison with SerializableConsumer

| Feature | SerializableConsumer | SerializableCheckedConsumer |
|---|---|---|
| Extends | `Consumer<T>` + `Serializable` | `Serializable` only |
| Checked exceptions | No | Yes |
| Primary use | Callbacks, progress hooks | Streaming batch processing |

## See Also

- [JobBuilder Reference](./job-builder)
- [BatchBuilder Reference](./batch-builder)
- [WorkflowCondition Reference](./workflow-condition)

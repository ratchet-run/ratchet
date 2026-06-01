---
sidebar_position: 3
title: JobBuilder Reference
description: Complete reference for the JobBuilder fluent API for configuring and submitting individual jobs.
---

# JobBuilder

Fluent builder API for creating and configuring individual jobs. `JobBuilder` is obtained from [`JobSchedulerService.enqueue()`](./job-scheduler-service#enqueue) or [`schedule()`](./job-scheduler-service#schedule) and provides methods for retry policies, priorities, timeouts, workflows, callbacks, tags, and parameters.

**Package:** `run.ratchet.api`
**Type:** Interface

## Basic Usage

```java
JobHandle handle = scheduler.enqueue(() -> orderService.process(orderId))
    .withPriority(JobPriority.HIGH)
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(10))
    .withTimeout(Duration.ofMinutes(5))
    .withTags("order-processing", "customer-123")
    .withParam("orderId", String.valueOf(orderId))
    .onSuccess(ctx -> log.info("Order {} processed", orderId))
    .onFailure((ctx, error) -> alertService.sendAlert(error))
    .submit();
```

## Configuration Methods

### withPriority

```java
JobBuilder withPriority(JobPriority priority)
```

Sets the execution priority. Higher priority jobs are executed before lower priority ones when multiple jobs are queued.

**Parameters:**
- `priority` -- the priority level. One of `LOWEST`, `LOW`, `NORMAL` (default), `HIGH`, `CRITICAL`.

```java
scheduler.enqueue(() -> criticalService.process())
    .withPriority(JobPriority.CRITICAL)
    .submit();
```

### withMaxRetries

```java
JobBuilder withMaxRetries(int retries)
```

Sets the maximum number of retry attempts after failure. The default is 0 (no retries).

**Parameters:**
- `retries` -- maximum retry count. Must be non-negative.

```java
scheduler.enqueue(() -> externalApi.call())
    .withMaxRetries(5)
    .submit();
```

### withBackoff

```java
JobBuilder withBackoff(BackoffPolicy policy, Duration param)
```

Configures the backoff strategy applied between retry attempts. Only relevant when `maxRetries > 0`.

**Parameters:**
- `policy` -- the backoff strategy: `NONE` (immediate retry), `FIXED` (constant delay), or `EXPONENTIAL` (doubling delay).
- `param` -- the base delay duration. For `FIXED`, this is the constant delay. For `EXPONENTIAL`, this is the initial delay.

```java
// Exponential backoff starting at 2 seconds
scheduler.enqueue(() -> paymentService.charge(amount))
    .withMaxRetries(4)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
    .submit();
// Retries at: 2s, 4s, 8s, 16s
```

### withTimeout

```java
JobBuilder withTimeout(Duration timeout)
```

Sets the maximum execution duration. Jobs exceeding this limit are forcibly terminated and marked as failed.

**Parameters:**
- `timeout` -- the maximum execution duration. Use `Duration.ZERO` to disable timeout.

```java
scheduler.enqueue(() -> longRunningImport.execute())
    .withTimeout(Duration.ofMinutes(30))
    .submit();
```

### withTags

```java
JobBuilder withTags(String... tags)
```

Adds one or more tags to the job. Tags are trimmed, converted to lowercase, and stored only if non-null and non-blank. Tags enable filtering and categorization.

**Parameters:**
- `tags` -- one or more tag strings.

```java
scheduler.enqueue(() -> reportService.generate(reportId))
    .withTags("reports", "finance", "q4-2024")
    .submit();
```

### withParam

```java
JobBuilder withParam(String key, String value)
```

Adds a key-value parameter accessible during execution via [`JobContext.param()`](./job-context#param). Parameters provide lightweight configuration without serializing complex objects.

**Parameters:**
- `key` -- parameter key. Must not be null or blank.
- `value` -- parameter value. Must not be null.

```java
scheduler.enqueue(() -> emailService.send())
    .withParam("recipient", "user@example.com")
    .withParam("template", "welcome")
    .submit();

// Inside the job:
// String recipient = JobContext.current().param("recipient");
```

### withResource

```java
JobBuilder withResource(String resourceName)
```

Specifies a shared resource that this job requires. The job will acquire a permit from the resource pool before execution. If no permits are available, the job is rescheduled with a delay.

This enables limiting concurrent access to shared resources regardless of job type (e.g., limiting concurrent API calls to a payment gateway to 5 total).

**Parameters:**
- `resourceName` -- the name of the resource to acquire. If null or blank, no resource limiting is applied.

```java
scheduler.enqueue(() -> paymentGateway.charge(paymentId))
    .withResource("payment-api")
    .submit();
```

### withIdempotencyKey

```java
JobBuilder withIdempotencyKey(String key)
```

Overrides the auto-generated idempotency key. By default, a UUID is generated at builder creation time. A custom idempotency key is **globally unique** -- once used, that key is consumed forever, preventing duplicate job creation.

**Parameters:**
- `key` -- the idempotency key. If null or blank, keeps the auto-generated UUID.

**Difference from `withBusinessKey`:**
- `idempotencyKey` is UNIQUE globally -- once used, that key is consumed forever.
- `businessKey` only blocks active (PENDING/RUNNING) jobs -- allows re-runs after completion.

```java
// Webhook handler: same delivery ID = same job forever
scheduler.enqueue(() -> webhookHandler.process(payload))
    .withIdempotencyKey(webhookDeliveryId)
    .submit();
```

### withBusinessKey

```java
JobBuilder withBusinessKey(String key)
```

Sets a business key for preventing concurrent execution against the same entity. Unlike `withIdempotencyKey`, the business key allows multiple completed jobs with the same key over time -- it only blocks when an active (PENDING/RUNNING) job exists with the same key.

**Parameters:**
- `key` -- the business key. If null or blank, no concurrent execution blocking is applied.

```java
// Only one sync per user at a time, re-runs allowed after completion
scheduler.enqueue(() -> syncService.syncUser(userId))
    .withBusinessKey("sync-user-" + userId)
    .submit();
```

### immediate

```java
JobBuilder immediate()
```

Marks the job for immediate execution notification. The scheduler publishes a wakeup to all cluster nodes, bypassing the normal adaptive polling delay.

Jobs with `CRITICAL` priority or zero delay are automatically treated as immediate. Use this method explicitly when you need immediate behavior for other configurations.

```java
scheduler.enqueue(() -> urgentService.handle(alert))
    .immediate()
    .submit();
```

### awaitSignal

```java
JobBuilder awaitSignal(String signalKey, Duration timeout)
```

Creates the job in `WAITING` status instead of making it immediately eligible for execution. The job stays blocked until a matching signal is delivered through [`JobSchedulerService.deliverSignal()`](./job-scheduler-service#signal-delivery-methods). If the signal is not delivered before `timeout`, Ratchet fails the job with `SignalTimeoutException`.

**Parameters:**
- `signalKey` -- named signal used for broadcast delivery. Use stable domain keys such as `order:123:approved`.
- `timeout` -- maximum time to wait for the signal. Must be positive.

```java
scheduler.enqueue(() -> approvalService.continueOrder(orderId))
    .awaitSignal("order:" + orderId + ":approved", Duration.ofHours(24))
    .withTags("approval", "orders")
    .submit();

// Later, from an admin action or another workflow:
scheduler.deliverSignal(
    "order:" + orderId + ":approved",
    SignalDecision.approved("approved-by-manager"));
```

Inside the waiting job, read the payload through [`JobContext.signalPayload()`](./job-context#signalpayload):

```java
public void continueOrder(UUID orderId) {
    SignalDecision decision = JobContext.current().signalPayload(SignalDecision.class);
    if (decision != null && decision.isRejected()) {
        throw new OrderRejectedException(decision.rejectionReason());
    }
    fulfillment.start(orderId);
}
```

## Callback Methods

### onSuccess

```java
JobBuilder onSuccess(SerializableConsumer<JobContext> s)
```

Registers a callback invoked after successful job completion. The callback receives the [`JobContext`](./job-context) of the completed job.

**Parameters:**
- `s` -- success callback accepting a `JobContext`.

```java
scheduler.enqueue(() -> importService.importData(batchId))
    .onSuccess(ctx -> log.info("Job {} completed", ctx.jobId()))
    .submit();
```

### onFailure

```java
JobBuilder onFailure(SerializableBiConsumer<JobContext, Throwable> f)
```

Registers a callback invoked if the job fails. The callback receives both the `JobContext` and the `Throwable` that caused the failure.

**Parameters:**
- `f` -- failure callback accepting a `JobContext` and `Throwable`.

```java
scheduler.enqueue(() -> riskyService.execute())
    .onFailure((ctx, error) -> {
        alertService.page("Job " + ctx.jobId() + " failed: " + error.getMessage());
    })
    .submit();
```

## Workflow Methods

### then

```java
JobBuilder then(SerializableCheckedRunnable next)
```

Adds a task to the sequential chain. Chain tasks execute in order after the primary task completes successfully. If any task in the chain fails, subsequent tasks are not executed (unless configured with workflow branches).

**Parameters:**
- `next` -- the next task in the chain. Must not be null.

```java
scheduler.enqueue(() -> validateData())
    .then(() -> processData())
    .then(() -> generateReport())
    .then(() -> sendNotification())
    .submit();
```

### thenOnSuccess

```java
JobBuilder thenOnSuccess(SerializableCheckedRunnable next)
```

Schedules a **separate job** to execute if the current job succeeds. This creates a workflow branch with a `SUCCESS` condition. Unlike `then()`, this creates an independent job, not a chain step.

**Parameters:**
- `next` -- the task to execute on success as a separate job.

```java
scheduler.enqueue(() -> processPayment(orderId))
    .thenOnSuccess(() -> sendReceipt(orderId))
    .thenOnFailure(() -> refundPayment(orderId))
    .submit();
```

### thenOnFailure

```java
JobBuilder thenOnFailure(SerializableCheckedRunnable next)
```

Schedules a **separate job** to execute if the current job fails (after all retries are exhausted). This creates a workflow branch with a `FAILURE` condition.

**Parameters:**
- `next` -- the task to execute on failure as a separate job.

```java
scheduler.enqueue(() -> processPayment(orderId))
    .thenOnFailure(() -> alertTeam(orderId))
    .submit();
```

### when

```java
<T> JobBuilder when(
    SerializablePredicate<JobResult<T>> condition,
    SerializableCheckedRunnable next)
```

Schedules a job to execute when a custom condition is met. The condition receives the full [`JobResult<T>`](./job-result) of the current job.

**Type Parameters:**
- `T` -- the type of the job result.

**Parameters:**
- `condition` -- predicate evaluated against the `JobResult`.
- `next` -- the task to execute when the condition is true.

```java
public final class DataAnalysisConditions {
    public static boolean isSlowSuccess(JobResult<?> result) {
        return result.isSuccess()
            && result.getExecutionTimeMsOrZero() > 60_000;
    }
}

scheduler.enqueue(() -> analyzeData())
    .when(DataAnalysisConditions::isSlowSuccess,
          () -> alertSlowExecution())
    .when(JobResult::isFailure,
          () -> notifyAdmins())
    .submit();
```

Custom workflow predicates are analyzed at submission time and stored as `JobPayload` JSON. Put compound logic in a public helper method or CDI bean method, then pass a method reference or a single-call lambda.

### when (with priority)

```java
<T> JobBuilder when(
    SerializablePredicate<JobResult<T>> condition,
    SerializableCheckedRunnable next,
    int priority)
```

Same as `when()` but with an explicit evaluation priority. Lower priority numbers are evaluated first.

**Parameters:**
- `condition` -- predicate evaluated against the `JobResult`.
- `next` -- the task to execute when the condition is true.
- `priority` -- evaluation priority (lower = higher priority, default is 0).

```java
scheduler.enqueue(() -> processOrder())
    .when(JobResult::isSuccess, () -> confirmOrder(), 0)    // evaluated first
    .when(JobResult::isFailure, () -> cancelOrder(), 1)     // evaluated second
    .submit();
```

### whenResult

```java
<T> JobBuilder whenResult(
    SerializableFunction<T, Boolean> condition,
    SerializableCheckedRunnable next)
```

Schedules a job based on the **return value** of the current job. The condition function receives only the value, not the full `JobResult`.

**Type Parameters:**
- `T` -- the type of the job's return value.

**Parameters:**
- `condition` -- function that evaluates the return value and returns a boolean.
- `next` -- the task to execute when the condition returns true.

```java
public final class StockConditions {
    public static boolean isHighStock(Integer stock) {
        return stock > 100;
    }

    public static boolean isLowStock(Integer stock) {
        return stock > 0 && stock <= 100;
    }

    public static boolean isOutOfStock(Integer stock) {
        return stock == 0;
    }
}

scheduler.enqueue(() -> inventoryService.checkStock(itemId))
    .whenResult(StockConditions::isHighStock, () -> placeOrder(itemId))
    .whenResult(StockConditions::isLowStock, () -> alertLowStock(itemId))
    .whenResult(StockConditions::isOutOfStock, () -> markOutOfStock(itemId))
    .submit();
```

### branch

```java
JobBuilder branch(WorkflowCondition condition,
                  SerializableCheckedRunnable next,
                  String description)
```

Adds a workflow branch with an explicit [`WorkflowCondition`](./workflow-condition) and a human-readable description for monitoring and debugging.

**Parameters:**
- `condition` -- the workflow condition determining when this branch fires.
- `next` -- the task to execute.
- `description` -- human-readable description for logs and dashboards.

```java
public final class MetricsConditions {
    public static boolean isSlowJob(JobResult<?> result) {
        return result.getExecutionTimeMsOrZero() > 30_000;
    }
}

scheduler.enqueue(() -> analyzeMetrics())
    .branch(WorkflowCondition.custom(MetricsConditions::isSlowJob),
            () -> alertSlowJob(),
            "Alert ops when analysis takes over 30 seconds")
    .submit();
```

## Submission

### submit

```java
JobHandle submit()
```

Submits the configured job (including the primary task, chain tasks, and workflow branches) to the scheduler for persistence and execution.

**Returns:** a [`JobHandle`](./job-scheduler-service#jobhandle) containing the unique job ID.

```java
JobHandle handle = scheduler.enqueue(() -> processData())
    .withMaxRetries(3)
    .submit();

log.info("Submitted job {}", handle.id());
```

## Accessor Methods

These methods allow reading the configured state of a builder. They are primarily used internally by the scheduler but are part of the public interface.

| Method | Return Type | Description |
|---|---|---|
| `task()` | `SerializableCheckedRunnable` | The primary task |
| `workflowBranches()` | `List<WorkflowBranch>` | Immutable list of workflow branches |
| `opts()` | `JobOptions` | Current immutable job options snapshot |
| `params()` | `Map<String, String>` | Unmodifiable parameter map |
| `tags()` | `List<String>` | Unmodifiable tag list |
| `resourceName()` | `String` | Resource name, or null if not set |
| `isImmediate()` | `boolean` | Whether immediate wakeup is requested |
| `onSuccess()` | `SerializableConsumer<JobContext>` | Success callback, or null |

## Example: Complete Configuration

```java
JobHandle handle = scheduler.enqueue(() -> paymentService.charge(orderId, amount))
    // Execution options
    .withPriority(JobPriority.HIGH)
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(5))
    .withTimeout(Duration.ofMinutes(2))
    // Resource limiting
    .withResource("payment-gateway")
    // Identity
    .withBusinessKey("charge-" + orderId)
    .withIdempotencyKey(requestId)
    // Metadata
    .withTags("payments", "orders")
    .withParam("orderId", String.valueOf(orderId))
    .withParam("amount", amount.toString())
    // Callbacks
    .onSuccess(ctx -> log.info("Payment charged for order {}", ctx.param("orderId")))
    .onFailure((ctx, err) -> alertService.paymentFailed(ctx.param("orderId"), err))
    // Workflow branches
    .thenOnSuccess(() -> fulfillmentService.startFulfillment(orderId))
    .thenOnFailure(() -> orderService.markPaymentFailed(orderId))
    // Immediate cluster notification
    .immediate()
    .submit();
```

## See Also

- [JobSchedulerService Reference](./job-scheduler-service)
- [JobOptions Reference](./job-options)
- [WorkflowCondition Reference](./workflow-condition)
- [Functional Interfaces](./functional-interfaces)

---
sidebar_position: 4
title: JobContext Reference
description: Thread-local access to job execution metadata, parameters, and logging during job execution.
---

# JobContext Reference

Thread-local context object providing job-specific information during execution. `JobContext` gives your job access to its ID, a structured logger, and parameters -- all without explicit parameter passing.

**Package:** `run.ratchet.api`
**Type:** Final class (not instantiable directly)

## Overview

When Ratchet executes your job, a `JobContext` is automatically bound to the executing thread and cleared when the job completes. Access it with `JobContext.current()`:

```java
void processOrder(long orderId) {
    JobContext ctx = JobContext.current();
    ctx.logger().info("Processing order " + orderId);

    String mode = ctx.param("validation-mode", "standard");
    ctx.logger().debug("Validation mode: " + mode);
}
```

`JobContext` is a plain thread-local. Work handed to another thread with `CompletableFuture.supplyAsync(...)` or a separate executor does not inherit it. If a job submits another job and expects caller-principal inheritance, make that submission on the Ratchet execution thread while the context is bound.

## Static methods

### current

```java
public static JobContext current()
```

Retrieves the `JobContext` bound to the current thread.

**Returns:** the `JobContext` for the currently executing job.

**Throws:** `IllegalStateException` if no `JobContext` is bound (i.e., called outside of a Ratchet-managed job execution).

```java
JobContext ctx = JobContext.current();
UUID id = ctx.jobId();
```

### currentOrNull

```java
public static JobContext currentOrNull()
```

Retrieves the `JobContext` bound to the current thread, or returns `null` when called outside a Ratchet-managed job execution.

### bind

```java
public static JobContext bind(UUID jobId, JobLogger logger)
```

Binds a new `JobContext` to the current thread. This is called internally by the Ratchet executor. For unit testing or advanced scenarios, you can call this manually.

**Parameters:**
- `jobId` -- the unique identifier of the job.
- `logger` -- the `JobLogger` instance for this job.

**Returns:** the newly created and bound `JobContext`.

```java
// Unit testing a job that uses JobContext
JobContext ctx = JobContext.bind(UUID.randomUUID(), new TestJobLogger());
try {
    myJob.execute();
} finally {
    JobContext.clear();
}
```

### bind (with parameters)

```java
public static JobContext bind(UUID jobId, JobLogger logger, Map<String, String> params)
```

Binds a new `JobContext` with parameters to the current thread.

**Parameters:**
- `jobId` -- the unique identifier of the job.
- `logger` -- the `JobLogger` instance for this job.
- `params` -- the job parameters map. Wrapped as unmodifiable internally.

**Returns:** the newly created and bound `JobContext`.

```java
// Unit testing with parameters
Map<String, String> params = Map.of("userId", "123", "action", "sync");
JobContext ctx = JobContext.bind(UUID.randomUUID(), new TestJobLogger(), params);
try {
    myJob.execute();
} finally {
    JobContext.clear();
}
```

### bind (with parameters and signal payload)

```java
public static JobContext bind(
    UUID jobId,
    JobLogger logger,
    Map<String, String> params,
    Serializable signalPayload)
```

Binds a context with both job parameters and a pre-deserialized signal payload. Ratchet calls this overload internally when executing a job that was released from `WAITING` status by `deliverSignal()`.

Most application code should not call this directly except in unit tests for signal-aware job methods.

### bind (with caller principal)

```java
public static JobContext bind(
    UUID jobId,
    JobLogger logger,
    Map<String, String> params,
    String callerPrincipal,
    Serializable signalPayload)
```

Binds a context with parameters, the persisted caller principal, and an optional signal payload. Ratchet calls this overload internally while executing persisted jobs. `callerPrincipal` may be `null` when no principal was captured at creation.

### bindAttempt

```java
public static JobContext bindAttempt(
    UUID jobId,
    JobLogger logger,
    Map<String, String> params,
    AttemptToken attemptToken,
    BooleanSupplier currentAttempt)
```

Binds a context that has an [attempt token](#attempt-tokens), for unit tests of job code that calls `attemptToken()` or `isCurrentAttempt()`. The supplier decides what `isCurrentAttempt()` returns, so a test can make the attempt current, then superseded. The token must belong to `jobId`.

```java
UUID jobId = UUID.randomUUID();
AtomicBoolean current = new AtomicBoolean(true);
JobContext.bindAttempt(
    jobId, new NoOpJobLogger(), Map.of(), new AttemptToken(jobId, 3), current::get);
try {
    current.set(false);
    myService.importBatch();  // should stop without writing
} finally {
    JobContext.clear();
}
```

### clear

```java
public static void clear()
```

Removes the `JobContext` bound to the current thread. Must be called when job execution completes to prevent memory leaks in thread pool environments.

This is called automatically by the Ratchet executor. You only need to call it manually if you use `bind()` directly.

```java
JobContext.bind(UUID.randomUUID(), logger);
try {
    // Job execution
} finally {
    JobContext.clear();
}
```

## Instance methods

### jobId

```java
public UUID jobId()
```

Returns the unique database identifier of the currently executing job. Use this for correlation in logs, monitoring, and when referencing the job in other system components.

**Returns:** the job's unique identifier.

```java
UUID id = JobContext.current().jobId();
log.info("Running job {}", id);
```

### logger

```java
public JobLogger logger()
```

Returns the logger instance for this job. The logger automatically includes job context information (job ID, timestamps) in all log entries.

**Returns:** the job-specific `JobLogger` instance.

The `JobLogger` interface provides five log levels:

```java
JobLogger log = ctx.logger();
log.trace("Fine-grained detail");
log.debug("Diagnostic information");
log.info("Progress milestone");
log.warn("Potential issue");
log.error("Failure occurred");
```

### callerPrincipal

```java
public String callerPrincipal()
```

Returns the principal captured when this job was created, or `null` when no principal was captured. A `null` value means Ratchet did not capture a principal; it is not a synthetic `"system"` value.

### param

```java
public String param(String key)
```

Retrieves a parameter value by key. Parameters are set at submission time via [`JobBuilder.withParam()`](./job-builder#withparam).

**Parameters:**
- `key` -- the parameter key to look up.

**Returns:** the parameter value, or `null` if the key does not exist.

```java
String email = ctx.param("email");
String invoiceId = ctx.param("invoiceId");
```

### param (with default)

```java
public String param(String key, String defaultValue)
```

Retrieves a parameter value with a fallback default. Useful for optional parameters where a sensible default exists.

**Parameters:**
- `key` -- the parameter key to look up.
- `defaultValue` -- the value to return if the key is not found.

**Returns:** the parameter value if present, otherwise the default value.

```java
String batchSize = ctx.param("batchSize", "100");
String timeout = ctx.param("timeout", "30000");
int size = Integer.parseInt(batchSize);
```

### params

```java
public Map<String, String> params()
```

Returns all parameters configured for this job as an unmodifiable map.

**Returns:** unmodifiable `Map<String, String>`, never null.

```java
Map<String, String> allParams = ctx.params();
allParams.forEach((k, v) -> ctx.logger().debug("Param: " + k + "=" + v));
```

### signalPayload

```java
public <T extends Serializable> T signalPayload(Class<T> type)
```

Returns the payload delivered to a signal-waiting job, cast to the requested type. Returns `null` when the current job was not created with [`awaitSignal()`](./job-builder#awaitsignal) or when the delivered signal did not include a payload.

```java
SignalDecision decision = JobContext.current().signalPayload(SignalDecision.class);

if (decision != null && decision.isApproved()) {
    audit.recordApproval(decision.payload(String.class));
}
```

For simple payloads delivered with `deliverSignal(jobId, payload)`, request the payload type directly:

```java
String reviewer = JobContext.current().signalPayload(String.class);
```

### attemptToken

```java
public Optional<AttemptToken> attemptToken()
```

Returns the token of the attempt this context belongs to. It is empty when there is no attempt: in `onSuccess` and `onFailure` callbacks, and in contexts bound with the plain `bind` methods. See [Attempt tokens](#attempt-tokens).

### isCurrentAttempt

```java
public boolean isCurrentAttempt()
```

Returns `true` only while the store shows this job `RUNNING` under this attempt's token. Returns `false` with no database call when there is no attempt, or when Ratchet already knows the attempt has lost its claim: the watchdog took it over, the store rejected one of its writes, or the node is shutting down.

Otherwise each call reads the store. A store error is thrown, never read as current. See [Attempt tokens](#attempt-tokens) for when to call it and what it does not guarantee.

## Attempt tokens

Every time a node claims a job, the claim gets a sequence number one higher than the job's last claim. An `AttemptToken` holds the job's ID and that sequence. A retry, or a reclaim after a node stops responding, gets a higher sequence. So does a job reset after it failed.

An old attempt can keep running after Ratchet has given its job to a new one: the watchdog's interrupt is cooperative, and a node that was paused for a long time does not know that orphan recovery has moved its jobs. `isCurrentAttempt()` lets such code find out, so it can stop before it does more work.

```java
void importBatch(long batchId) {
    JobContext ctx = JobContext.current();
    for (Chunk chunk : chunks(batchId)) {
        if (!ctx.isCurrentAttempt()) {
            ctx.logger().info("Superseded; leaving the rest to the current attempt");
            return;
        }
        importChunk(chunk);
    }
}
```

### A check is not a fence

`isCurrentAttempt()` can become false just after it returns true. Use it to stop early, not to protect a write. To make sure an old attempt cannot overwrite a newer one, store the token's `sequence()` next to your own data and refuse a write that carries a lower sequence, in the same statement as the write:

```java
AttemptToken token = ctx.attemptToken().orElseThrow();
int updated = jdbc.update(
    "UPDATE import_progress SET last_chunk = ?, attempt_seq = ? "
        + "WHERE batch_id = ? AND attempt_seq <= ?",
    chunkNo, token.sequence(), batchId, token.sequence());
if (updated == 0) {
    return;  // a newer attempt has written here
}
```

Compare tokens only for the same job. `isNewerThan` throws for tokens of different jobs. Each firing of a recurring job, each batch child, chain step and workflow branch is its own job with its own ID, so a token never orders two firings of one schedule. `toString()` gives `jobId:sequence`, and `AttemptToken.parse` reads it back.

### Cost and threads

On SQL stores each `isCurrentAttempt()` call runs a small read in its own transaction. If your job holds a database transaction at the time, the read needs a second connection from the pool, and it waits when the pool is empty. Call it at safe points, outside long transactions, and never in a tight loop. Leave headroom in the pool for these reads.

The token is a value. You can pass it, or the `JobContext` itself, to threads your job starts. `isCurrentAttempt()` works from any thread that holds the context.

## Thread-local lifecycle

`JobContext` uses `ThreadLocal` storage. The lifecycle is:

1. **Bind** -- Ratchet creates and binds a `JobContext` when job execution begins.
2. **Available** -- `JobContext.current()` returns the context throughout job execution.
3. **Clear** -- Ratchet removes the context when the job completes (success or failure).

Each thread has its own isolated context, preventing cross-contamination when multiple jobs run concurrently.

:::warning
`JobContext.current()` only works inside a Ratchet-managed job execution. Calling it from other threads, background tasks, or outside a job throws `IllegalStateException`.
:::

## Example: multi-step job with parameters

```java
@ApplicationScoped
public class OrderProcessor {

    @Inject OrderRepository orders;
    @Inject InventoryService inventory;
    @Inject NotificationService notifications;

    public void processOrder(long orderId) {
        JobContext ctx = JobContext.current();
        ctx.logger().info("Processing order " + orderId);

        // Read configuration from parameters
        String mode = ctx.param("fulfillment-mode", "standard");
        boolean priority = "express".equals(mode);

        // Step 1: Validate
        ctx.logger().debug("Validating order");
        orders.validate(orderId);

        // Step 2: Reserve inventory
        ctx.logger().debug("Reserving inventory");
        inventory.reserve(orderId, priority);

        // Step 3: Confirm
        ctx.logger().info("Order " + orderId + " confirmed");
        notifications.orderConfirmed(orderId);
    }
}
```

Submit the job with parameters:

```java
scheduler.enqueue(() -> orderProcessor.processOrder(orderId))
    .withParam("fulfillment-mode", "express")
    .withTags("orders", "fulfillment")
    .submit();
```

## Example: unit testing with JobContext

```java
@Test
void testJobUsesParameters() {
    // Arrange
    Map<String, String> params = Map.of("mode", "strict");
    JobContext.bind(UUID.randomUUID(), new NoOpJobLogger(), params);
    try {
        // Act
        myService.processWithContext();

        // Assert
        // verify behavior when mode=strict
    } finally {
        JobContext.clear();
    }
}
```

## See also

- [JobBuilder Parameters](./job-builder#withparam)
- [Signal-Waiting Jobs](./job-builder#awaitsignal)
- [JobLogger SPI](./spi-interfaces#joblogger)
- [JobSchedulerService Reference](./job-scheduler-service)

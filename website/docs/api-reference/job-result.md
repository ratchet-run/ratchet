---
sidebar_position: 5
title: JobResult Reference
description: Complete reference for JobResult, capturing job execution outcomes including success/failure, return values, timing, and metadata.
---

# JobResult Reference

Result object that captures the outcome of a job execution. `JobResult<T>` holds success/failure status, return values, error details, timing information, and custom metadata. It is the primary input for workflow condition evaluation.

**Package:** `run.ratchet.api`
**Type:** `class JobResult<T> implements Serializable`

## Overview

`JobResult` is created internally by the Ratchet executor after each job execution. You interact with it primarily through workflow conditions:

```java
public final class AnalysisConditions {
    private static final int HIGH_VALUE_THRESHOLD = 100;

    public static boolean isHighValue(JobResult<Integer> result) {
        return result.isSuccess()
            && result.hasValue()
            && result.getValue() > HIGH_VALUE_THRESHOLD;
    }

    public static boolean isSlow(JobResult<?> result) {
        return result.getExecutionTimeMsOrZero() > 60_000;
    }
}

scheduler.enqueue(() -> analyzeData())
    .when(AnalysisConditions::isHighValue,
          () -> handleHighValue())
    .when(AnalysisConditions::isSlow,
          () -> alertSlowExecution())
    .thenOnFailure(() -> notifyAdmins())
    .submit();
```

## Factory methods

### success

```java
public static <T> JobResult<T> success(T value)
```

Creates a successful result with the given return value.

**Type Parameters:**
- `T` -- the type of the return value.

**Parameters:**
- `value` -- the return value from the job (may be null for void methods).

**Returns:** a successful `JobResult` containing the value.

```java
JobResult<Integer> result = JobResult.success(42);
JobResult<String> result = JobResult.success("completed");
JobResult<Void> result = JobResult.success(null);
```

### failure

```java
public static <T> JobResult<T> failure(String error, Throwable exception)
```

Creates a failure result with an error message and exception.

**Type Parameters:**
- `T` -- the type of the return value (not used for failures).

**Parameters:**
- `error` -- human-readable error message.
- `exception` -- the exception that caused the failure.

**Returns:** a failed `JobResult` containing the error details.

```java
JobResult<Void> result = JobResult.failure(
    "Database connection failed",
    new SQLException("Connection refused"));
```

### of

```java
public static <T> JobResult<T> of(
    boolean success, T value, String error, Throwable exception,
    Long executionTimeMs, Instant startTime, Instant endTime,
    Map<String, Object> metadata)
```

Creates a `JobResult` with all fields specified. This is the full-control factory used internally by the executor.

**Parameters:**
- `success` -- whether the job completed successfully.
- `value` -- the return value from the job (null for void or failures).
- `error` -- human-readable error message (null for successes).
- `exception` -- the exception that caused failure (null for successes).
- `executionTimeMs` -- total execution time in milliseconds (null if not recorded).
- `startTime` -- timestamp when execution began (null if not recorded).
- `endTime` -- timestamp when execution completed (null if not recorded).
- `metadata` -- extensible key-value pairs (null if none).

**Returns:** a `JobResult` with all fields populated.

```java
JobResult<String> result = JobResult.of(
    true, "processed", null, null,
    1250L, startInstant, endInstant,
    Map.of("recordsProcessed", 500));
```

## Status methods

### isSuccess

```java
public boolean isSuccess()
```

Returns `true` if the job completed without throwing an exception, regardless of whether it returned a value.

```java
if (result.isSuccess()) {
    processValue(result.getValue());
}
```

### isFailure

```java
public boolean isFailure()
```

Convenience method equivalent to `!isSuccess()`.

```java
if (result.isFailure()) {
    log.error("Job failed: " + result.getError());
}
```

### hasValue

```java
public boolean hasValue()
```

Returns `true` if the job returned a non-null value. A job can be successful without returning a value (e.g., void methods).

```java
if (result.hasValue()) {
    Integer count = (Integer) result.getValue();
}
```

### hasError

```java
public boolean hasError()
```

Returns `true` if either an error message or exception is present.

```java
if (result.hasError()) {
    log.error("Error: {}", result.getError());
}
```

## Value access

### getValue

```java
public T getValue()
```

Returns the value produced by the job. May be `null` for void methods or failed jobs.

**Returns:** the return value of type `T`, or null.

```java
Integer count = result.getValue();
```

### getError

```java
public String getError()
```

Returns the human-readable error message, or `null` if no error occurred.

```java
String msg = result.getError(); // e.g., "Connection refused"
```

### getException

```java
public Throwable getException()
```

Returns the exception that caused the failure, or `null` if no exception occurred. Preserves the full stack trace and cause chain.

:::note
Some exception types may not be serializable. When `JobResult` is serialized (e.g., for persistence), the exception is automatically sanitized to a `RuntimeException` that preserves the class name, message, and stack trace.
:::

```java
Throwable cause = result.getException();
if (cause instanceof TimeoutException) {
    // Handle timeout specifically
}
```

## Timing methods

### getStartTime

```java
public Instant getStartTime()
```

Returns when job execution began, or `null` if not recorded.

```java
Instant started = result.getStartTime();
```

### getEndTime

```java
public Instant getEndTime()
```

Returns when job execution completed, or `null` if not recorded.

```java
Instant ended = result.getEndTime();
```

### getExecutionTimeMs

```java
public Long getExecutionTimeMs()
```

Returns the total execution time in milliseconds, or `null` if not recorded.

```java
Long ms = result.getExecutionTimeMs();
if (ms != null && ms > 30000) {
    log.warn("Slow job: {} ms", ms);
}
```

### getExecutionTimeMsOrZero

```java
public long getExecutionTimeMsOrZero()
```

Returns the execution time in milliseconds, or `0` if not recorded. Safe to use in calculations without null checking.

```java
long duration = result.getExecutionTimeMsOrZero();
metrics.record("job.duration", duration);
```

## Metadata methods

### getMetadata (map)

```java
public Map<String, Object> getMetadata()
```

Returns the full metadata map, or `null` if no metadata was attached.

```java
Map<String, Object> meta = result.getMetadata();
```

### getMetadata (by key)

```java
public Object getMetadata(String key)
```

Returns a single metadata value by key, or `null` if not found.

**Parameters:**
- `key` -- the metadata key.

```java
Object count = result.getMetadata("recordsProcessed");
```

### getMetadata (typed with default)

```java
public <V> V getMetadata(String key, V defaultValue)
```

Returns a typed metadata value with a fallback default. Automatically casts the value.

**Type Parameters:**
- `V` -- the expected type of the metadata value.

**Parameters:**
- `key` -- the metadata key.
- `defaultValue` -- the value to return if the key is not found.

**Returns:** the metadata value if present, otherwise the default.

```java
Integer count = result.getMetadata("processedCount", 0);
String status = result.getMetadata("status", "unknown");
Double rate = result.getMetadata("successRate", 1.0);
```

## Using in workflow conditions

`JobResult` is the primary input for workflow branching decisions:

### Success/failure branching

```java
scheduler.enqueue(() -> processPayment(orderId))
    .thenOnSuccess(() -> sendReceipt(orderId))
    .thenOnFailure(() -> refundPayment(orderId))
    .submit();
```

### Value-based branching

```java
public final class StockConditions {
    public static boolean isHighStock(Integer stock) {
        return stock > 100;
    }

    public static boolean isOutOfStock(Integer stock) {
        return stock == 0;
    }
}

scheduler.enqueue(() -> inventoryService.checkStock(itemId))
    .whenResult(StockConditions::isHighStock, () -> placeOrder(itemId))
    .whenResult(StockConditions::isOutOfStock, () -> notifyOutOfStock(itemId))
    .submit();
```

### Complex condition branching

```java
public final class AnalysisConditions {
    public static boolean isSlowSuccess(JobResult<?> result) {
        return result.isSuccess()
            && result.getExecutionTimeMsOrZero() > 30_000;
    }

    public static boolean isCritical(JobResult<?> result) {
        return result.isSuccess()
            && "critical".equals(result.getMetadata("severity"));
    }
}

scheduler.enqueue(() -> analyzeData())
    .when(AnalysisConditions::isSlowSuccess,
          () -> alertSlowJob())
    .when(AnalysisConditions::isCritical,
          () -> escalateToOps())
    .submit();
```

## Serialization

`JobResult` implements `Serializable`. During serialization, non-serializable `Throwable` instances in the `exception` field are automatically converted to a safe `RuntimeException` that preserves the original class name, message, and stack trace. This is transparent -- in-memory access via `getException()` returns the original `Throwable`.

## See also

- [WorkflowCondition Reference](./workflow-condition)
- [JobBuilder Workflow Methods](./job-builder#workflow-methods)
- [Job Lifecycle](/concepts/job-lifecycle)

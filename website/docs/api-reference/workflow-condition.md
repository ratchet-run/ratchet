---
sidebar_position: 8
title: WorkflowCondition Reference
description: Reference for WorkflowCondition types, WorkflowBranch records, and priority-based workflow evaluation.
---

# WorkflowCondition Reference

Defines conditions for dynamic workflow branching in job execution. `WorkflowCondition` determines whether a workflow branch should fire based on the outcome of a parent job or batch.

**Package:** `run.ratchet.api`
**Type:** `record WorkflowCondition(ConditionType type, Serializable expression, int priority) implements Serializable`

## Overview

Workflow conditions are used with [`JobBuilder.when()`](./job-builder#when), [`JobBuilder.thenOnSuccess()`](./job-builder#thenonsuccess), and the various `BatchBuilder` workflow methods. You typically use the static factory methods rather than constructing conditions directly.

```java
scheduler.enqueue(() -> analyzeData())
    .when(WorkflowCondition.success(), () -> archiveResults(), "Archive on success")
    .when(WorkflowCondition.failure(), () -> notifyAdmins(), "Alert on failure")
    .submit();
```

## Condition Types

The `ConditionType` enum defines 9 evaluation strategies:

### Job Conditions

| Type | Description | Expression |
|---|---|---|
| `SUCCESS` | Job completed without exception | null |
| `FAILURE` | Job threw an exception | null |
| `CUSTOM` | Custom predicate on full `JobResult` | `SerializablePredicate<JobResult<T>>` |
| `RESULT_VALUE` | Predicate on the job's return value only | `SerializableFunction<T, Boolean>` |

### Batch Conditions

| Type | Description | Expression |
|---|---|---|
| `BATCH_SUCCESS` | All child jobs completed successfully | null |
| `BATCH_FAILURE` | One or more child jobs failed | null |
| `BATCH_SUCCESS_RATE` | Success rate meets threshold | `Double` (0.0 - 1.0) |
| `BATCH_FAILURE_COUNT` | Failure count within threshold | `Integer` |
| `BATCH_CUSTOM` | Custom predicate on `BatchContext` | `SerializablePredicate<BatchContext>` |

## Factory Methods

### success

```java
public static WorkflowCondition success()
```

Creates a condition that triggers when the job completes successfully.

```java
scheduler.enqueue(() -> processData())
    .thenOnSuccess(() -> sendConfirmation())
    .submit();

// Equivalent using explicit condition:
scheduler.enqueue(() -> processData())
    .branch(WorkflowCondition.success(), () -> sendConfirmation(), "Confirm on success")
    .submit();
```

### failure

```java
public static WorkflowCondition failure()
```

Creates a condition that triggers when the job fails.

```java
scheduler.enqueue(() -> processPayment())
    .thenOnFailure(() -> refundPayment())
    .submit();
```

### custom

```java
public static <T> WorkflowCondition custom(SerializablePredicate<JobResult<T>> predicate)
```

Creates a custom condition evaluated against the full [`JobResult`](./job-result). This is the most flexible option, giving access to success status, return value, execution time, error details, and metadata.

**Type Parameters:**
- `T` -- the type of the job's return value.

```java
// Trigger alert on slow successful jobs
WorkflowCondition slowJob = WorkflowCondition.custom(
    result -> result.isSuccess() && result.getExecutionTimeMsOrZero() > 30000);

scheduler.enqueue(() -> processData())
    .branch(slowJob, () -> alertSlowExecution(), "Alert when job takes >30s")
    .submit();
```

### custom (with priority)

```java
public static <T> WorkflowCondition custom(
    SerializablePredicate<JobResult<T>> predicate, int priority)
```

Same as `custom()` but with an explicit evaluation priority. Lower values are evaluated first.

```java
// High-priority condition evaluated first
WorkflowCondition critical = WorkflowCondition.custom(
    result -> result.isFailure() && result.getError().contains("CRITICAL"), -1);
```

### result

```java
public static <T> WorkflowCondition result(SerializableFunction<T, Boolean> function)
```

Creates a condition based on the job's **return value** only. The function receives the return value (not the full `JobResult`) and returns a boolean.

**Type Parameters:**
- `T` -- the type of the job's return value.

```java
// Branch on inventory count
WorkflowCondition highStock = WorkflowCondition.result(
    (Integer stock) -> stock > 100);

scheduler.enqueue(() -> inventoryService.checkStock(itemId))
    .whenResult(stock -> stock > 100, () -> placeOrder(itemId))
    .whenResult(stock -> stock == 0, () -> markOutOfStock(itemId))
    .submit();
```

### batchSuccess

```java
public static WorkflowCondition batchSuccess()
```

Creates a condition that triggers when **all** child jobs in a batch complete successfully.

```java
scheduler.enqueueBatch("Sync All Accounts")
    .forEach(accounts, acct -> syncService.sync(acct))
    .thenOnBatchSuccess(() -> notifyComplete())
    .submit();
```

### batchFailure

```java
public static WorkflowCondition batchFailure()
```

Creates a condition that triggers when **one or more** child jobs fail.

```java
scheduler.enqueueBatch("Process Records")
    .forEach(records, r -> processRecord(r))
    .thenOnBatchFailure(() -> handlePartialFailure())
    .submit();
```

### successRate

```java
public static WorkflowCondition successRate(double minRate)
```

Creates a condition based on batch success rate threshold.

**Parameters:**
- `minRate` -- minimum success rate (0.0 to 1.0).

**Throws:** `IllegalArgumentException` if `minRate` is not between 0.0 and 1.0.

```java
scheduler.enqueueBatch("Email Campaign")
    .forEach(recipients, r -> sendEmail(r))
    .thenWhenSuccessRate(0.95, () -> log.info("Campaign successful"))
    .submit();
```

### failureCount

```java
public static WorkflowCondition failureCount(int maxFailures)
```

Creates a condition based on maximum acceptable batch failures. The condition evaluates to true when failures are **at or below** the threshold.

**Parameters:**
- `maxFailures` -- maximum number of acceptable failures.

**Throws:** `IllegalArgumentException` if `maxFailures` is negative.

```java
scheduler.enqueueBatch("Import Data")
    .forEach(rows, row -> importRow(row))
    .thenWhenFailureCount(5, () -> log.info("Import acceptable (<=5 failures)"))
    .submit();
```

### batchCustom

```java
public static WorkflowCondition batchCustom(SerializablePredicate<BatchContext> predicate)
```

Creates a custom batch condition evaluated against the [`BatchContext`](./batch-context).

```java
WorkflowCondition criticalBatch = WorkflowCondition.batchCustom(
    ctx -> ctx.failedItems() > 10 && ctx.isComplete());

scheduler.enqueueBatch("Critical Process")
    .forEach(items, item -> processItem(item))
    .thenBranch(criticalBatch, () -> escalateToOps(), "Escalate on >10 failures")
    .submit();
```

### batchCustom (with priority)

```java
public static WorkflowCondition batchCustom(
    SerializablePredicate<BatchContext> predicate, int priority)
```

Same as `batchCustom()` but with an explicit evaluation priority.

```java
WorkflowCondition earlyWarning = WorkflowCondition.batchCustom(
    ctx -> ctx.failedItems() > 5, -1); // evaluated first
```

## Priority System

When multiple workflow branches have conditions that evaluate to `true`, they are executed in priority order (lower values first). Branches with the same priority execute in definition order.

```java
scheduler.enqueue(() -> processData())
    // Priority 0 (default) -- evaluated first if tied
    .when(result -> result.isSuccess(), () -> archiveResults())
    // Priority 1 -- evaluated after default
    .when(result -> result.isSuccess() && result.getExecutionTimeMsOrZero() > 30000,
          () -> logSlowExecution(), 1)
    // Priority -1 -- evaluated before default
    .when(result -> result.isFailure() && result.getError().contains("CRITICAL"),
          () -> escalateCritical(), -1)
    .submit();
```

## WorkflowBranch {#workflowbranch}

`WorkflowBranch` pairs a `WorkflowCondition` with a task and optional description.

```java
public record WorkflowBranch(
    WorkflowCondition condition,
    Serializable task,
    String description
) implements Serializable
```

### Record Components

| Component | Type | Description |
|---|---|---|
| `condition` | `WorkflowCondition` | When this branch should fire |
| `task` | `Serializable` | The serialized task to execute |
| `description` | `String` | Optional human-readable label for monitoring |

### Factory Methods

#### of (with description)

```java
public static WorkflowBranch of(
    WorkflowCondition condition, Serializable task, String description)
```

Creates a branch with a descriptive label.

#### of (without description)

```java
public static WorkflowBranch of(WorkflowCondition condition, Serializable task)
```

Creates a branch without a description.

### getPriority

```java
public int getPriority()
```

Returns the priority from the underlying condition. Used for ordering when multiple branches match.

### Usage

`WorkflowBranch` is typically created implicitly by `JobBuilder` and `BatchBuilder` methods. You rarely construct them directly:

```java
// These builder methods create WorkflowBranch instances internally:
scheduler.enqueue(() -> processData())
    .thenOnSuccess(() -> archive())        // creates WorkflowBranch with SUCCESS condition
    .thenOnFailure(() -> alert())          // creates WorkflowBranch with FAILURE condition
    .when(result -> ..., () -> action())   // creates WorkflowBranch with CUSTOM condition
    .submit();

// Access branches via the builder (advanced):
List<WorkflowBranch> branches = builder.workflowBranches();
```

## See Also

- [JobBuilder Workflow Methods](./job-builder#workflow-methods)
- [BatchBuilder Reference](./batch-builder)
- [JobResult Reference](./job-result)
- [BatchContext Reference](./batch-context)

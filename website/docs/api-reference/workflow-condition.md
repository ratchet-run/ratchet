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
    .branch(WorkflowCondition.success(), () -> archiveResults(), "Archive on success")
    .branch(WorkflowCondition.failure(), () -> notifyAdmins(), "Alert on failure")
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

Creates a custom condition evaluated against the full [`JobResult`](./job-result). The predicate has access to success status, return value, execution time, error details, and metadata.

**Type Parameters:**
- `T` -- the type of the job's return value.

```java
// Define condition logic in a CDI bean or static utility class
@ApplicationScoped
public class JobConditions {
    public boolean isSlowSuccess(JobResult<?> result) {
        return result.isSuccess() && result.getExecutionTimeMsOrZero() > 30_000;
    }
}

// Reference the method — do NOT use an inline multi-step lambda
WorkflowCondition slowJob = WorkflowCondition.custom(
    result -> jobConditions.isSlowSuccess(result));

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
// In your CDI bean or static utility:
public static boolean isCriticalFailure(JobResult<?> result) {
    return result.isFailure() && result.getError() != null
        && result.getError().contains("CRITICAL");
}

// Priority -1 — evaluated before default branches
WorkflowCondition critical = WorkflowCondition.custom(
    JobConditions::isCriticalFailure, -1);
```

### result

```java
public static <T> WorkflowCondition result(SerializableFunction<T, Boolean> function)
```

Creates a condition based on the job's **return value** only. The function receives the return value (not the full `JobResult`) and returns a boolean.

**Type Parameters:**
- `T` -- the type of the job's return value.

```java
// Define result checks as static methods — comparison operators require a method boundary
public class InventoryConditions {
    public static boolean isHighStock(Object stock) { return ((Integer) stock) > 100; }
    public static boolean isOutOfStock(Object stock) { return ((Integer) stock) == 0; }
}

WorkflowCondition highStock = WorkflowCondition.result(InventoryConditions::isHighStock);

scheduler.enqueue(() -> inventoryService.checkStock(itemId))
    .whenResult(InventoryConditions::isHighStock, () -> placeOrder(itemId))
    .whenResult(InventoryConditions::isOutOfStock, () -> markOutOfStock(itemId))
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
// Multi-step predicates must live in a method — comparison + boolean logic won't analyze inline
public class BatchConditions {
    public static boolean hasCriticalFailures(BatchContext ctx) {
        return ctx.failedItems() > 10 && ctx.isComplete();
    }
}

WorkflowCondition criticalBatch = WorkflowCondition.batchCustom(
    BatchConditions::hasCriticalFailures);

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
public class BatchConditions {
    public static boolean hasExcessiveFailures(BatchContext ctx) {
        return ctx.failedItems() > 5;
    }
}

WorkflowCondition earlyWarning = WorkflowCondition.batchCustom(
    BatchConditions::hasExcessiveFailures, -1); // evaluated first
```

## Priority System

Workflow routing is exclusive. Ratchet evaluates lower priority numbers first and preserves registration order when priorities are equal. The first matching branch runs; every remaining sibling branch is canceled, even if its condition would also evaluate to `true`.

```java
// Conditions with multi-step logic live in a helper — the framework stores method refs as JSON
public class JobConditions {
    public static boolean isSlowSuccess(JobResult<?> r) {
        return r.isSuccess() && r.getExecutionTimeMsOrZero() > 30_000;
    }
    public static boolean isCriticalFailure(JobResult<?> r) {
        return r.isFailure() && r.getError() != null && r.getError().contains("CRITICAL");
    }
}

scheduler.enqueue(() -> processData())
    // Priority 0 (default) — single method ref on context, no helper needed
    .when(JobResult::isSuccess, () -> archiveResults())
    // Priority 1 — evaluated after default
    .when(JobConditions::isSlowSuccess, () -> logSlowExecution(), 1)
    // Priority -1 — evaluated before default
    .when(JobConditions::isCriticalFailure, () -> escalateCritical(), -1)
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

### Constructors

#### With description

```java
public WorkflowBranch(
    WorkflowCondition condition, Serializable task, String description)
```

Creates a branch with a descriptive label. `condition` must not be null.

#### Without description

```java
public WorkflowBranch(WorkflowCondition condition, Serializable task)
```

Creates a branch without a description.

### getPriority

```java
public int getPriority()
```

Returns the priority from the underlying condition. Ratchet uses it to decide evaluation order before applying registration order as the tie-break.

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

## Predicate Serialization Contract

Custom predicates (`CUSTOM`, `BATCH_CUSTOM`, `RESULT_VALUE`) are stored in the database as `JobPayload` JSON, using the same Class/Method/Args format as job task lambdas. The predicate is analyzed at **scheduling time** using ASM bytecode inspection and must resolve to a **single public method call**.

### Supported shapes

**Method reference on the context argument:**
```java
// JobResult::isSuccess — isSuccess() called on the JobResult passed at evaluation time
WorkflowCondition.custom(JobResult::isSuccess)
```

**Static method reference:**
```java
// MyConditions.check(BatchContext) — static method, no CDI bean needed
WorkflowCondition.batchCustom(MyConditions::allSucceeded)
```

**Instance method reference on a CDI bean:**
```java
// myService is injected; evaluate(JobResult) is called on the CDI-resolved instance
WorkflowCondition.custom(result -> myService.evaluate(result))
```

### Constraint: single method call

Predicates must contain exactly one method invocation. Multi-step inline logic fails at scheduling time with an `IllegalArgumentException`:

```java
// FAILS at scheduling — two operations (isSuccess + getValue) not reducible to one call
WorkflowCondition.custom(result -> result.isSuccess() && result.getValue() != null)

// FIX — wrap in a CDI bean method or a static helper
WorkflowCondition.custom(result -> myConditions.isSuccessAndNonNull(result))
```

### Why this matters

Because predicates are stored as JSON, they are stable across recompilations and deployments. Unlike JDK-serialized lambda blobs (which break when class files change), a stored `JobPayload` like `{"target":"com.example.MyConditions","method":"allSucceeded",...}` remains valid as long as the method exists with the same signature.

## See Also

- [JobBuilder Workflow Methods](./job-builder#workflow-methods)
- [BatchBuilder Reference](./batch-builder)
- [JobResult Reference](./job-result)
- [BatchContext Reference](./batch-context)

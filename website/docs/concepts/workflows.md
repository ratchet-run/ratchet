---
sidebar_position: 9
title: Workflows
description: Job chaining, conditional branching, and WorkflowCondition types
---

# Workflows

Ratchet workflows let you define multi-step job pipelines with conditional branching. Simple chains execute steps sequentially. Workflows add conditional logic -- different paths execute based on the outcome of a previous job.

## Chains: Sequential Execution

The simplest workflow is a chain. Steps execute one after another, each waiting for the previous step to complete:

```java
scheduler.enqueue(() -> validateOrder(orderId))
    .then(() -> chargePayment(orderId))
    .then(() -> fulfillOrder(orderId))
    .then(() -> sendConfirmation(orderId))
    .submit();
```

### How Chains Work

When you call `.then()`, the engine creates multiple jobs linked by `depends_on`:

```
  ┌──────────────┐  depends_on  ┌──────────────┐  depends_on  ┌──────────────┐
  │ validateOrder│◄─────────────│chargePayment │◄─────────────│ fulfillOrder │
  │ scheduled_time│              │ scheduled_time│              │ scheduled_time│
  │ = now        │              │ = 9999-12-31 │              │ = 9999-12-31 │
  └──────────────┘              └──────────────┘              └──────────────┘
```

All chain steps are persisted at submission time. Steps 2-N use a sentinel `scheduled_time` of `9999-12-31T23:59:59Z`, making them invisible to the Poller. When step 1 succeeds, the `ChainScheduler` sets step 2's `scheduled_time = now`, releasing it for polling. This pattern continues until the chain completes.

### Chain Failure

If any step fails permanently (exhausts retries or hits `@DoNotRetry`), all downstream steps are canceled. The `ChainScheduler.cancelChain()` uses depth-first traversal to recursively cancel all dependents:

```
  Step 1: SUCCEEDED
  Step 2: FAILED (permanent)
  Step 3: CANCELED (cascaded)
  Step 4: CANCELED (cascaded)
```

This prevents executing steps that depend on data from a step that never completed.

## Conditional Branching

Workflows extend chains with conditions. Instead of always executing the next step, the engine evaluates predicates against the job's result:

```java
scheduler.enqueue(() -> analyzeData(dataId))
    .thenOnSuccess(() -> archiveResults(dataId))
    .thenOnFailure(() -> notifyAdmins(dataId))
    .submit();
```

### Branching API

| Method | Condition | Description |
|--------|-----------|-------------|
| `thenOnSuccess(task)` | SUCCESS | Execute if parent succeeds |
| `thenOnFailure(task)` | FAILURE | Execute if parent fails permanently |
| `when(predicate, task)` | CUSTOM | Execute based on `JobResult` predicate |
| `when(predicate, task, priority)` | CUSTOM | Same, with evaluation priority |
| `whenResult(function, task)` | RESULT_VALUE | Execute based on return value |
| `branch(condition, task, desc)` | Any | Full control with description |

### Success/Failure Branching

The simplest conditional pattern:

```java
scheduler.enqueue(() -> paymentService.charge(invoiceId))
    .thenOnSuccess(() -> fulfillmentService.ship(invoiceId))
    .thenOnFailure(() -> customerService.notifyPaymentFailed(invoiceId))
    .submit();
```

Both `thenOnSuccess` and `thenOnFailure` create `WorkflowBranch` objects with `WorkflowCondition.success()` and `WorkflowCondition.failure()` respectively.

### Result-Based Branching

Branch based on the actual return value of a job:

```java
scheduler.enqueue(() -> scoringService.calculateScore(applicantId))
    .whenResult(score -> score > 750, () -> autoApprove(applicantId))
    .whenResult(score -> score < 500, () -> autoReject(applicantId))
    .whenResult(score -> score >= 500 && score <= 750,
                () -> manualReview(applicantId))
    .submit();
```

The `whenResult` method creates a `RESULT_VALUE` condition. The predicate function receives the job's return value (not the full `JobResult`).

### Custom Conditions on JobResult

For more complex conditions that need access to execution metadata:

```java
scheduler.enqueue(() -> etlService.processFile(fileId))
    .when(result -> result.isSuccess() && result.getExecutionTimeMs() > 30000,
          () -> performanceService.flagSlowJob(fileId))
    .when(result -> result.isFailure() && result.getError().contains("timeout"),
          () -> retryWithLargerTimeout(fileId))
    .submit();
```

The predicate receives the full `JobResult<T>` object with:

| Method | Description |
|--------|-------------|
| `isSuccess()` / `isFailure()` | Completion status |
| `getValue()` | Return value (generic typed) |
| `getError()` | Error message |
| `getException()` | Full exception |
| `getExecutionTimeMs()` | Execution duration |
| `getStartTime()` / `getEndTime()` | Timing data |
| `getMetadata(key)` | Custom key-value pairs |

### Priority-Based Evaluation

When multiple conditions might match, priority controls evaluation order:

```java
scheduler.enqueue(() -> classifyDocument(docId))
    // Priority 0 (default) -- evaluated first
    .when(result -> result.getValue().equals("URGENT"),
          () -> escalateToManager(docId))
    // Priority 1 -- evaluated second
    .when(result -> result.isSuccess(),
          () -> archiveDocument(docId),
          1)
    .submit();
```

Lower priority values are evaluated first. Branches with the same priority execute in definition order. Multiple branches can fire from a single parent -- this is fan-out, not exclusive routing.

## WorkflowCondition Types

The `WorkflowCondition` record supports these condition types:

### Job-Level Conditions

| Type | Factory Method | Expression | Description |
|------|---------------|------------|-------------|
| `SUCCESS` | `WorkflowCondition.success()` | none | Job completed successfully |
| `FAILURE` | `WorkflowCondition.failure()` | none | Job failed permanently |
| `CUSTOM` | `WorkflowCondition.custom(predicate)` | `SerializablePredicate<JobResult<T>>` | Custom predicate on full JobResult |
| `RESULT_VALUE` | `WorkflowCondition.result(function)` | `SerializableFunction<T, Boolean>` | Predicate on return value only |

### Batch-Level Conditions

| Type | Factory Method | Expression | Description |
|------|---------------|------------|-------------|
| `BATCH_SUCCESS` | `WorkflowCondition.batchSuccess()` | none | All children succeeded |
| `BATCH_FAILURE` | `WorkflowCondition.batchFailure()` | none | One or more children failed |
| `BATCH_SUCCESS_RATE` | `WorkflowCondition.successRate(0.95)` | `Double` (0.0-1.0) | Success rate meets threshold |
| `BATCH_FAILURE_COUNT` | `WorkflowCondition.failureCount(5)` | `Integer` | Failure count within limit |
| `BATCH_CUSTOM` | `WorkflowCondition.batchCustom(pred)` | `SerializablePredicate<BatchContext>` | Custom predicate on BatchContext |

### Using Conditions Directly

For full control, use the `branch()` method with a `WorkflowCondition`:

```java
scheduler.enqueue(() -> processBatch(batchId))
    .branch(
        WorkflowCondition.custom(
            result -> result.isSuccess() &&
                      result.getExecutionTimeMsOrZero() < 5000),
        () -> fastPathService.optimize(batchId),
        "Optimize if processing was fast")
    .branch(
        WorkflowCondition.failure(),
        () -> manualReviewService.flag(batchId),
        "Flag for manual review on failure")
    .submit();
```

## Batch Workflows

Batch-level conditions are used on `BatchBuilder` and `StreamingBatchBuilder`:

```java
scheduler.enqueueBatch("Migration")
    .forEach(records, record -> migrate(record))

    .thenOnBatchSuccess(() -> certify())
    .thenOnBatchFailure(() -> rollback())

    .thenWhenSuccessRate(0.99, () -> sendHighQualityReport())
    .thenWhenFailureCount(100, () -> escalate())

    .thenWhenBatch(
        ctx -> ctx.isComplete() && ctx.failedItems() > 0 && ctx.successRate() > 0.9,
        () -> partialRecovery())

    .thenBranch(
        WorkflowCondition.batchCustom(
            ctx -> ctx.completedItems() > 10000, 1),
        () -> analyticsService.recordLargeBatch(),
        "Track large migrations")

    .submit();
```

## Workflow Evaluation

When a job completes, the `WorkflowScheduler`:

1. Loads all `WorkflowConditionEntity` rows linked to the job
2. Sorts conditions by priority (lower first)
3. Evaluates each condition against the job's result
4. For each matching condition, creates a new WORKFLOW_BRANCH job
5. If no conditions match and the job has chain dependents, falls back to linear chain scheduling

The `WorkflowConditionEvaluator` handles the actual evaluation, deserializing the condition expression and applying it to the appropriate context (JobResult or BatchContext).

### Serialization of Conditions

All condition expressions must be `Serializable` because they are persisted in the database as part of the `WorkflowConditionEntity`. When you write:

```java
.whenResult(score -> score > 0.8, () -> handleHighScore())
```

Both the predicate (`score -> score > 0.8`) and the task (`() -> handleHighScore()`) are serialized using Java serialization and stored in the database. They are deserialized at evaluation time.

This is why the API uses `SerializablePredicate` and `SerializableFunction` rather than plain Java functional interfaces.

## Combining Chains and Workflows

You can mix linear chains with conditional branches:

```java
scheduler.enqueue(() -> step1())
    .then(() -> step2())                          // Linear chain
    .thenOnSuccess(() -> step3OnSuccess())          // Branch on step2 success
    .thenOnFailure(() -> step3OnFailure())          // Branch on step2 failure
    .submit();
```

In this case:
- `step1` executes first
- `step2` executes when `step1` succeeds (linear chain)
- `step3OnSuccess` executes if `step2` succeeds (workflow branch)
- `step3OnFailure` executes if `step2` fails permanently (workflow branch)
- If `step1` fails, both `step2` and all branches are canceled

## Workflow Patterns

### Error Recovery Pipeline

```java
scheduler.enqueue(() -> importService.importData(source))
    .thenOnSuccess(() -> validationService.validate(source))
    .thenOnFailure(() -> cleanupService.rollback(source))
    .when(result -> result.isSuccess() &&
                    result.getMetadata("warnings", 0) > 0,
          () -> reviewService.flagForReview(source))
    .submit();
```

### Fan-Out

Multiple branches fire from one parent:

```java
scheduler.enqueue(() -> orderService.process(orderId))
    .thenOnSuccess(() -> inventoryService.reserve(orderId))
    .thenOnSuccess(() -> billingService.invoice(orderId))
    .thenOnSuccess(() -> notificationService.confirm(orderId))
    .submit();
```

All three success branches will fire when the parent succeeds.

### Threshold-Based Escalation

```java
scheduler.enqueueBatch("SLA Check")
    .forEach(services, svc -> healthCheck(svc))
    .thenWhenSuccessRate(1.0, () -> log.info("All services healthy"))
    .thenWhenSuccessRate(0.9, () -> alertService.warn("Some services degraded"))
    .thenWhenFailureCount(5, () -> alertService.critical("Major outage"))
    .submit();
```

## Related

- [Job Types](./job-types.md) -- WORKFLOW_BRANCH and CHAIN_STEP execution types
- [Batches](./batches.md) -- Batch-level workflow conditions
- [Job Lifecycle](./job-lifecycle.md) -- How workflow branches follow the state machine
- [Persistence](./persistence.md) -- WorkflowConditionEntity storage

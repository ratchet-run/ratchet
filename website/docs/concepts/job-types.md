---
sidebar_position: 3
title: Job Types
description: "SINGLE, RECURRING, BATCH, CHAIN, WORKFLOW, and SYSTEM job types"
---

# Job Types

Ratchet classifies jobs into **public types** (what users see) and **internal execution types** (what the engine uses). The public `JobType` enum describes the scheduling pattern. The internal `JobExecutionType` enum describes the execution role within that pattern.

## Public Job Types

The `JobType` enum appears in events, SPIs, and monitoring. It represents the user-visible category:

| Type | Description | Created By |
|------|-------------|------------|
| `SINGLE` | One-time execution at a scheduled time | `enqueue()`, `schedule()` |
| `RECURRING` | Automatically rescheduled on a cron expression | `recurring()`, `@Recurring` |
| `BATCH` | Coordinated parallel execution of many items | `enqueueBatch()`, `streamingBatch()` |
| `CHAIN` | Sequential multi-step pipeline | `then()` on JobBuilder |
| `WORKFLOW` | Conditional branching based on job results | `thenOnSuccess()`, `when()`, `branch()` |
| `SYSTEM` | Framework-managed internal work | Engine only (not user-creatable) |

## Internal Execution Types

The `JobExecutionType` enum adds granularity the engine needs for routing:

| Execution Type | Maps to Public Type | Role |
|---------------|--------------------|----|
| `SINGLE` | SINGLE | Standard one-time job |
| `RECURRING` | RECURRING | Recurring master job |
| `BATCH_PARENT` | BATCH | Batch coordinator (tracks progress, no work) |
| `BATCH_CHILD` | BATCH | Individual item within a batch |
| `CHAIN_STEP` | CHAIN | One step in a sequential chain |
| `WORKFLOW_BRANCH` | WORKFLOW | Conditional branch job |
| `WORKFLOW_JOIN` | WORKFLOW | Join point for workflow convergence |
| `DLQ_ALERT` | SYSTEM | Dead letter queue alert tracking |

This separation lets external observers see clean semantic categories while the engine routes jobs to the correct handler based on their execution role.

## SINGLE Jobs

The most common type. A SINGLE job executes exactly once at its scheduled time. It supports all standard features: retries, timeouts, priorities, tags, callbacks, and idempotency keys.

```java
// Immediate execution
scheduler.enqueue(() -> orderService.processOrder(orderId))
    .withPriority(JobPriority.HIGH)
    .withMaxRetries(3)
    .submit();

// Delayed execution
scheduler.schedule(Duration.ofMinutes(30), () -> reminderService.send(userId))
    .submit();
```

After a SINGLE job completes (succeeds or permanently fails), it is eligible for archival. It does not reschedule itself.

## RECURRING Jobs

Recurring jobs execute on a cron schedule. Each execution spawns a new job instance, so the recurring "master" persists indefinitely while its individual runs follow the normal lifecycle.

### Annotation-Based

The simplest way to create recurring jobs:

```java
@ApplicationScoped
public class MaintenanceService {

    @Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
    public void performCleanup() {
        // Runs at 2 AM daily
    }

    @Recurring(
        cron = "0 */15 * * * ?",
        zone = "America/New_York",
        maxRetries = 5,
        backoffPolicy = BackoffPolicy.EXPONENTIAL,
        tags = {"health", "monitoring"}
    )
    public void healthCheck(JobContext context) {
        context.logger().info("Running health check");
    }
}
```

At startup, the `RecurringJobProcessor` CDI bean scans for `@Recurring` methods, validates them, and registers them with the scheduler. The annotation's `id` (or auto-generated fully-qualified method name) serves as the business key, ensuring exactly one active master per annotation.

### Programmatic API

```java
scheduler.scheduleRecurring(
        "0 0 * * * ?",
        ZoneId.of("UTC"),
        () -> reportService.generateHourly())
    .withOptions(JobOptions.defaults()
        .withMaxRetries(3)
        .withTimeout(Duration.ofMinutes(10)))
    .withTags(List.of("reports"))
    .withBusinessKey("hourly-report")
    .submit();
```

### How Recurring Execution Works

1. The recurring master job stores the cron expression and timezone
2. `RecurringScheduler` calculates the next fire time from the cron expression
3. When the fire time arrives, a new SINGLE job is created for that execution
4. After the execution completes, the next fire time is recalculated
5. This continues until the recurring job is canceled

The `@Recurring` annotation supports these properties:

| Property | Default | Description |
|----------|---------|-------------|
| `cron` | (required) | Quartz cron expression |
| `zone` | `"UTC"` | Timezone for cron evaluation |
| `name` | method name | Human-readable name |
| `id` | class + method | Business key for idempotency |
| `priority` | `5` (NORMAL) | Priority on 1-10 scale |
| `maxRetries` | `3` | Retry attempts per execution |
| `backoffPolicy` | `EXPONENTIAL` | Backoff between retries |
| `backoffDelayMs` | `1000` | Base delay for backoff |
| `timeoutSeconds` | `3600` | Max execution time |
| `enabled` | `"true"` | Supports property placeholders |
| `tags` | `{}` | Tags for filtering |

## BATCH Jobs

Batch jobs process a collection of items in parallel. Internally, a BATCH_PARENT job is created to track overall progress, and individual BATCH_CHILD jobs are created for each item.

```java
scheduler.enqueueBatch("Import Users")
    .forEach(users, user -> userService.importUser(user))
    .onProgress(ctx -> log.info("{}% complete", ctx.percentDone()))
    .thenOnBatchSuccess(() -> notificationService.sendImportComplete())
    .thenOnBatchFailure(() -> alertService.sendImportFailed())
    .submit();
```

The parent job doesn't execute work itself -- it monitors child job completion. When all children finish, the parent evaluates batch-level workflow conditions and fires the appropriate branches.

See [Batches](./batches.md) for details on `BatchBuilder`, `StreamingBatchBuilder`, and chunk processing.

## CHAIN Jobs

Chains execute tasks sequentially. Each step depends on the previous step's completion. Internally, each step is a `CHAIN_STEP` job linked by the `depends_on` column.

```java
scheduler.enqueue(() -> validateData())
    .then(() -> transformData())
    .then(() -> loadData())
    .then(() -> sendNotification())
    .submit();
```

### How Chains Work Internally

1. All steps are persisted at submission time
2. Steps 2-N are created with `scheduled_time = 9999-12-31T23:59:59Z` (a sentinel value that makes them invisible to the Poller)
3. When step 1 succeeds, `ChainScheduler.scheduleNext()` sets step 2's `scheduled_time = now`, making it eligible for polling
4. This continues until the final step completes

**Failure cascading:** If any step fails permanently (exhausts retries), all downstream steps are canceled recursively using depth-first traversal.

## WORKFLOW Jobs

Workflows extend chains with conditional branching. Instead of always executing the next step, the engine evaluates `WorkflowCondition` predicates against the job's result to decide which branches fire.

```java
scheduler.enqueue(() -> analyzeData())
    .thenOnSuccess(() -> archiveResults())
    .thenOnFailure(() -> notifyAdmins())
    .whenResult(score -> score > 0.8, () -> triggerHighScoreWorkflow())
    .submit();
```

Workflow branches are stored as `WorkflowConditionEntity` rows linked to the parent job. When the parent completes, `WorkflowScheduler` loads all conditions, evaluates them in priority order, and schedules the matching branches.

Multiple branches can fire from a single parent -- unlike chains (which are linear), workflows support fan-out.

See [Workflows](./workflows.md) for the full condition type catalog and branching patterns.

## SYSTEM Jobs

System jobs are framework-managed and not directly creatable through the public API. Currently, the `DLQ_ALERT` execution type is the only system job, used internally for dead letter queue alert tracking.

## Type Routing in the Engine

When a job completes, the `PostExecutionHandler` routes the next action based on the job's execution type:

| Execution Type | On Success | On Permanent Failure |
|---------------|------------|---------------------|
| SINGLE | Invoke callbacks | Invoke callbacks, move to DLQ |
| BATCH_CHILD | Update parent progress | Update parent progress (as failure) |
| CHAIN_STEP | Schedule next step | Cancel downstream steps |
| WORKFLOW_BRANCH | Evaluate conditions, schedule matches | Evaluate conditions (FAILURE branches may fire) |
| RECURRING | Calculate next fire time | Calculate next fire time (retry if configured) |

This routing is the reason the internal `JobExecutionType` exists -- the engine needs to know not just "this is a BATCH job" but "this is a BATCH_CHILD job" to route correctly.

## Related

- [Batches](./batches.md) -- BatchBuilder and StreamingBatchBuilder
- [Workflows](./workflows.md) -- Conditional branching and WorkflowCondition
- [Scheduling](./scheduling.md) -- Immediate, delayed, and cron-based scheduling
- [Job Lifecycle](./job-lifecycle.md) -- State machine for all job types

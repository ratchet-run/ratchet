---
sidebar_position: 4
title: Scheduling
description: Immediate, delayed, and cron-based job scheduling with adaptive polling
---

# Scheduling

Ratchet supports three scheduling modes: immediate execution, delayed execution, and cron-based recurring schedules. All scheduling operations are transactional -- jobs are persisted atomically with your business data.

## Scheduling Modes

### Immediate Execution

The simplest mode. The job is persisted with `scheduled_time = now` and becomes eligible for the next poll cycle.

```java
// Fire-and-forget (no configuration needed)
scheduler.enqueueNow(() -> emailService.sendWelcome(userId));

// With configuration
scheduler.enqueue(() -> orderService.processOrder(orderId))
    .withPriority(JobPriority.HIGH)
    .withMaxRetries(3)
    .submit();
```

For truly urgent work, mark the job as `immediate()` or use `CRITICAL` priority. This triggers a wakeup notification via the `ClusterCoordinator` SPI, causing all cluster nodes to poll immediately instead of waiting for the next adaptive polling cycle:

```java
scheduler.enqueue(() -> alertService.sendCriticalAlert(incident))
    .withPriority(JobPriority.CRITICAL)  // auto-triggers wakeup
    .submit();

// Or explicitly:
scheduler.enqueue(() -> paymentService.capture(paymentId))
    .immediate()  // triggers cluster wakeup
    .submit();
```

### Delayed Execution

Schedule a job to run after a specified delay:

```java
// Send a reminder in 30 minutes
scheduler.schedule(Duration.ofMinutes(30), () -> reminderService.send(userId))
    .submit();

// Retry with custom delay
scheduler.schedule(Duration.ofHours(1), () -> syncService.retryFailed(batchId))
    .withMaxRetries(5)
    .submit();
```

The delay is computed at submission time: `scheduled_time = now + delay`. The job stays invisible to the Poller until that time passes.

### Cron-Based Recurring

Recurring jobs use Quartz cron expressions (6-7 fields):

```
second minute hour day-of-month month day-of-week [year]
```

#### Annotation-Based (Declarative)

The most common approach for recurring jobs:

```java
@ApplicationScoped
public class MaintenanceService {

    @Recurring(cron = "0 0 2 * * ?", name = "Nightly Cleanup")
    public void performCleanup() {
        // Runs at 2 AM daily (UTC)
    }

    @Recurring(
        cron = "0 */15 * * * ?",
        zone = "America/New_York",
        name = "Health Check",
        priority = 8,
        maxRetries = 5,
        backoffPolicy = BackoffPolicy.EXPONENTIAL,
        tags = {"health", "monitoring"}
    )
    public void healthCheck(JobContext context) {
        context.logger().info("Health check running");
    }
}
```

**Method requirements:**
- Must be `public`
- Must be on a CDI-managed bean (`@ApplicationScoped`, `@Stateless`, etc.)
- Must have either no parameters or a single `JobContext` parameter
- Return type can be anything (return values are ignored)

At startup, `RecurringJobProcessor` scans all CDI beans for `@Recurring` methods, validates them via `RecurringMethodValidator`, and registers them with the scheduler. The annotation's `id` property (defaulting to the fully qualified class + method name) is used as the business key to ensure exactly one active recurring master exists per annotation.

#### Programmatic API

For runtime-defined schedules:

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

#### Cron Expression Examples

| Expression | Schedule |
|-----------|----------|
| `0 0 2 * * ?` | Every day at 2:00 AM |
| `0 */15 * * * ?` | Every 15 minutes |
| `0 0 9 ? * MON` | Every Monday at 9:00 AM |
| `0 0 0 1 * ?` | First day of every month at midnight |
| `0 30 8 ? * MON-FRI` | Weekdays at 8:30 AM |
| `0 0 */4 * * ?` | Every 4 hours |

#### Timezone Handling

Cron expressions are evaluated in the specified timezone. This matters for schedules that cross DST boundaries:

```java
@Recurring(
    cron = "0 0 2 * * ?",
    zone = "America/New_York"  // respects EST/EDT transitions
)
public void nightlyJob() { ... }
```

If no timezone is specified, UTC is used.

## The `enqueue` vs `schedule` vs `recurring` API

| Method | Delay | Recurring | Returns |
|--------|-------|-----------|---------|
| `enqueue(task)` | None (immediate) | No | `JobBuilder` |
| `enqueueNow(task)` | None (immediate) | No | `JobHandle` (no further config) |
| `schedule(delay, task)` | Specified duration | No | `JobBuilder` |
| `scheduleRecurring(cron, zone, task)` | Cron-based | Yes | `RecurringJobBuilder` |
| `enqueueBatch(name)` | None | No | `BatchBuilder` |
| `streamingBatch(name)` | None | No | `StreamingBatchBuilder` |

## Idempotency and Deduplication

Every job gets an **idempotency key** -- a globally unique identifier that prevents duplicate creation. By default, a UUID is auto-generated at builder creation time. You can provide a custom key for application-level deduplication:

```java
// Same webhook delivery ID = same job, forever
scheduler.enqueue(() -> processWebhook(payload))
    .withIdempotencyKey(webhookDeliveryId)
    .submit();
```

The idempotency key is enforced by a UNIQUE constraint in the database. If a duplicate key is submitted, the constraint violation is detected and the duplicate is silently rejected.

**Business keys** serve a different purpose -- they prevent concurrent execution of the same logical operation, but allow re-runs after completion:

```java
// Only one sync per user at a time, but re-runs allowed
scheduler.enqueue(() -> syncUser(userId))
    .withBusinessKey("sync-user-" + userId)
    .submit();
```

Business keys are enforced as unique only among active jobs (PENDING, RUNNING, PAUSED). Once a job reaches a terminal state, the key is freed for reuse.

| Mechanism | Scope | Lifetime | Use Case |
|-----------|-------|----------|----------|
| Idempotency Key | Globally unique | Forever | Webhook IDs, payment request IDs |
| Business Key | Active-unique | Until terminal state | "One sync per user at a time" |

## Job Priority

Priority determines which jobs are claimed first when multiple are eligible. The `JobPriority` enum defines five levels:

| Priority | Ordinal | Behavior |
|----------|---------|----------|
| `LOWEST` | 0 | Last to be picked up |
| `LOW` | 1 | Below normal |
| `NORMAL` | 2 | Default for all jobs |
| `HIGH` | 3 | Preferred over normal |
| `CRITICAL` | 4 | Picked first; triggers immediate wakeup |

The Poller's claim query orders by **effective priority**, then due time. Effective priority is the persisted priority plus an age-based boost:

```text
effective_priority = priority + floor(wait_minutes / priorityBoostIntervalMinutes)
```

With the default 15-minute boost interval, long-waiting low-priority jobs can outrank newer high-priority jobs. This is intentional starvation prevention. Set `RatchetOptions.builder().store(s -> s.priorityBoostIntervalMinutes(0))` to disable boosting and use raw priority ordering.

```java
scheduler.enqueue(() -> billingService.process(invoice))
    .withPriority(JobPriority.HIGH)
    .submit();
```

:::caution
Priority ordinal values are persisted in the database. Do not reorder or insert between existing enum values -- this would silently corrupt the priority of existing jobs.
:::

## Job Replacement

To replace an existing job with a new definition:

```java
JobHandle newHandle = scheduler.replace(
    existingJobId,
    Duration.ofMinutes(5),
    () -> updatedService.process(newData),
    JobOptions.defaults().withMaxRetries(2)
);
```

The old job is marked as superseded (`superseded_by` column points to the new job), and the new job takes its place. This is useful for debouncing -- replacing a pending job with an updated version instead of creating duplicates.

## Adaptive Polling

The engine doesn't poll at a fixed interval. The `PollingStrategy` dynamically adjusts the polling delay based on job availability patterns:

```
                    Wakeup Signal
                         │
                         ▼
    ┌───────────────────────────────────────┐
    │            BURST MODE                 │
    │   Delay: 500ms (aggressive)           │
    │   Exits after idle threshold          │
    └───────────────┬───────────────────────┘
                    │ No jobs found
                    ▼
    ┌───────────────────────────────────────┐
    │           NORMAL MODE                 │
    │   Delay: 2-30 seconds (adaptive)      │
    │   Based on rolling job count average  │
    └───────────────┬───────────────────────┘
                    │ No jobs for 5+ minutes
                    ▼
    ┌───────────────────────────────────────┐
    │           DEEP IDLE                   │
    │   Delay: 60 seconds                   │
    │   Exits on wakeup signal              │
    └───────────────────────────────────────┘
```

The strategy considers multiple factors:

- **Rolling window:** Tracks job counts over the last 10 polls to identify trends
- **Full batch detection:** When consecutive polls return full batches, polling becomes more aggressive
- **Load factor:** Adjusts based on thread pool utilization
- **Idle detection:** Progressive backoff when no jobs are found
- **Deep idle:** Extended delay after 5 minutes of inactivity
- **Burst mode:** Aggressive polling after a wakeup notification

This means Ratchet is responsive to new work (sub-second latency with wakeup) while consuming minimal resources during quiet periods.

## Resource Limiting

Jobs can declare a shared resource they require, enabling cross-job-type concurrency control:

```java
// Limit concurrent payment API calls to the configured pool size
scheduler.enqueue(() -> paymentService.charge(paymentId))
    .withResource("payment-api")
    .submit();
```

When a job with a resource runs, it attempts to acquire a permit from the `ResourcePermitService`. If no permits are available (resource at capacity), the job is rescheduled with a delay -- without counting as a retry attempt. This is distinct from retry logic: the job isn't failing, it's waiting for capacity.

## Related

- [Execution Model](./execution-model.md) -- How polling and execution work together
- [Job Lifecycle](./job-lifecycle.md) -- State transitions triggered by scheduling
- [Retry Strategies](./retry-strategies.md) -- Backoff between retry attempts
- [Clustering](./clustering.md) -- Wakeup notifications across nodes

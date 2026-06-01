---
sidebar_position: 9
title: Job Options and Enums
description: Reference for JobOptions record, JobPriority, BackoffPolicy, and JobType enums.
---

# Job Options and Enums

Configuration types used throughout the Ratchet API for controlling job execution behavior.

## JobOptions

Immutable configuration record for job execution behavior and policies. `JobOptions` is used internally by `JobBuilder` and can be passed directly to `RecurringJobBuilder.withOptions()`.

**Package:** `run.ratchet.api`
**Type:** `record JobOptions(JobPriority priority, int maxRetries, BackoffPolicy backoffPolicy, Duration backoffParam, int timeoutSec)`

### Record Components

| Component | Type | Default | Description |
|---|---|---|---|
| `priority` | `JobPriority` | `NORMAL` | Execution priority |
| `maxRetries` | `int` | `0` | Maximum retry attempts (0 = no retries) |
| `backoffPolicy` | `BackoffPolicy` | `NONE` | Retry delay strategy |
| `backoffParam` | `Duration` | `Duration.ZERO` | Base delay for backoff calculations |
| `timeoutSec` | `int` | `0` | Maximum execution time in seconds (0 = no timeout) |

### defaults

```java
public static JobOptions defaults()
```

Creates a `JobOptions` with default values: NORMAL priority, no retries, no backoff, no timeout.

```java
JobOptions opts = JobOptions.defaults();
```

### withPriority

```java
public JobOptions withPriority(JobPriority p)
```

Returns a new `JobOptions` with the specified priority.

```java
JobOptions opts = JobOptions.defaults().withPriority(JobPriority.HIGH);
```

### withMaxRetries

```java
public JobOptions withMaxRetries(int r)
```

Returns a new `JobOptions` with the specified retry limit.

```java
JobOptions opts = JobOptions.defaults().withMaxRetries(5);
```

### withBackoff

```java
public JobOptions withBackoff(BackoffPolicy bp, Duration param)
```

Returns a new `JobOptions` with the specified backoff configuration.

```java
JobOptions opts = JobOptions.defaults()
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2));
```

### withTimeout

```java
public JobOptions withTimeout(Duration t)
```

Returns a new `JobOptions` with the specified execution timeout.

```java
JobOptions opts = JobOptions.defaults().withTimeout(Duration.ofMinutes(10));
```

### Example: Recurring Job Options

```java
scheduler.scheduleRecurring(
        "0 0 * * * ?", ZoneId.of("UTC"),
        () -> healthCheck.run())
    .withOptions(JobOptions.defaults()
        .withPriority(JobPriority.HIGH)
        .withMaxRetries(2)
        .withBackoff(BackoffPolicy.FIXED, Duration.ofSeconds(30))
        .withTimeout(Duration.ofMinutes(5)))
    .submit();
```

### Relationship to JobBuilder

When you use `JobBuilder` methods like `withMaxRetries()`, `withBackoff()`, `withPriority()`, and `withTimeout()`, they modify the internal `JobOptions`. You can read the current options via `builder.opts()`.

## JobPriority

Enum defining execution priority levels. Higher ordinal values indicate higher priority. Jobs with higher priority are executed before lower priority ones when multiple jobs are queued.

**Package:** `run.ratchet.api`

| Value | Ordinal | Description |
|---|---|---|
| `LOWEST` | 0 | Lowest priority, executed last |
| `LOW` | 1 | Below normal priority |
| `NORMAL` | 2 | Default priority |
| `HIGH` | 3 | Above normal priority |
| `CRITICAL` | 4 | Highest priority, executed first; triggers immediate cluster wakeup |

:::warning
Ordinal values are persisted in the database. Do **not** reorder, insert between, or remove existing entries. New priorities must only be appended to the end of the enum.
:::

```java
scheduler.enqueue(() -> criticalTask())
    .withPriority(JobPriority.CRITICAL)
    .submit();
```

### Priority in @Recurring

The `@Recurring` annotation uses a 1-10 integer scale that maps to `JobPriority` buckets:

| Annotation Value | JobPriority |
|---|---|
| 1-2 | `LOWEST` |
| 3-4 | `LOW` |
| 5-6 | `NORMAL` (default: 5) |
| 7-8 | `HIGH` |
| 9-10 | `CRITICAL` |

```java
@Recurring(cron = "0 0 * * * ?", priority = 9) // maps to CRITICAL
public void urgentTask() { }
```

## BackoffPolicy

Enum defining retry delay strategies applied between job execution attempts.

**Package:** `run.ratchet.api`

### NONE

No delay between retries. Jobs are retried immediately after failure. Suitable for non-transient errors like validation failures.

```java
.withBackoff(BackoffPolicy.NONE, Duration.ZERO)
```

### FIXED

Constant delay between each retry attempt. Suitable for rate-limited services.

**Example with `backoffParam = 5 seconds`:**

| Attempt | Delay |
|---|---|
| 1 (initial) | immediate |
| 2 | 5 seconds |
| 3 | 5 seconds |
| 4 | 5 seconds |

```java
.withMaxRetries(3)
.withBackoff(BackoffPolicy.FIXED, Duration.ofSeconds(5))
```

### EXPONENTIAL

Delays grow exponentially (doubling) with each attempt. Ideal for reducing load on stressed systems. The delay is capped at a reasonable maximum (24 hours).

**Example with `backoffParam = 1 second`:**

| Attempt | Delay |
|---|---|
| 1 (initial) | immediate |
| 2 | 1 second |
| 3 | 2 seconds |
| 4 | 4 seconds |
| 5 | 8 seconds |

```java
.withMaxRetries(5)
.withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(1))
```

## JobType

Enum representing high-level job categories. These describe the user-visible scheduling pattern, not internal execution mechanics.

**Package:** `run.ratchet.api`

| Value | Description |
|---|---|
| `SINGLE` | Standard one-time execution job |
| `RECURRING` | Automatically rescheduled job based on cron expression |
| `BATCH` | Coordinated batch of child jobs |
| `CHAIN` | Sequential multi-step pipeline |
| `WORKFLOW` | Conditional branching execution |
| `SYSTEM` | Scheduler-managed internal work (not user-creatable) |

`JobType` appears in events (e.g., `AbstractJobSchedulerEvent.getJobType()`) and SPI callbacks (e.g., `MetricsCollector.jobStarted()`).

```java
scheduler.addEventListener(event -> {
    if (event instanceof JobStartedEvent started) {
        if (started.getJobType() == JobType.BATCH) {
            log.info("Batch job started: {}", started.getJobId());
        }
    }
});
```

## See Also

- [JobBuilder Reference](./job-builder)
- [Annotations Reference](./annotations)
- [SPI Interfaces](./spi-interfaces)

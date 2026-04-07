---
sidebar_position: 7
title: Retry Strategies
description: BackoffPolicy, RetryPolicy SPI, and how backoff delays are calculated
---

# Retry Strategies

When a job fails and is eligible for retry, Ratchet calculates a delay before rescheduling. This delay is controlled by two mechanisms: the built-in `BackoffPolicy` enum and the `RetryPolicy` SPI.

## BackoffPolicy

The `BackoffPolicy` enum defines three built-in strategies:

### NONE -- Immediate Retry

No delay between retry attempts. The job is rescheduled with `scheduled_time = now`.

```java
scheduler.enqueue(() -> quickValidation(data))
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.NONE, Duration.ZERO)
    .submit();
```

Best for failures that are unlikely to be caused by temporary conditions (e.g., retrying with slightly different parameters, or when the fix is expected to be deployed between attempts).

### FIXED -- Constant Delay

A constant delay is applied between each retry attempt, regardless of the attempt number.

```java
scheduler.enqueue(() -> externalApi.call(payload))
    .withMaxRetries(5)
    .withBackoff(BackoffPolicy.FIXED, Duration.ofSeconds(10))
    .submit();
```

| Attempt | Delay |
|---------|-------|
| 1 (initial) | immediate |
| 2 | 10 seconds |
| 3 | 10 seconds |
| 4 | 10 seconds |

Best for rate-limited services where a predictable interval between calls is desired.

### EXPONENTIAL -- Doubling Delay

The delay doubles with each attempt, starting from the configured base. This is the most commonly used strategy for transient failures.

```java
scheduler.enqueue(() -> paymentService.charge(invoice))
    .withMaxRetries(5)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(1))
    .submit();
```

| Attempt | Delay | Cumulative Wait |
|---------|-------|----------------|
| 1 (initial) | immediate | 0s |
| 2 | 1 second | 1s |
| 3 | 2 seconds | 3s |
| 4 | 4 seconds | 7s |
| 5 | 8 seconds | 15s |

The formula is: `delay = baseMs * 2^(attempt - 1)`

**Overflow protection:** The exponent is capped at 20 (2^20 = ~1 million), and the total delay is capped at 24 hours. With a 1-second base:

| Attempt | Delay |
|---------|-------|
| 10 | ~8.5 minutes |
| 15 | ~4.5 hours |
| 20 | ~12 days (capped to 24h) |
| 21+ | 24 hours (cap) |

## Configuring Backoff

Backoff is configured per job at submission time:

```java
// On JobBuilder
scheduler.enqueue(() -> task())
    .withMaxRetries(3)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(5))
    .submit();
```

Or through `JobOptions` for recurring jobs:

```java
scheduler.scheduleRecurring("0 0 * * * ?", ZoneId.of("UTC"), () -> task())
    .withOptions(JobOptions.defaults()
        .withMaxRetries(3)
        .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(5)))
    .submit();
```

Or via the `@Recurring` annotation:

```java
@Recurring(
    cron = "0 0 * * * ?",
    maxRetries = 5,
    backoffPolicy = BackoffPolicy.EXPONENTIAL,
    backoffDelayMs = 2000
)
public void hourlySync() { ... }
```

### Default Values

If no backoff is configured:

| Property | Default |
|----------|---------|
| `maxRetries` | 0 (no retries) |
| `backoffPolicy` | NONE |
| `backoffParam` | Duration.ZERO |

A job with `maxRetries = 0` never retries -- any failure goes directly to the DLQ.

## RetryPolicy SPI

For more sophisticated retry logic, implement the `RetryPolicy` SPI:

```java
public interface RetryPolicy {
    boolean shouldRetry(int attempt, Throwable cause);
    Duration getDelay(int attempt);
}
```

### How RetryPolicy Interacts with BackoffPolicy

The engine consults both:

1. `RetryPolicy.shouldRetry(attempt, cause)` decides **whether** to retry
2. If retrying, `RetryPolicy.getDelay(attempt)` is checked first
3. If the policy returns `Duration.ZERO`, the engine falls back to `BackoffPolicyHandler.computeDelay()` using the job's configured `BackoffPolicy`
4. If the policy returns a non-zero duration, that duration is used as the delay

This layered design means the `RetryPolicy` SPI can override both the retry decision and the delay, or defer to the per-job configuration.

### Default Implementation

The default `DefaultRetryPolicy` always returns `true` for `shouldRetry()` and `Duration.ZERO` for `getDelay()`. This means it defers entirely to the job's configured `maxRetries` and `BackoffPolicy`.

### Custom RetryPolicy Example

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class SmartRetryPolicy implements RetryPolicy {

    @Override
    public boolean shouldRetry(int attempt, Throwable cause) {
        // Never retry authentication failures
        if (cause instanceof AuthenticationException) {
            return false;
        }
        // Allow up to 10 retries for network errors
        if (cause instanceof IOException) {
            return attempt <= 10;
        }
        // Default: defer to job configuration
        return true;
    }

    @Override
    public Duration getDelay(int attempt) {
        // Use custom delay for network errors
        // Return Duration.ZERO to use the job's configured backoff
        return Duration.ZERO;
    }
}
```

### When to Use RetryPolicy vs BackoffPolicy

| Scenario | Use |
|----------|-----|
| Fixed delay for all retries of a job | `BackoffPolicy.FIXED` |
| Exponential backoff | `BackoffPolicy.EXPONENTIAL` |
| Different retry behavior by exception type | Custom `RetryPolicy` |
| Global retry rules across all jobs | Custom `RetryPolicy` |
| Per-job retry configuration | `BackoffPolicy` on `JobBuilder` |

## Delay Calculation Internals

The `BackoffPolicyHandler` is a stateless utility that computes delays:

```java
public static long computeDelay(BackoffPolicy policy, int baseMs, int attempts) {
    return switch (policy) {
        case NONE -> 0L;
        case FIXED -> baseMs;
        case EXPONENTIAL -> {
            int cappedExponent = Math.min(attempts - 1, 20);
            long multiplier = 1L << cappedExponent;
            long exponentialDelay =
                (multiplier > 0 && baseMs <= MAX_DELAY / multiplier)
                    ? baseMs * multiplier
                    : MAX_DELAY;
            yield Math.min(exponentialDelay, MAX_DELAY);  // 24h cap
        }
    };
}
```

The `attempts` parameter is 1-based and represents the **next** attempt number. After the first failure, `attempts = 1`; after the second, `attempts = 2`, etc.

**Overflow safety:** The implementation uses bit-shifting (`1L << exponent`) rather than `Math.pow()` to avoid floating-point issues. It also guards against `long` overflow by checking bounds before multiplication.

## Retry Scheduling Flow

When a retry is decided:

1. **Delay calculation:** `RetryPolicy.getDelay()` is checked first; if zero, `BackoffPolicyHandler.computeDelay()` is used
2. **Rescheduling:** `scheduled_time = now + delay`, `status = PENDING`
3. **Event:** `JobRetryingEvent` is published with the attempt count and next scheduled time
4. **Wakeup:** If the delay is non-zero, a delayed wakeup is scheduled so the poller knows to check at the right time

```java
// What happens internally:
long backoff = retryPolicy.getDelay(attempt).isZero()
    ? BackoffPolicyHandler.computeDelay(
        job.getBackoffPolicy(), job.getBackoffParamMs(), attempt)
    : retryPolicy.getDelay(attempt).toMillis();

Instant newScheduledTime = Instant.now().plusMillis(backoff);
jobStore.scheduleJobRetry(job.getId(), errorMessage, newScheduledTime, attempt);
```

## Combining with @DoNotRetry

The `@DoNotRetry` annotation is checked **before** the `RetryPolicy` SPI. If the exception class is annotated with `@DoNotRetry`, the job moves directly to the DLQ regardless of retry configuration or policy:

```
Exception thrown
     │
     ▼
@DoNotRetry? ──Yes──▶ DLQ (immediate)
     │
     No
     ▼
RetryPolicy.shouldRetry()? ──No──▶ DLQ
     │
     Yes
     ▼
attempt <= maxRetries? ──No──▶ DLQ
     │
     Yes
     ▼
Calculate delay, reschedule
```

## Related

- [Error Handling](./error-handling.md) -- The full error handling pipeline
- [Job Lifecycle](./job-lifecycle.md) -- State transitions during retries
- [Scheduling](./scheduling.md) -- How rescheduled jobs become visible to the Poller

---
sidebar_position: 3
title: Custom Retry Policies
description: Implementing custom retry policies for fine-grained control over job failure recovery
---

# Custom Retry Policies

Ratchet's retry behavior is controlled by two cooperating systems: the **job-level configuration** (max retries and backoff policy set at scheduling time) and the **RetryPolicy SPI** (a global policy that can override or augment per-job settings). Together, they determine whether a failed job should be retried, and how long to wait before the next attempt.

## How Retry Decisions Work

When a job fails, the engine evaluates retry eligibility through a pipeline:

1. **@DoNotRetry check** -- If the exception (or any exception in its cause chain) is annotated with `@DoNotRetry` or matches the built-in non-retryable exception list, the job is sent directly to the dead letter flow. No further retry evaluation occurs.

2. **RetryPolicy.shouldRetry()** -- The SPI is consulted. If it returns `false`, the job is not retried regardless of remaining attempts.

3. **Max retries check** -- If the job has exhausted its configured `maxRetries`, it moves to the dead letter flow.

4. **Delay calculation** -- The `RetryPolicy.getDelay()` value takes precedence. If it returns a non-zero `Duration`, that value is used directly as the delay before the next attempt. Only when it returns `Duration.ZERO` does the engine fall back to the job's backoff policy delay.

## RetryPolicy SPI Interface

```java
package run.ratchet.spi;

public interface RetryPolicy {

    /**
     * Evaluates whether a retry attempt should be made.
     *
     * @param attempt the current retry attempt number, starting from 1
     * @param cause   the exception that caused the failure
     * @return true if another retry should be attempted; false to stop retrying
     */
    boolean shouldRetry(int attempt, Throwable cause);

    /**
     * Calculates the delay before the next retry attempt.
     *
     * @param attempt the current retry attempt number, starting from 1
     * @return the delay duration before the next attempt
     */
    Duration getDelay(int attempt);
}
```

## Default Retry Behavior

The default `DefaultRetryPolicy` is a passthrough -- it always returns `true` for `shouldRetry()` and `Duration.ZERO` for `getDelay()`. This means the job's own configuration controls retry behavior entirely:

```java
@ApplicationScoped
public class DefaultRetryPolicy implements RetryPolicy {

    @Override
    public boolean shouldRetry(int attempt, Throwable cause) {
        return true; // Defer to job-level maxRetries
    }

    @Override
    public Duration getDelay(int attempt) {
        return Duration.ZERO; // Defer to job-level backoff policy
    }
}
```

With the default policy, a job configured with `.withMaxRetries(3)` and exponential backoff will retry up to 3 times with increasing delays.

## BackoffPolicy Interaction

The job-level `BackoffPolicy` enum controls the delay pattern between retries:

| Policy | Behavior | Example (baseMs = 1000) |
|--------|----------|-------------------------|
| `NONE` | No delay -- immediate retry | 0, 0, 0, ... |
| `FIXED` | Constant delay | 1s, 1s, 1s, ... |
| `EXPONENTIAL` | Doubling delay with 24-hour cap | 1s, 2s, 4s, 8s, ... |

When a custom `RetryPolicy` returns a non-zero delay from `getDelay()`, the engine uses **that value directly** and ignores the job's backoff policy. The backoff policy is consulted only when the SPI returns `Duration.ZERO`. This means a custom policy that returns a non-zero delay takes full control of the wait time and overrides the backoff escalation:

```
Final delay = retryPolicy.getDelay(attempt).isZero()
    ? BackoffPolicyHandler.computeDelay(policy, baseMs, attempt)
    : retryPolicy.getDelay(attempt)
```

## @DoNotRetry Annotation

Mark exception types that should never be retried with `@DoNotRetry`. This annotation is checked before the `RetryPolicy` SPI, ensuring permanent failures bypass retry logic entirely:

```java
import run.ratchet.api.DoNotRetry;

@DoNotRetry("Invalid payment method cannot be fixed by retrying")
public class InvalidPaymentException extends RuntimeException {
    public InvalidPaymentException(String message) {
        super(message);
    }
}
```

The engine checks the entire cause chain. If a `@DoNotRetry` exception is wrapped inside another exception, the job is still sent to the dead letter flow:

```java
try {
    paymentGateway.charge(request);
} catch (InvalidPaymentException e) {
    // Even wrapped, the @DoNotRetry annotation is detected
    throw new RuntimeException("Payment failed", e);
}
```

### Built-in Non-Retryable Exceptions

The `DoNotRetryPolicy` also recognizes certain well-known exception types as permanently non-retryable, without requiring the annotation:

- `java.lang.IllegalArgumentException`
- `java.lang.NullPointerException`
- `java.lang.SecurityException`
- `jakarta.security.enterprise.AuthenticationException`
- `run.ratchet.api.exception.PayloadDecryptionException`
- `run.ratchet.api.exception.KeyNotFoundException`

These are validation, authorization, or unrecoverable decryption failures that cannot succeed on retry.

## Implementing Custom Retry Policies

### Exception-Based Routing

Route retry behavior based on the exception type. For example, retry transient infrastructure errors but not validation failures:

```java
import run.ratchet.spi.RetryPolicy;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class ExceptionAwareRetryPolicy implements RetryPolicy {

    @Override
    public boolean shouldRetry(int attempt, Throwable cause) {
        // Only retry transient infrastructure errors
        return isTransient(cause);
    }

    @Override
    public Duration getDelay(int attempt) {
        // Exponential backoff with jitter
        long baseMs = 1000L * (1L << Math.min(attempt - 1, 10));
        long jitter = (long) (baseMs * 0.2 * Math.random());
        return Duration.ofMillis(baseMs + jitter);
    }

    private boolean isTransient(Throwable cause) {
        if (cause == null) return false;
        if (cause instanceof IOException
            || cause instanceof TimeoutException
            || cause instanceof ConnectException) {
            return true;
        }
        // Check wrapped causes
        return isTransient(cause.getCause());
    }
}
```

### Rate-Limited Retries

Enforce a global retry budget that limits how many retries happen per time window, preventing retry storms:

```java
import run.ratchet.spi.RetryPolicy;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class RateLimitedRetryPolicy implements RetryPolicy {

    // Allow at most 100 retries per minute across all jobs
    private final Semaphore retryBudget = new Semaphore(100);
    private final AtomicInteger pendingRefills = new AtomicInteger(0);

    @Override
    public boolean shouldRetry(int attempt, Throwable cause) {
        // Deny retry if budget is exhausted
        return retryBudget.tryAcquire();
    }

    @Override
    public Duration getDelay(int attempt) {
        // Fixed 5-second delay between retries
        return Duration.ofSeconds(5);
    }

    // Call this periodically (e.g., via a scheduled task) to refill the budget
    public void refillBudget() {
        int current = retryBudget.availablePermits();
        if (current < 100) {
            retryBudget.release(100 - current);
        }
    }
}
```

### Circuit-Breaker-Aware Retries

Combine retry policy with circuit breaker awareness to stop retrying when the downstream service is known to be unavailable:

```java
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.api.exception.CircuitBreakerOpenException;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.time.Duration;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class CircuitBreakerAwareRetryPolicy implements RetryPolicy {

    @Inject
    private ResilienceStrategy resilienceStrategy;

    @Override
    public boolean shouldRetry(int attempt, Throwable cause) {
        // Don't retry if the circuit breaker rejected the call
        if (cause instanceof CircuitBreakerOpenException) {
            return false;
        }
        // Allow up to 5 retries for other failures
        return attempt <= 5;
    }

    @Override
    public Duration getDelay(int attempt) {
        // Exponential backoff: 2s, 4s, 8s, 16s, 32s
        return Duration.ofSeconds(2L * (1L << (attempt - 1)));
    }
}
```

## Testing Custom Retry Policies

### Unit Testing

Test retry decisions and delay calculations independently:

```java
class ExceptionAwareRetryPolicyTest {

    private final ExceptionAwareRetryPolicy policy = new ExceptionAwareRetryPolicy();

    @Test
    void shouldRetryOnIOException() {
        assertTrue(policy.shouldRetry(1, new IOException("Connection reset")));
    }

    @Test
    void shouldNotRetryOnIllegalArgument() {
        assertFalse(policy.shouldRetry(1, new IllegalArgumentException("Bad input")));
    }

    @Test
    void shouldRetryOnWrappedTransientException() {
        Exception wrapped = new RuntimeException("Wrapper",
            new IOException("Connection reset"));
        assertTrue(policy.shouldRetry(1, wrapped));
    }

    @Test
    void delayShouldIncreaseExponentially() {
        Duration delay1 = policy.getDelay(1);
        Duration delay2 = policy.getDelay(2);
        Duration delay3 = policy.getDelay(3);

        assertTrue(delay2.compareTo(delay1) > 0);
        assertTrue(delay3.compareTo(delay2) > 0);
    }

    @Test
    void delayShouldIncludeJitter() {
        // Run multiple times to verify jitter produces variation
        Set<Long> delays = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            delays.add(policy.getDelay(3).toMillis());
        }
        // With jitter, we should see more than one distinct value
        assertTrue(delays.size() > 1);
    }
}
```

## Best Practices

**Let job-level configuration handle the common case.** The default `RetryPolicy` defers to per-job `maxRetries` and `backoffPolicy`, which is sufficient for most workloads. Implement a custom policy only when you need cross-cutting retry logic (rate limiting, exception routing, circuit breaker awareness).

**Use `@DoNotRetry` for business exceptions.** Rather than encoding exception-type checks in your retry policy, annotate your permanent-failure exceptions directly. This makes intent clear and survives refactoring.

**Include jitter in delay calculations.** When many jobs fail simultaneously and retry at the same time, they can create thundering herd effects. Adding randomized jitter to delays spreads out retries and reduces load spikes.

**Cap your delays.** Exponential backoff without a cap can produce unreasonably long delays. The built-in `BackoffPolicyHandler` caps at 24 hours. Apply similar caps in custom policies.

**Log retry decisions.** When implementing `shouldRetry()`, log the decision with the exception type and attempt number. This makes it much easier to diagnose why jobs ended up in the dead letter queue.

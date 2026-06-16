---
sidebar_position: 6
title: Error Handling
description: What happens when a job fails -- retries, DLQ routing, @DoNotRetry, and error sanitization
---

# Error Handling

When a job throws an exception, Ratchet's error handling pipeline determines whether to retry, route to the Dead Letter Queue (DLQ), or take special action based on the exception type.

## Error Handling Pipeline

<div class="docs-diagram docs-decision-grid" role="img" aria-label="Error handling decision tree: check DoNotRetry, increment attempts on the retryable path, consult RetryPolicy, compare attempt count, then either route to DLQ or schedule a retry.">
  <div class="docs-diagram-card docs-diagram-card--danger">
    <strong>Job throws exception</strong>
    <small>The engine runs the `@DoNotRetry` check first, then increments the attempt counter only on the retryable path.</small>
  </div>

  <div class="docs-decision-row">
    <div class="docs-diagram-card">
      <strong>`@DoNotRetry` on exception?</strong>
      <small>Checked first on the thrown exception and each exception in its cause chain.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--danger">
      <strong>Yes</strong>
      <small>Move directly to DLQ.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--active">
      <strong>No</strong>
      <small>Continue to policy evaluation.</small>
    </div>
  </div>

  <div class="docs-decision-row">
    <div class="docs-diagram-card">
      <strong>`RetryPolicy.shouldRetry()`?</strong>
      <small>Custom SPI can reject retries by exception type, attempt, or external state.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--danger">
      <strong>No</strong>
      <small>Move to DLQ.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--active">
      <strong>Yes</strong>
      <small>Check the job's retry budget.</small>
    </div>
  </div>

  <div class="docs-decision-row">
    <div class="docs-diagram-card">
      <strong>`attempt <= maxRetries`?</strong>
      <small>The job's configured retry budget is the final gate.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--danger">
      <strong>No</strong>
      <small>Move to DLQ.</small>
    </div>
    <div class="docs-diagram-card docs-diagram-card--success">
      <strong>Yes</strong>
      <small>Calculate backoff and reschedule as PENDING.</small>
    </div>
  </div>
</div>

## Retry vs DLQ Decision

The engine makes three checks in order:

1. **`@DoNotRetry` annotation** -- If the thrown exception class, or any exception in its cause chain, is annotated with `@DoNotRetry`, the job skips all retry logic and moves directly to the DLQ without incrementing the attempt counter. This is checked first, before incrementing the attempt counter or consulting the RetryPolicy. The annotation is matched on the concrete exception class; it is not inherited by subclasses.

2. **`RetryPolicy.shouldRetry(attempt, cause)`** -- The SPI is consulted with the current attempt number and the exception. The default `DefaultRetryPolicy` always returns `true` (passthrough), deferring to the attempt counter. Custom implementations can reject retries based on exception type, attempt count, or external conditions.

3. **Attempt counter** -- If `attempt <= maxRetries`, the job is rescheduled with a backoff delay. Otherwise, it moves to the DLQ.

## The `@DoNotRetry` Annotation

Mark exception classes that represent permanent, non-recoverable failures:

```java
@DoNotRetry("Invalid input data cannot be fixed by retrying")
public class InvalidOrderException extends RuntimeException {
    public InvalidOrderException(String message) {
        super(message);
    }
}
```

When a job throws `InvalidOrderException`, Ratchet skips all retry attempts and moves it directly to the DLQ, regardless of how many retries are configured.

**When to use `@DoNotRetry`:**
- Validation errors (bad input data)
- Authorization failures (user doesn't have permission)
- Configuration errors (missing required settings)
- Business rule violations (order already canceled)

**When NOT to use it:**
- Network timeouts (transient, likely to succeed on retry)
- Database connection failures (infrastructure recovery)
- Rate limiting (will succeed after backoff)

The annotation's `value` attribute is an optional human-readable reason that appears in logs:

```java
@DoNotRetry("Payment method permanently declined by issuer")
public class PaymentDeclinedException extends RuntimeException { ... }
```

## Error Sanitization

Before persisting error messages to the database or publishing them in events, Ratchet sanitizes them through the `ErrorSanitizer` SPI. This prevents sensitive information from leaking into error columns.

The default `DefaultErrorSanitizer`:
- Truncates messages to a maximum length
- Strips common PII patterns (JDBC URLs with credentials, email addresses)
- Preserves the exception class name for diagnostic value

```java
// What the job throws:
throw new RuntimeException(
    "Connection failed: jdbc:mysql://admin:s3cret@db.internal:3306/prod");

// What gets stored in last_error (the whole JDBC URL is replaced):
"java.lang.RuntimeException: Connection failed: ***REDACTED***"
```

To customize, provide your own `ErrorSanitizer` implementation:

```java
@Alternative
@Priority(APPLICATION)
@ApplicationScoped
public class CustomErrorSanitizer implements ErrorSanitizer {
    @Override
    public String sanitize(Throwable ex) {
        // Your custom sanitization logic
        return ex.getClass().getSimpleName() + ": " + truncate(ex.getMessage(), 500);
    }
}
```

## Dead Letter Queue (DLQ)

When a job permanently fails (exhausts retries, `@DoNotRetry`, or `RetryPolicy` rejects), it moves to the DLQ. The DLQ is not a separate table -- it's the set of jobs with `status = FAILED` and no remaining retries.

### What Happens on DLQ Entry

1. **Status update:** The job transitions from RUNNING to FAILED via compare-and-swap
2. **Error recording:** The sanitized error message is stored in `last_error`
3. **Alert recording:** A `DlqAlertEntity` is created for audit trail
4. **Deduplication:** If the same job+error hash combination was recorded within the last hour, the alert is suppressed to prevent notification storms
5. **Event publishing:** A `JobDlqEvent` is published
6. **Downstream handling:**
   - For batch children: parent batch progress is updated (as failure)
   - For chain steps: downstream steps may receive failure notification
   - For workflow branches: FAILURE-condition branches may fire
7. **Failure callback:** The `onFailure` callback is invoked if configured

### Observing DLQ Events

```java
public void onDlq(@Observes JobDlqEvent event) {
    slackService.alert(String.format(
        "Job %s moved to DLQ after %d attempts: %s",
        event.getJobId(), event.getRetryAttempt(), event.getErrorMessage()));
}
```

### Manual Recovery

Jobs in the DLQ can be manually retried:

```java
scheduler.retryJob(jobId);
```

This resets the attempt counter to 0, clears error information, sets `scheduled_time` to now, and transitions the job from FAILED to PENDING. The job becomes immediately eligible for polling.

### Automatic Purge

The `DeadLetterService` runs a cron-based purge that removes old DLQ entries after a configurable retention period. The purge uses distributed locking to ensure only one node runs the cleanup in a cluster.

## Failure Callbacks

Configure per-job failure handlers:

```java
scheduler.enqueue(() -> importService.processFile(fileId))
    .withMaxRetries(3)
    .onFailure((ctx, error) -> {
        alertService.sendFailureAlert(ctx.jobId(), error);
        cleanupService.removePartialImport(fileId);
    })
    .submit();
```

The callback receives:
- `JobContext ctx` -- the execution context with job ID and parameters
- `Throwable error` -- the exception that caused the failure

The failure callback is invoked **only on permanent failure** (DLQ entry), not on each retry attempt. For per-retry observation, use `JobRetryingEvent`.

## Events Published During Error Handling

| Event | When | Key Fields |
|-------|------|------------|
| `JobRetryingEvent` | Each retry attempt | `jobId`, `errorMessage`, `retryAttempt`, `scheduledTime` |
| `JobDlqEvent` | Permanent failure (DLQ entry) | `jobId`, `errorMessage`, `retryAttempt` |
| `JobFailedEvent` | Terminal failure only (job reaches FAILED state) -- not fired on retryable attempts | `jobId`, `errorMessage`, `retryAttempt` |

## Circuit Breaker Integration

When a circuit breaker is OPEN for a job's target service, the job is not executed and not counted as a failure. Instead, it is rescheduled with a delay matching the circuit breaker's OPEN-to-HALF_OPEN transition window:

```java
@CircuitBreakerProtected(service = "payment-gateway")
public class PaymentService {
    public void processPayment(long paymentId) { ... }
}
```

If the circuit breaker for `payment-gateway` is OPEN, jobs targeting `PaymentService.processPayment` are rescheduled without consuming retry attempts. This prevents retry exhaustion during outages.

## Timeout as Failure

When a job exceeds its configured timeout, the worker thread is interrupted. The resulting `InterruptedException` flows through the normal failure pipeline -- `@DoNotRetry` check, `RetryPolicy` consultation, retry scheduling, or DLQ routing.

```java
scheduler.enqueue(() -> longRunningService.process(data))
    .withTimeout(Duration.ofMinutes(5))
    .withMaxRetries(2)
    .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(30))
    .submit();
```

## Related

- [Retry Strategies](./retry-strategies.md) -- BackoffPolicy, RetryPolicy SPI, delay calculations
- [Job Lifecycle](./job-lifecycle.md) -- State transitions during failure handling
- [Execution Model](./execution-model.md) -- How the executor handles exceptions

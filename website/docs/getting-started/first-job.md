---
sidebar_position: 4
title: Your First Job
description: A complete walkthrough of creating a job with retries, backoff, parameters, callbacks, and event monitoring
---

# Your First Job

The [Quick Start](./quickstart.md) showed the fire-and-forget pattern. This guide builds a complete, production-quality job with retry handling, exponential backoff, parameters, success/failure callbacks, and event monitoring. By the end, you'll understand every option on the `JobBuilder` fluent API.

:::note Prerequisite
This guide assumes you've already produced a `@ApplicationScoped RatchetOptions` bean. If you haven't, see [Configuration](./configuration.md) — the scheduler won't start without one.
:::

## The Scenario

We're building an invoice processing service. When an invoice is submitted, we need to:

1. Call an external payment gateway to charge the customer
2. Retry up to 3 times with exponential backoff if the gateway is down
3. Log progress through parameters and the job context
4. Notify the team on success or failure
5. Monitor the entire pipeline through events

## Step 1: Create the Job

Start with the `JobBuilder` fluent API. Instead of `enqueueNow`, use `enqueue` to get a builder:

```java
package com.example.billing;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobContext;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.logging.Logger;

@ApplicationScoped
public class InvoiceService {

    private static final Logger log = Logger.getLogger(InvoiceService.class.getName());

    @Inject
    JobSchedulerService scheduler;

    public JobHandle submitInvoice(long invoiceId, String customerEmail) {
        return scheduler.enqueue(() -> processInvoice(invoiceId))
            .withPriority(JobPriority.HIGH)
            .withMaxRetries(3)
            .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
            .withTimeout(Duration.ofMinutes(5))
            .withParam("customerEmail", customerEmail)
            .withParam("invoiceId", String.valueOf(invoiceId))
            .withTags("billing", "invoice")
            .withBusinessKey("invoice-" + invoiceId)
            .onSuccess(ctx -> onInvoiceProcessed(ctx))
            .onFailure((ctx, error) -> onInvoiceFailed(ctx, error))
            .submit();
    }

    public void processInvoice(long invoiceId) throws Exception {
        JobContext ctx = JobContext.current();
        ctx.logger().info("Processing invoice " + invoiceId);

        // Simulate calling the payment gateway
        chargeCustomer(invoiceId);

        ctx.logger().info("Invoice " + invoiceId + " processed successfully");
    }

    private void onInvoiceProcessed(JobContext ctx) {
        String email = ctx.param("customerEmail");
        String invoiceId = ctx.param("invoiceId");
        log.info("Invoice " + invoiceId + " completed. Notifying " + email);
        // Send confirmation email...
    }

    private void onInvoiceFailed(JobContext ctx, Throwable error) {
        String invoiceId = ctx.param("invoiceId");
        log.severe("Invoice " + invoiceId + " failed after all retries: " + error.getMessage());
        // Alert the billing team...
    }

    private void chargeCustomer(long invoiceId) throws Exception {
        // Your payment gateway integration
    }
}
```

Let's walk through every builder method used here.

## Step 2: Configure Retries and Backoff

```java
.withMaxRetries(3)
.withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
```

These two methods work together. When `processInvoice` throws an exception:

| Attempt | Delay before retry | What happens |
|---------|-------------------|--------------|
| 1 (initial) | -- | Executes immediately |
| 2 (1st retry) | 2 seconds | `BackoffPolicy.EXPONENTIAL` with base 2s |
| 3 (2nd retry) | 4 seconds | Delay doubles each time |
| 4 (3rd retry) | 8 seconds | Last attempt |
| After 4th failure | -- | Job moves to FAILED status, `onFailure` callback fires |

Ratchet supports three backoff policies:

- **`BackoffPolicy.NONE`** -- Retry immediately. Use for non-transient errors where timing isn't the issue.
- **`BackoffPolicy.FIXED`** -- Constant delay between retries. Use for rate-limited APIs where you need predictable spacing.
- **`BackoffPolicy.EXPONENTIAL`** -- Doubling delay (capped at 5 minutes). Use for external services where you want to back off under load.

## Step 3: Set Priority and Timeout

```java
.withPriority(JobPriority.HIGH)
.withTimeout(Duration.ofMinutes(5))
```

**Priority** determines execution order when multiple jobs are queued. Ratchet defines five levels:

| Priority | Ordinal | Use case |
|----------|---------|----------|
| `LOWEST` | 0 | Background cleanup, analytics |
| `LOW` | 1 | Non-critical notifications |
| `NORMAL` | 2 | Default -- most jobs |
| `HIGH` | 3 | User-facing operations, billing |
| `CRITICAL` | 4 | System health, SLA-bound operations |

The poller selects higher-priority jobs first when multiple are available. Jobs with `CRITICAL` priority also trigger immediate wakeup notifications across cluster nodes, bypassing the normal polling delay.

**Timeout** sets the maximum wall-clock time a job can run. If `processInvoice` takes longer than 5 minutes, the job is terminated and marked as failed. Set this based on realistic expectations -- too short causes unnecessary failures, too long delays detection of stuck jobs. The default SLA timeout (when no explicit timeout is set) is 30 minutes, configurable with `RatchetOptions.timeout(t -> t.defaultSlaSeconds(...))`.

## Step 4: Pass Parameters

```java
.withParam("customerEmail", customerEmail)
.withParam("invoiceId", String.valueOf(invoiceId))
```

Parameters are simple string key-value pairs stored alongside the job. They're available during execution via `JobContext`:

```java
public void processInvoice(long invoiceId) throws Exception {
    JobContext ctx = JobContext.current();

    // Read parameters
    String email = ctx.param("customerEmail");
    String batchSize = ctx.param("batchSize", "100"); // with default

    // All parameters
    Map<String, String> allParams = ctx.params();

    ctx.logger().info("Processing invoice for " + email);
}
```

Parameters vs. lambda arguments serve different purposes:

| | Lambda arguments | Parameters |
|---|---|---|
| **How set** | Captured in the lambda expression | Set via `withParam` on the builder |
| **Types** | Any serializable type | String key-value pairs only |
| **Available to** | The job method directly | `JobContext.current().param(key)` |
| **Use case** | Primary business data (IDs, keys) | Configuration, metadata, callback context |

Parameters are especially useful in callbacks, where you need context that wasn't passed through the lambda:

```java
.onSuccess(ctx -> {
    String email = ctx.param("customerEmail");
    sendConfirmation(email);
})
```

## Step 5: Add Callbacks

```java
.onSuccess(ctx -> onInvoiceProcessed(ctx))
.onFailure((ctx, error) -> onInvoiceFailed(ctx, error))
```

**`onSuccess`** fires when the job completes without throwing an exception. It receives the `JobContext`, giving you access to the job ID, logger, and parameters.

**`onFailure`** fires when the job fails after exhausting all retries. It receives both the `JobContext` and the `Throwable` that caused the final failure.

Both callbacks are serialized alongside the job payload. They execute in the same thread that ran the job, after the main task completes or fails.

:::note Callbacks vs. events
Callbacks are scoped to a single job -- you attach them at submission time. Events (covered below) are global observers that fire for every job in the system. Use callbacks for job-specific reactions (send this user an email) and events for cross-cutting concerns (log all failures to Slack).
:::

## Step 6: Tag and Identify Jobs

```java
.withTags("billing", "invoice")
.withBusinessKey("invoice-" + invoiceId)
```

**Tags** are lowercase labels for categorization and filtering. Use them to group related jobs (all billing jobs, all import jobs) for monitoring or bulk operations. You can cancel all recurring jobs with a given tag using `scheduler.cancelRecurringJobsByTag("billing")`.

**Business key** prevents concurrent execution against the same entity. With `withBusinessKey("invoice-42")`, if another job with the same business key is already PENDING or RUNNING, the new submission is rejected. Once the first job reaches a terminal state (SUCCEEDED, FAILED, CANCELED), the key is available for reuse.

This is different from the **idempotency key**, which is globally unique forever:

```java
// Idempotency key: same webhook delivery = same job, permanently
scheduler.enqueue(() -> processWebhook(payload))
    .withIdempotencyKey(webhookDeliveryId)
    .submit();

// Business key: one sync per user at a time, re-runs allowed
scheduler.enqueue(() -> syncUser(userId))
    .withBusinessKey("sync-user-" + userId)
    .submit();
```

## Step 7: Monitor with Events

Create a CDI observer bean to monitor all jobs in the system:

```java
package com.example.billing;

import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobStartedEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.logging.Logger;

@ApplicationScoped
public class BillingJobMonitor {

    private static final Logger log = Logger.getLogger(BillingJobMonitor.class.getName());

    public void onJobStarted(@Observes JobStartedEvent event) {
        log.info("Job " + event.getJobId() + " started on node " + event.getNodeId());
    }

    public void onJobCompleted(@Observes JobCompletedEvent event) {
        log.info("Job " + event.getJobId() + " completed successfully");
    }

    public void onJobRetrying(@Observes JobRetryingEvent event) {
        log.warning("Job " + event.getJobId() + " is retrying");
    }

    public void onJobFailed(@Observes JobFailedEvent event) {
        log.severe("Job " + event.getJobId()
            + " failed (attempt " + event.getRetryAttempt() + "): "
            + event.getErrorMessage());
    }
}
```

Ratchet fires events for every job lifecycle transition. The full set of event types:

| Event | When it fires |
|-------|--------------|
| `JobStartedEvent` | Worker thread begins executing the job |
| `JobCompletedEvent` | Job finishes successfully |
| `JobFailedEvent` | Job fails after exhausting retries |
| `JobRetryingEvent` | Job failed but will be retried |
| `JobCancellingEvent` | Job cancellation requested |
| `JobCancelledEvent` | Job successfully cancelled |
| `JobPausedEvent` | Job paused via `pauseJob()` |
| `JobResumedEvent` | Job resumed via `resumeJob()` |
| `JobDlqEvent` | Job moved to the dead letter queue |
| `BatchCompletingEvent` | All batch children finished, parent completing |
| `BatchCompletedEvent` | Batch parent completed |
| `ChainStartedEvent` | First step in a job chain begins |
| `ChainCompletedEvent` | Final step in a chain completes |
| `ChainFailedEvent` | A chain step failed |
| `WorkflowBranchTriggeredEvent` | A conditional workflow branch was triggered |
| `PerformanceMetricsEvent` | Periodic performance metrics snapshot |

All events extend `AbstractJobSchedulerEvent`, which provides common fields: `jobId`, `businessKey`, `jobType`, `priority`, `nodeId`, and `timestamp`.

### Programmatic Listeners

If you need to receive events outside of CDI (or want a single handler for all event types), use the programmatic listener API:

```java
scheduler.addEventListener(event -> {
    if (event instanceof JobFailedEvent failed) {
        alertService.sendAlert(failed.getErrorMessage());
    }
});
```

## Step 8: Job Chaining

For multi-step workflows, chain jobs so they execute sequentially:

```java
scheduler.enqueue(() -> validateInvoice(invoiceId))
    .then(() -> chargeCustomer(invoiceId))
    .then(() -> sendReceipt(invoiceId))
    .withMaxRetries(2)
    .withBackoff(BackoffPolicy.FIXED, Duration.ofSeconds(5))
    .submit();
```

Each step is a separate persisted job. If `chargeCustomer` fails, `sendReceipt` never runs -- but `validateInvoice`'s results are preserved.

For conditional branching based on the outcome of a job:

```java
scheduler.enqueue(() -> assessCreditRisk(applicationId))
    .thenOnSuccess(() -> approveApplication(applicationId))
    .thenOnFailure(() -> rejectApplication(applicationId))
    .submit();
```

Or branch on the actual return value:

```java
scheduler.enqueue(() -> calculateRiskScore(applicationId))
    .when(result -> result.isSuccess() && result.getValue() < 50,
          () -> autoApprove(applicationId))
    .when(result -> result.isSuccess() && result.getValue() >= 50,
          () -> manualReview(applicationId))
    .thenOnFailure(() -> escalateToManager(applicationId))
    .submit();
```

## Step 9: Control Running Jobs

The `JobHandle` returned by `submit()` gives you the job ID. Use it with the control methods on `JobSchedulerService`:

```java
JobHandle handle = scheduler.enqueue(() -> longRunningImport(datasetId))
    .withTimeout(Duration.ofMinutes(30))
    .withBusinessKey("import-" + datasetId)
    .withTags("import", "finance")
    .submit();

UUID jobId = handle.id();

// Pause a pending or failed job
scheduler.pauseJob(jobId);

// Resume a paused job (returns to its pre-pause state)
scheduler.resumeJob(jobId);

// Cancel a pending or running job
scheduler.cancelJob(jobId);

// Retry a failed job (resets to PENDING, clears error, resets attempt counter)
scheduler.retryJob(jobId);
```

## Marking Exceptions as Non-Retryable

Some exceptions represent permanent failures that should never be retried. Annotate your exception class with `@DoNotRetry`:

```java
import run.ratchet.api.DoNotRetry;

@DoNotRetry("Invalid invoice data cannot be fixed by retrying")
public class InvalidInvoiceException extends RuntimeException {
    public InvalidInvoiceException(String message) {
        super(message);
    }
}
```

When a job throws a `@DoNotRetry` exception, the scheduler skips remaining retries and moves the job directly to the dead letter flow, regardless of the configured `maxRetries`.

## Putting It All Together

Here's the complete example with all the pieces:

```java
@ApplicationScoped
public class InvoiceService {

    private static final Logger log = Logger.getLogger(InvoiceService.class.getName());

    @Inject
    JobSchedulerService scheduler;

    public JobHandle submitInvoice(long invoiceId, String customerEmail) {
        return scheduler.enqueue(() -> processInvoice(invoiceId))
            .withPriority(JobPriority.HIGH)
            .withMaxRetries(3)
            .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofSeconds(2))
            .withTimeout(Duration.ofMinutes(5))
            .withParam("customerEmail", customerEmail)
            .withBusinessKey("invoice-" + invoiceId)
            .withTags("billing", "invoice")
            .onSuccess(ctx ->
                log.info("Invoice " + ctx.param("invoiceId") + " processed for "
                    + ctx.param("customerEmail")))
            .onFailure((ctx, error) ->
                log.severe("Invoice processing failed: " + error.getMessage()))
            .submit();
    }

    public void processInvoice(long invoiceId) throws Exception {
        JobContext ctx = JobContext.current();
        ctx.logger().info("Processing invoice " + invoiceId);
        chargeCustomer(invoiceId);
        ctx.logger().info("Invoice " + invoiceId + " charged successfully");
    }

    private void chargeCustomer(long invoiceId) throws Exception {
        // Payment gateway call
    }
}
```

## What's Next

- [Configuration](./configuration.md) -- Set up CDI producers, configure `beans.xml`, and tune runtime behavior with `RatchetOptions`

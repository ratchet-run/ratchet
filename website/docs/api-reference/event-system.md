---
sidebar_position: 12
title: Event System Reference
description: Complete reference for job lifecycle events, batch events, chain events, and event observation patterns.
---

# Event System Reference

Observing and reacting to job lifecycle events. Ratchet publishes events at every state transition. Use them for monitoring, alerting, and custom integrations.

## Listening to events

### CDI observers

Use CDI `@Observes` for type-safe event handling:

```java
@ApplicationScoped
public class JobMonitor {

    public void onJobStarted(@Observes JobStartedEvent event) {
        log.info("Job {} started on node {}", event.getJobId(), event.getNodeId());
    }

    public void onJobFailed(@Observes JobFailedEvent event) {
        log.error("Job {} failed (attempt {}): {}",
            event.getJobId(), event.getRetryAttempt(), event.getErrorMessage());
    }

    public void onJobCompleted(@Observes JobCompletedEvent event) {
        log.info("Job {} completed in {} ms",
            event.getJobId(), event.getExecutionTimeMs());
    }
}
```

### Programmatic listeners

Register listeners via [`JobSchedulerService.addEventListener()`](./job-scheduler-service#addeventlistener):

```java
scheduler.addEventListener(event -> {
    if (event instanceof JobFailedEvent failed) {
        metrics.counter("jobs.failed").increment();
    } else if (event instanceof JobCompletedEvent completed) {
        metrics.timer("jobs.duration")
            .record(completed.getExecutionTimeMs(), TimeUnit.MILLISECONDS);
    }
});
```

## Event base class

All job lifecycle events extend `AbstractJobSchedulerEvent`:

```java
public abstract class AbstractJobSchedulerEvent implements Serializable {
    public UUID getJobId()
    public String getBusinessKey()
    public JobType getJobType()
    public JobPriority getPriority()
    public String getNodeId()
    public Instant getTimestamp()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getJobId()` | `UUID` | UUIDv7 database ID of the job that triggered this event |
| `getBusinessKey()` | `String` | Human-readable business key (may be null) |
| `getJobType()` | `JobType` | Job category: SINGLE, BATCH, CHAIN, WORKFLOW, RECURRING, SYSTEM |
| `getPriority()` | `JobPriority` | Priority level of the job |
| `getNodeId()` | `String` | Identifier of the cluster node that processed this job |
| `getTimestamp()` | `Instant` | When this event was created |

## Job lifecycle events

### JobStartedEvent

Fired when a job begins execution.

```java
public class JobStartedEvent extends AbstractJobSchedulerEvent
```

No additional fields beyond the base class.

```java
public void onStarted(@Observes JobStartedEvent event) {
    log.info("[{}] Job {} ({}) started on node {}",
        event.getTimestamp(), event.getJobId(),
        event.getJobType(), event.getNodeId());
}
```

### JobCompletedEvent

Fired when a job completes successfully.

```java
public class JobCompletedEvent extends AbstractJobSchedulerEvent {
    public Long getExecutionTimeMs()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getExecutionTimeMs()` | `Long` | Execution duration in milliseconds (may be null) |

```java
public void onCompleted(@Observes JobCompletedEvent event) {
    metrics.timer("job.duration").record(
        event.getExecutionTimeMs(), TimeUnit.MILLISECONDS);
}
```

### JobFailedEvent

Fired when a job fails after its final attempt (all retries exhausted, or marked `@DoNotRetry`).

```java
public class JobFailedEvent extends AbstractJobSchedulerEvent {
    public String getErrorMessage()
    public int getRetryAttempt()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getErrorMessage()` | `String` | Error message from the failure |
| `getRetryAttempt()` | `int` | Final retry attempt number |

```java
public void onFailed(@Observes JobFailedEvent event) {
    log.error("Job {} failed on attempt {}: {}",
        event.getJobId(), event.getRetryAttempt(), event.getErrorMessage());
}
```

### JobRetryingEvent

Fired when a job is being retried after a failure.

```java
public class JobRetryingEvent extends AbstractJobSchedulerEvent {
    public String getErrorMessage()
    public int getRetryAttempt()
    public Instant getScheduledTime()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getErrorMessage()` | `String` | Error from the failure that triggered the retry |
| `getRetryAttempt()` | `int` | Current retry attempt number |
| `getScheduledTime()` | `Instant` | When the retry is scheduled (after backoff) |

```java
public void onRetrying(@Observes JobRetryingEvent event) {
    log.warn("Job {} retrying (attempt {}), next run at {}",
        event.getJobId(), event.getRetryAttempt(), event.getScheduledTime());
}
```

### JobCancelledEvent

Fired when a job cancellation is confirmed.

```java
public class JobCancelledEvent extends AbstractJobCancellationEvent
```

```java
public String getPreviousStatus()
public Long getExecutionTimeMs()
```

| Method | Return Type | Description |
|---|---|---|
| `getPreviousStatus()` | `String` | Status before cancellation |
| `getExecutionTimeMs()` | `Long` | Execution duration in milliseconds when known |

```java
public void onCancelled(@Observes JobCancelledEvent event) {
    log.info("Job {} canceled", event.getJobId());
}
```

### JobsBulkCancelledEvent

Fired exactly once per successful bulk cancel-by-tag operation, when at least one job was cancelled. Produced by [`cancelJobsByTag()`](./job-scheduler-service) and [`cancelRecurringJobsByTag()`](./job-scheduler-service).

Bulk cancellation does not carry a single job id, business key, or priority, so this event does **not** extend `AbstractJobSchedulerEvent`.

```java
public class JobsBulkCancelledEvent implements Serializable {
    public String getTag()
    public int getCount()
    public Instant getCancelledAt()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getTag()` | `String` | Tag used to select jobs for cancellation |
| `getCount()` | `int` | Number of jobs successfully cancelled |
| `getCancelledAt()` | `Instant` | When the bulk operation completed |

```java
public void onBulkCancelled(@Observes JobsBulkCancelledEvent event) {
    log.info("Cancelled {} jobs tagged {}", event.getCount(), event.getTag());
}
```

### JobPausedEvent

Fired when a job is paused.

```java
public class JobPausedEvent extends AbstractJobSchedulerEvent
```

No additional fields.

### JobResumedEvent

Fired when a paused job is resumed.

```java
public class JobResumedEvent extends AbstractJobSchedulerEvent
```

No additional fields.

### JobDlqEvent

Fired when a job is moved to the Dead Letter Queue after exhausting all retries.

```java
public class JobDlqEvent extends AbstractJobSchedulerEvent {
    public String getErrorMessage()
    public int getRetryAttempt()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getErrorMessage()` | `String` | Error from the final failure |
| `getRetryAttempt()` | `int` | Total retry attempts before DLQ |

```java
public void onDlq(@Observes JobDlqEvent event) {
    alertService.sendDlqAlert(event.getJobId(), event.getErrorMessage());
    metrics.counter("jobs.dlq").increment();
}
```

### JobCallbackFailedEvent

Fired when a lifecycle callback (`onSuccess` / `onFailure`) throws an exception. The callback failure does not affect the job's recorded outcome.

```java
public class JobCallbackFailedEvent extends AbstractJobSchedulerEvent {
    public CallbackType getCallbackType()
    public String getErrorMessage()
    public String getCauseClassName()
    public int getCallbackAttempt()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getCallbackType()` | `CallbackType` | Which callback failed: `ON_SUCCESS` or `ON_FAILURE` |
| `getErrorMessage()` | `String` | Message from the thrown callback exception (may be null) |
| `getCauseClassName()` | `String` | Class name of the thrown callback exception |
| `getCallbackAttempt()` | `int` | 1-based callback invocation attempt |

```java
public void onCallbackFailed(@Observes JobCallbackFailedEvent event) {
    log.warn("Job {} {} callback failed: {} ({})",
        event.getJobId(), event.getCallbackType(),
        event.getErrorMessage(), event.getCauseClassName());
}
```

## Batch events

### BatchCompletingEvent

Fired when a batch is finishing (the last child job is completing).

```java
public class BatchCompletingEvent extends AbstractJobSchedulerEvent
```

```java
public int getTotalItems()
public int getCompletedItems()
public int getFailedItems()
```

| Method | Return Type | Description |
|---|---|---|
| `getTotalItems()` | `int` | Total child jobs in the batch |
| `getCompletedItems()` | `int` | Successfully completed child jobs so far |
| `getFailedItems()` | `int` | Failed child jobs so far |

### BatchCompletedEvent

Fired when a batch is fully complete (all child jobs have finished).

```java
public class BatchCompletedEvent extends AbstractJobSchedulerEvent {
    public int getTotalItems()
    public int getCompletedItems()
    public int getFailedItems()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getTotalItems()` | `int` | Total child jobs in the batch |
| `getCompletedItems()` | `int` | Successfully completed child jobs |
| `getFailedItems()` | `int` | Failed child jobs |

```java
public void onBatchCompleted(@Observes BatchCompletedEvent event) {
    double successRate = event.getTotalItems() > 0
        ? (double) event.getCompletedItems() / event.getTotalItems()
        : 1.0;

    log.info("Batch {} complete: {}/{} succeeded, {} failed",
        event.getJobId(), event.getCompletedItems(),
        event.getTotalItems(), event.getFailedItems());

    if (event.getFailedItems() > 0) {
        alertService.batchPartialFailure(event.getJobId(), event.getFailedItems());
    }
}
```

## Chain event types {#chainevent-types}

### ChainStartedEvent

Fired when a workflow chain begins execution.

```java
public class ChainStartedEvent extends AbstractJobSchedulerEvent {
    public UUID getParentJobId()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getParentJobId()` | `UUID` | UUIDv7 ID of the parent job that owns this chain |

### ChainCompletedEvent

Fired when a workflow chain succeeds.

```java
public class ChainCompletedEvent extends AbstractJobSchedulerEvent {
    public UUID getParentJobId()
}
```

### ChainFailedEvent

Fired when a workflow chain fails.

```java
public class ChainFailedEvent extends AbstractJobSchedulerEvent {
    public UUID getParentJobId()
    public String getErrorMessage()
}
```

```java
public void onChainFailed(@Observes ChainFailedEvent event) {
    log.error("Chain for parent job {} failed: {}",
        event.getParentJobId(), event.getErrorMessage());
}
```

## Workflow events

### WorkflowBranchTriggeredEvent

Fired when a workflow condition matches and a branch is triggered.

```java
public class WorkflowBranchTriggeredEvent extends AbstractJobSchedulerEvent
```

```java
public String getBranchCondition()
public UUID getNextJobId()
```

| Method | Return Type | Description |
|---|---|---|
| `getBranchCondition()` | `String` | Description of the branch condition that matched |
| `getNextJobId()` | `UUID` | Child job ID scheduled for the branch |

## Signal events

These events accompany the [signal-waiting job](./job-scheduler-service) lifecycle (`WAITING` status and `deliverSignal()`).

### JobSignalWaitingEvent

Fired when a job has been created in `WAITING` state, blocked on a named signal.

```java
public class JobSignalWaitingEvent extends AbstractJobSchedulerEvent {
    public String getSignalKey()
    public Duration getSignalTimeout()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getSignalKey()` | `String` | Signal key the job is waiting on |
| `getSignalTimeout()` | `Duration` | Maximum wait duration, or null for no timeout |

### JobSignaledEvent

Fired after a signal is successfully delivered to a `WAITING` job, transitioning it to `PENDING`. Published only on a successful delivery; a delivery that finds the job already terminal or non-`WAITING` produces no event.

```java
public class JobSignaledEvent extends AbstractJobSchedulerEvent {
    public String getSignalKey()
    public String getSignalDeliveredBy()
    public SignalDecision.Outcome getOutcome()
    public String getRejectionReason()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getSignalKey()` | `String` | Signal key delivered to the waiting job |
| `getSignalDeliveredBy()` | `String` | Principal or component that delivered the signal |
| `getOutcome()` | `SignalDecision.Outcome` | Approval/rejection outcome |
| `getRejectionReason()` | `String` | Rejection reason, or null when approved |

### JobsBulkSignaledEvent

Fired exactly once per successful key-based signal delivery when at least one `WAITING` job is unblocked. Like [`JobsBulkCancelledEvent`](#jobsbulkcancelledevent), a key-based delivery can unblock many jobs, so this event does **not** extend `AbstractJobSchedulerEvent`.

```java
public class JobsBulkSignaledEvent implements Serializable {
    public String getSignalKey()
    public int getCount()
    public String getSignalDeliveredBy()
    public SignalDecision.Outcome getOutcome()
    public String getRejectionReason()
    public Instant getSignaledAt()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getSignalKey()` | `String` | Signal key that was delivered |
| `getCount()` | `int` | Number of WAITING jobs unblocked |
| `getSignalDeliveredBy()` | `String` | Principal or component that delivered the signal |
| `getOutcome()` | `SignalDecision.Outcome` | Approval/rejection outcome |
| `getRejectionReason()` | `String` | Rejection reason, or null when approved |
| `getSignaledAt()` | `Instant` | When the signal was delivered |

### JobSignalTimedOutEvent

Fired when a `WAITING` job's signal timeout elapses and it is transitioned to `FAILED`.

```java
public class JobSignalTimedOutEvent extends AbstractJobSchedulerEvent {
    public String getSignalKey()
    public Duration getSignalTimeout()
}
```

| Method | Return Type | Description |
|---|---|---|
| `getSignalKey()` | `String` | Signal key the job was waiting on |
| `getSignalTimeout()` | `Duration` | Configured maximum wait duration that elapsed (never null) |

## System metrics

Ratchet does not publish aggregate system-metrics through the event listener API. For per-job metrics (start, completion, failure, retry timings), implement the [`MetricsCollector`](./spi-interfaces#metricscollector) SPI and wire it to your monitoring backend (Micrometer, StatsD, Prometheus, etc.).

## Example: comprehensive monitoring

```java
@ApplicationScoped
public class SchedulerMonitoring {

    @Inject Logger log;
    @Inject MeterRegistry metrics;
    @Inject AlertingService alerts;

    public void onStarted(@Observes JobStartedEvent e) {
        metrics.counter("jobs.started",
            "type", e.getJobType().name(),
            "priority", e.getPriority().name()).increment();
    }

    public void onCompleted(@Observes JobCompletedEvent e) {
        metrics.counter("jobs.completed", "type", e.getJobType().name()).increment();
        if (e.getExecutionTimeMs() != null) {
            metrics.timer("jobs.duration", "type", e.getJobType().name())
                .record(e.getExecutionTimeMs(), TimeUnit.MILLISECONDS);
        }
    }

    public void onFailed(@Observes JobFailedEvent e) {
        metrics.counter("jobs.failed", "type", e.getJobType().name()).increment();
    }

    public void onDlq(@Observes JobDlqEvent e) {
        metrics.counter("jobs.dlq", "type", e.getJobType().name()).increment();
        alerts.notify("Job " + e.getJobId() + " moved to DLQ: " + e.getErrorMessage());
    }

    public void onBatchCompleted(@Observes BatchCompletedEvent e) {
        metrics.gauge("batch.success_rate",
            (double) e.getCompletedItems() / Math.max(e.getTotalItems(), 1));
    }

    public void onBulkCancelled(@Observes JobsBulkCancelledEvent e) {
        metrics.counter("jobs.cancelled.bulk", "tag", e.getTag())
            .increment(e.getCount());
    }
}
```

## See also

- [JobSchedulerService Event Methods](./job-scheduler-service#event-listener-management)
- [Job Lifecycle](/concepts/job-lifecycle)
- [SPI Interfaces](./spi-interfaces)

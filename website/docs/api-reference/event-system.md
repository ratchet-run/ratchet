---
sidebar_position: 12
title: Event System Reference
description: Complete reference for job lifecycle events, batch events, chain events, and event observation patterns.
---

# Event System Reference

Observing and reacting to job lifecycle events. Ratchet publishes events at every significant state transition, enabling monitoring, alerting, and custom integrations.

## Listening to Events

### CDI Observers

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

### Programmatic Listeners

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

## Event Base Class

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

## Job Lifecycle Events

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

### JobCancellingEvent

Fired when a job cancellation is initiated (before the state transition completes).

```java
public class JobCancellingEvent extends AbstractJobCancellationEvent
```

```java
public String getPreviousStatus()
public Long getExecutionTimeMs()
```

| Method | Return Type | Description |
|---|---|---|
| `getPreviousStatus()` | `String` | Status before cancellation |
| `getExecutionTimeMs()` | `Long` | Execution duration in milliseconds when known |

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

## Batch Events

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

## Chain Event Types {#chainevent-types}

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

## Workflow and Observability Events

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

### PerformanceMetricsEvent

System-level performance metrics snapshot. Unlike other events, this does **not** extend `AbstractJobSchedulerEvent` because it represents aggregate system metrics, not a per-job event.

```java
public record PerformanceMetricsEvent(
    Map<String, Object> performanceData
) implements Serializable
```

The `performanceData` map is defensively copied and immutable after construction. Keys and values must be non-null and serializable by your event transport.

The `performanceData` map typically contains:

| Key | Type | Description |
|---|---|---|
| `queueDepth` | Number | Pending jobs in the queue |
| `readyJobs` | Number | Jobs ready for immediate execution |
| `oldestJobAge` | Number | Age of oldest pending job (seconds) |
| `processingRate` | Number | Jobs processed per minute |
| `successRate` | Number | Percentage of successful completions |
| `avgDuration` | Number | Average execution duration (ms) |
| `threadUtilization` | Number | Thread pool utilization percentage |
| `activeThreads` | Number | Currently active worker threads |
| `queuedTasks` | Number | Tasks waiting in thread pool queue |
| `cpuUsage` | Number | CPU utilization |
| `memoryUsage` | Number | Memory usage (bytes) |
| `memoryPercent` | Number | Memory as percentage of available |

```java
public void onMetrics(@Observes PerformanceMetricsEvent event) {
    Map<String, Object> data = event.performanceData();
    dashboard.update("queue_depth", data.get("queueDepth"));
    dashboard.update("processing_rate", data.get("processingRate"));
    dashboard.update("success_rate", data.get("successRate"));
}
```

## Example: Comprehensive Monitoring

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

    public void onMetrics(@Observes PerformanceMetricsEvent e) {
        e.performanceData().forEach((key, value) -> {
            if (value instanceof Number num) {
                metrics.gauge("scheduler." + key, num.doubleValue());
            }
        });
    }
}
```

## See Also

- [JobSchedulerService Event Methods](./job-scheduler-service#event-listener-management)
- [Job Lifecycle](/docs/concepts/job-lifecycle)
- [SPI Interfaces](./spi-interfaces)

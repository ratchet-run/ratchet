---
sidebar_position: 13
title: Monitoring
description: Micrometer metrics, event-based monitoring, health checks, and alerting patterns for Ratchet deployments
---

# Monitoring

Ratchet provides two monitoring channels: **Micrometer metrics** for quantitative dashboards and alerts, and the **event system** for real-time programmatic observation.

## Micrometer integration

### Setup

Add the Micrometer module:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-micrometer</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

`ratchet-micrometer` ships a default `SimpleMeterRegistry`, so a registry is always present. To send metrics to a real backend such as Prometheus, override the default with an `@Alternative` producer:

```java
@Produces
@Alternative
@Priority(2000)
@Singleton // @Singleton avoids a Weld proxy on the abstract MeterRegistry (WELD-001435)
public MeterRegistry meterRegistry() {
    return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
}
```

The `MicrometerMetricsCollector` is annotated `@Alternative @Priority(1000)`, so it automatically overrides the default `NoOpMetricsCollector` when the module is on the classpath.

### Published metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `ratchet.jobs.started` | Counter | `type`, `priority` | Jobs that began execution |
| `ratchet.jobs.completed` | Counter | `type` | Jobs that finished successfully |
| `ratchet.jobs.failed` | Counter | `type`, `family` | Jobs that failed (exception-family classification as tag) |
| `ratchet.jobs.duration` | Timer | `type` | Execution time per job |

The `type` tag corresponds to `JobType` (`SINGLE`, `RECURRING`, `BATCH`, `CHAIN`, `WORKFLOW`, `SYSTEM`). The `priority` tag corresponds to `JobPriority` (`LOWEST`, `LOW`, `NORMAL`, `HIGH`, `CRITICAL`). The `family` tag corresponds to `ExceptionFamily` — a coarse classification of the failure cause rather than the raw exception class name.

### Grafana dashboard queries

**Job throughput (Prometheus):**
```promql
rate(ratchet_jobs_completed_total[5m])
```

**Failure rate:**
```promql
rate(ratchet_jobs_failed_total[5m])
  / (rate(ratchet_jobs_completed_total[5m]) + rate(ratchet_jobs_failed_total[5m]))
```

**P95 execution time:**
```promql
histogram_quantile(0.95, rate(ratchet_jobs_duration_seconds_bucket[5m]))
```

**Failures by exception family:**
```promql
topk(5, sum by (family) (rate(ratchet_jobs_failed_total[5m])))
```

### Custom `MetricsCollector`

If you need different metric names, additional tags, or a non-Micrometer backend, implement the `MetricsCollector` SPI. The SPI declares several callbacks beyond the three core job-lifecycle hooks (`jobStarted`, `jobCompleted`, `jobFailed`) — including `successFinalizationRetried`/`successFinalizationMinimal`/`successFinalizationStuck`, `claimTransientFailure`, `jobsClaimed`, `gateRejected`, and `localWakeup`, plus default no-op hooks for cluster wakeup, callback failures, signal events, store operations, and poller circuit-breaker state. The simplest approach is to extend `NoOpMetricsCollector` and override only the callbacks you emit:

```java
public class MyMetricsCollector extends NoOpMetricsCollector {

    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
        // record your metric
    }

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
        // record your metric
    }
}
```

Register your implementation as a CDI alternative with a higher priority than 1000 to override the Micrometer collector.

## Event-based monitoring

The event system provides fine-grained lifecycle notifications. Unlike metrics (which are aggregated counters/timers), events carry full context about individual job executions.

### CDI observers

```java
@ApplicationScoped
public class JobMonitor {

    public void onStarted(@Observes JobStartedEvent event) {
        log.info("Job {} started on node {}", event.getJobId(), event.getNodeId());
    }

    public void onCompleted(@Observes JobCompletedEvent event) {
        log.info("Job {} completed in {}ms",
            event.getJobId(), event.getExecutionTimeMs());
    }

    public void onFailed(@Observes JobFailedEvent event) {
        log.error("Job {} failed (attempt {}): {}",
            event.getJobId(), event.getRetryAttempt(), event.getErrorMessage());
    }

    public void onRetrying(@Observes JobRetryingEvent event) {
        log.warn("Job {} retrying (attempt {}): {}",
            event.getJobId(), event.getRetryAttempt(), event.getErrorMessage());
    }

    public void onDlq(@Observes JobDlqEvent event) {
        // Alert: job exhausted all retries
        alertOps("Job " + event.getJobId() + " moved to DLQ: " + event.getErrorMessage());
    }
}
```

### Programmatic listeners

For dynamic registration (useful in frameworks or libraries that can't use CDI observers):

```java
scheduler.addEventListener(event -> {
    if (event instanceof JobFailedEvent failed) {
        log.error("Job {} failed (attempt {}): {}",
            failed.getJobId(), failed.getRetryAttempt(), failed.getErrorMessage());
    }
});
```

For aggregate poll-cycle and throughput metrics, use the `MetricsCollector` SPI (or the Micrometer module) rather than the event system — events carry per-job context, not cycle-level summaries.

### Event types

| Event | When fired |
|-------|-----------|
| `JobStartedEvent` | Worker begins executing a job |
| `JobCompletedEvent` | Job finished successfully |
| `JobFailedEvent` | Job threw an exception |
| `JobRetryingEvent` | Job failed but will be retried |
| `JobDlqEvent` | Job exhausted retries, moved to dead-letter queue |
| `JobCancelledEvent` | Job was cancelled via API |
| `BatchCompletingEvent` | All children in a batch have finished |
| `ChainStartedEvent` | A chained job was triggered by its parent |
| `WorkflowBranchTriggeredEvent` | A conditional branch was activated |

## Health checks

### Node heartbeat check

Query the `scheduler_node` table to verify all expected nodes are alive (MySQL syntax):

```sql
SELECT node_id, heartbeat_ts,
       TIMESTAMPDIFF(SECOND, heartbeat_ts, NOW()) AS seconds_stale
FROM scheduler_node
WHERE heartbeat_ts < NOW() - INTERVAL 2 MINUTE;
```

Any rows returned indicate stale nodes. For programmatic health checks:

```java
@ApplicationScoped
public class RatchetHealthCheck {

    @Inject
    NodeStore nodeStore;

    public boolean isHealthy() {
        Instant cutoff = Instant.now().minus(Duration.ofMinutes(2));
        return nodeStore.findInactiveNodesSince(cutoff).isEmpty();
    }
}
```

### Queue depth check

Monitor the number of pending jobs to detect backlog:

```sql
-- PENDING is live state on scheduler_job_queue (priority is denormalized there).
SELECT priority, COUNT(*) as pending_count
FROM scheduler_job_queue
WHERE status = 'PENDING'
GROUP BY priority;
```

Alert if `CRITICAL` or `HIGH` jobs have been pending for more than a few seconds.

### DLQ check

Jobs in the dead-letter queue need human attention:

```sql
SELECT COUNT(*) FROM scheduler_dlq_alerts;
```

Wire this into your alerting system — a non-zero DLQ count means something failed permanently.

## Alerting recommendations

| Condition | Severity | Action |
|-----------|----------|--------|
| DLQ count > 0 | **Critical** | Investigate and retry or archive |
| Failure rate > 5% (5min window) | **Warning** | Check logs for systematic errors |
| P95 duration > 2x normal | **Warning** | Check for resource contention or slow dependencies |
| Node heartbeat stale > 2min | **Critical** | Node may be down — check process health |
| Pending CRITICAL jobs > 30s | **Critical** | Cluster may be overloaded |
| Pending queue depth growing | **Warning** | Scale up nodes or increase thread pool |

## See also

- [Event System](../api-reference/event-system.md) — Complete event type reference
- [Metrics Collection](../advanced/metrics-collection.md) — Advanced custom metrics patterns
- [Troubleshooting](./troubleshooting.md) — Diagnosing common deployment issues

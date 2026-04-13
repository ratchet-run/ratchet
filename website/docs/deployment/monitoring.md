---
sidebar_position: 13
title: Monitoring
description: Micrometer metrics, event-based monitoring, health checks, and alerting patterns for Ratchet deployments
---

# Monitoring

Ratchet provides two monitoring channels: **Micrometer metrics** for quantitative dashboards and alerts, and the **event system** for real-time programmatic observation.

## Micrometer Integration

### Setup

Add the Micrometer module:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-micrometer</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Ensure a `MeterRegistry` is available as a CDI bean. If you're running on a framework like Quarkus or Spring Boot (via CDI bridge), this is typically provided automatically. Otherwise, produce one:

```java
@Produces
@ApplicationScoped
public MeterRegistry meterRegistry() {
    return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
}
```

The `MicrometerMetricsCollector` is annotated `@Alternative @Priority(1000)`, so it automatically overrides the default `NoOpMetricsCollector` when the module is on the classpath.

### Published Metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `ratchet.jobs.started` | Counter | `type`, `priority` | Jobs that began execution |
| `ratchet.jobs.completed` | Counter | `type` | Jobs that finished successfully |
| `ratchet.jobs.failed` | Counter | `type`, `exception` | Jobs that failed (exception class name as tag) |
| `ratchet.jobs.duration` | Timer | `type` | Execution time in milliseconds |

The `type` tag corresponds to `JobType` (e.g., `SINGLE`, `RECURRING`, `BATCH_CHILD`). The `priority` tag corresponds to `JobPriority` (e.g., `NORMAL`, `HIGH`, `CRITICAL`).

### Grafana Dashboard Queries

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

**Failures by exception type:**
```promql
topk(5, sum by (exception) (rate(ratchet_jobs_failed_total[5m])))
```

### Custom MetricsCollector

If you need different metric names, additional tags, or a non-Micrometer backend, implement the `MetricsCollector` SPI directly:

```java
public interface MetricsCollector {
    void jobStarted(long jobId, JobType type, JobPriority priority);
    void jobCompleted(long jobId, JobType type, long executionTimeMs);
    void jobFailed(long jobId, JobType type, Throwable cause, int attempt);
}
```

Register your implementation as a CDI alternative with a higher priority than 1000 to override the Micrometer collector.

## Event-Based Monitoring

The event system provides fine-grained lifecycle notifications. Unlike metrics (which are aggregated counters/timers), events carry full context about individual job executions.

### CDI Observers

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

### Programmatic Listeners

For dynamic registration (useful in frameworks or libraries that can't use CDI observers):

```java
scheduler.addEventListener(event -> {
    if (event instanceof PerformanceMetricsEvent perf) {
        log.info("Poll cycle: claimed={}, executed={}, duration={}ms",
            perf.getClaimedCount(), perf.getExecutedCount(), perf.getDurationMs());
    }
});
```

### Event Types

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
| `PerformanceMetricsEvent` | End-of-cycle summary from the poller |

## Health Checks

### Node Heartbeat Check

Query the `scheduler_node` table to verify all expected nodes are alive:

```sql
SELECT node_id, last_heartbeat,
       TIMESTAMPDIFF(SECOND, last_heartbeat, NOW()) AS seconds_stale
FROM scheduler_node
WHERE last_heartbeat < NOW() - INTERVAL 2 MINUTE;
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

### Queue Depth Check

Monitor the number of pending jobs to detect backlog:

```sql
SELECT priority, COUNT(*) as pending_count
FROM scheduler_job
WHERE status = 'PENDING'
GROUP BY priority;
```

Alert if `CRITICAL` or `HIGH` jobs have been pending for more than a few seconds.

### DLQ Check

Jobs in the dead-letter queue need human attention:

```sql
SELECT COUNT(*) FROM scheduler_job WHERE status = 'DEAD_LETTER';
```

Wire this into your alerting system — a non-zero DLQ count means something failed permanently.

## Alerting Recommendations

| Condition | Severity | Action |
|-----------|----------|--------|
| DLQ count > 0 | **Critical** | Investigate and retry or archive |
| Failure rate > 5% (5min window) | **Warning** | Check logs for systematic errors |
| P95 duration > 2x normal | **Warning** | Check for resource contention or slow dependencies |
| Node heartbeat stale > 2min | **Critical** | Node may be down — check process health |
| Pending CRITICAL jobs > 30s | **Critical** | Cluster may be overloaded |
| Pending queue depth growing | **Warning** | Scale up nodes or increase thread pool |

## See Also

- [Event System](../api-reference/event-system.md) — Complete event type reference
- [Metrics Collection](../advanced/metrics-collection.md) — Advanced custom metrics patterns
- [Troubleshooting](./troubleshooting.md) — Diagnosing common deployment issues

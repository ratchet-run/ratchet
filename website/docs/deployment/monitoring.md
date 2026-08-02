---
sidebar_position: 13
title: Monitoring
description: Micrometer metrics, event-based monitoring, health checks, and alerting patterns for Ratchet deployments
---

# Monitoring

Ratchet provides two monitoring channels: Micrometer metrics for quantitative dashboards and alerts, and the event system for real-time programmatic observation.

## Micrometer integration

### Setup

Add the Micrometer module:

```xml
<dependency>
  <groupId>run.ratchet</groupId>
  <artifactId>ratchet-micrometer</artifactId>
  <version>0.3.1</version>
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

You can install `ratchet-otel` beside `ratchet-micrometer`. In that combination, OpenTelemetry's
priority-1100 adapter handles tracing while Micrometer continues to publish metrics. If both
modules are present, Ratchet deliberately chooses direct OpenTelemetry tracing instead of the
Micrometer tracing bridge. A custom tracing collector with a priority above 1100 overrides both.

### Published metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `ratchet.jobs.started` | Counter | `type`, `priority` | Job execution attempts started |
| `ratchet.jobs.completed` | Counter | `type` | Job execution attempts completed successfully |
| `ratchet.jobs.failed` | Counter | `type`, `family` | Failed attempts, grouped by bounded exception family |
| `ratchet.jobs.duration` | Timer | `type` | Wall-clock duration of successful attempts |
| `ratchet.store.finalization.retries` | Counter | `type` | Transient conflicts while persisting a successful result |
| `ratchet.store.finalization.minimal_success` | Counter | `type` | Full-result persistence was exhausted and Ratchet used the minimal success write |
| `ratchet.store.finalization.stuck` | Counter | `type` | Both finalization paths were exhausted; the job remains `RUNNING` for recovery |
| `ratchet.store.claim.transient_failures` | Counter | `execution_type` | Transient store conflicts on the claim path |
| `ratchet.poller.claimed.jobs` | Counter | `execution_type` | Number of jobs claimed |
| `ratchet.submission.gate.rejections` | Counter | `execution_type`, `gate_status` | Claimed jobs blocked by a local submission gate |
| `ratchet.wakeup.local` | Counter | `source` | Direct local poller wakeups after new work |
| `ratchet.execution.target.fallback` | Counter | `requested_target`, `effective_target` | Requested executor target was unavailable and Ratchet used a fallback pool |
| `ratchet.wakeup.cluster.publish` | Counter | `transport`, `outcome` | Cluster wakeup publish attempts |
| `ratchet.wakeup.cluster.receive` | Counter | `transport`, `outcome` | Cluster wakeup messages observed by a receiver |
| `ratchet.callbacks.failed` | Counter | `type`, `family` | `onSuccess` or `onFailure` callback failures; these do not fail the parent job |
| `ratchet.signal.waiting` | Counter | `type`, `signal_key` | Jobs created in `WAITING` for a signal |
| `ratchet.signal.delivered` | Counter | `type`, `signal_key`, `outcome` | Signal deliveries that moved a job to `PENDING` |
| `ratchet.signal.timed_out` | Counter | `type`, `signal_key` | Signal waits that expired |
| `ratchet.signal.cancelled` | Counter | `type`, `signal_key` | Signal waits cancelled before delivery |
| `ratchet.store.operation` | Timer | `store`, `operation`, `outcome` | Timed store operations on claim and execution hot paths |
| `ratchet.poller.breaker.state` | Gauge | `breaker` | Poller claim-breaker state: `0` closed/unknown, `1` half-open, `2` open |
| `ratchet.circuit.breaker.state` | Gauge | `service`, `profile` | Application circuit-breaker state: `0` closed/unknown, `1` half-open, `2` open |
| `ratchet.encryption.integrity.violations` | Counter | `surface` | A row marked as encrypted contained plaintext. The read succeeds, but the writer or rollout needs investigation. |
| `ratchet.encryption.envelope.version_skew` | Counter | `version_gap` | A newer envelope reached this node. `next`, `multiple_versions_ahead`, and `not_newer` keep the diagnostic bounded; Ratchet releases valid newer jobs for an upgraded peer. |

The `type` tag corresponds to `JobType` (`SINGLE`, `RECURRING`, `BATCH`, `CHAIN`, `WORKFLOW`, `SYSTEM`). The `priority` tag corresponds to `JobPriority` (`LOWEST`, `LOW`, `NORMAL`, `HIGH`, `CRITICAL`). The `family` tag corresponds to `ExceptionFamily`, a coarse classification of the failure cause rather than the raw exception class name.

`MicrometerMetricTagPolicy` keeps string-valued tags bounded. Framework values pass through, blanks become `UNKNOWN`, and unrecognized values become `OTHER`. Signal keys are application strings and collapse to `OTHER` unless you explicitly allowlist selected keys. See [Metrics Collection](../advanced/metrics-collection.md#published-metrics) for the tag-policy details.

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

If you need different metric names, additional tags, or a non-Micrometer backend, implement the `MetricsCollector` SPI. Its callbacks cover job outcomes, success finalization, claims and gates, wakeups and executor routing, callback and signal events, store timing, poller and application circuit breakers, and encryption integrity/version signals.

The example below is intentionally partial: it records two basic job outcomes and drops every other signal. Extending `NoOpMetricsCollector` is convenient when that is what you want. For a production replacement, review every callback in [Metrics Collection](../advanced/metrics-collection.md#metricscollector-spi) and either export it or delegate it to a complete collector.

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

    public void onExecutionTimeout(@Observes JobExecutionTimedOutEvent event) {
        // Dispatch external alerts on an application-managed executor: observers are synchronous.
        timeoutAlertExecutor.execute(() -> alertOps(
            "Job " + event.getJobId() + " exceeded " + event.getExecutionTimeout()));
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

For aggregate poll-cycle and throughput metrics, use the `MetricsCollector` SPI (or the Micrometer module) rather than the event system. Events carry per-job context, not cycle-level summaries.

### Event types

| Event | When fired |
|-------|-----------|
| `JobStartedEvent` | Worker begins executing a job |
| `JobCompletedEvent` | Job finished successfully |
| `JobFailedEvent` | Job reached terminal `FAILED` state |
| `JobRetryingEvent` | Job failed but will be retried |
| `JobDlqEvent` | Job entered terminal dead-letter/FAILED handling |
| `JobExecutionTimedOutEvent` | Running job exceeded its configured execution timeout |
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
SELECT COUNT(*)
FROM scheduler_job
WHERE terminal_status = 'FAILED';
```

Wire this into your alerting system. The durable `scheduler_job` row remains the source of truth
after its live `scheduler_job_queue` row is removed.

## Alerting recommendations

| Condition | Severity | Action |
|-----------|----------|--------|
| DLQ count > 0 | **Critical** | Investigate and retry or archive |
| Failure rate > 5% (5min window) | **Warning** | Check logs for systematic errors |
| P95 duration > 2x normal | **Warning** | Check for resource contention or slow dependencies |
| Node heartbeat stale > 2min | **Critical** | Node may be down; check process health |
| Pending CRITICAL jobs > 30s | **Critical** | Cluster may be overloaded |
| Pending queue depth growing | **Warning** | Scale up nodes or increase thread pool |
| `ratchet.store.finalization.stuck` > 0 | **Critical** | Check store health; successful work is waiting for recovery |
| `ratchet.poller.breaker.state` remains `2` | **Critical** | Investigate claim-path store failures |
| `ratchet.encryption.integrity.violations` > 0 | **Critical** | Investigate a downgrade, lagging writer, or encryption bug |
| `ratchet.encryption.envelope.version_skew` persists after rollout | **Warning** | Find and upgrade the lagging node |

## See also

- [Event System](../api-reference/event-system.md) -- Complete event type reference
- [Metrics Collection](../advanced/metrics-collection.md) -- Advanced custom metrics patterns
- [Troubleshooting](./troubleshooting.md) -- Diagnosing common deployment issues

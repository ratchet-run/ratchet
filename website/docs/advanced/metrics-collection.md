---
sidebar_position: 4
title: Metrics Collection
description: Collecting job execution metrics with the MetricsCollector SPI and Micrometer integration
---

# Metrics Collection

Ratchet provides a `MetricsCollector` SPI that receives callbacks during the job execution lifecycle. The reference implementation ships with a no-op default and a Micrometer adapter module (`ratchet-micrometer`), and you can implement the SPI directly for custom integrations.

## MetricsCollector SPI

`MetricsCollector` covers the job lifecycle and the scheduler paths operators need when work is not progressing normally. It currently declares 22 callbacks:

| Area | Callbacks |
|------|-----------|
| Job execution | `jobStarted`, `jobCompleted`, `jobFailed` |
| Success finalization | `successFinalizationRetried`, `successFinalizationMinimal`, `successFinalizationStuck` |
| Claim and admission | `claimTransientFailure`, `jobsClaimed`, `gateRejected` |
| Wakeups and routing | `localWakeup`, `executionTargetFallback`, `clusterWakeupPublished`, `clusterWakeupReceived` |
| Callbacks and signals | `callbackFailed`, `signalWaiting`, `signalDelivered`, `signalTimedOut`, `signalCancelled` |
| Store health | `storeOperation`, `pollerBreakerState` |
| Payload encryption | `encryptionIntegrityViolation`, `encryptionEnvelopeVersionSkew` |

The first ten methods are required for a direct implementation. The remaining methods have default no-op bodies so the incubating SPI can grow without breaking existing implementations. Treat those defaults as a compatibility mechanism, not a claim that the signals are unimportant. A complete monitoring adapter should make an explicit decision about every callback.

## Default No-Op Collector

When no monitoring integration is configured, `NoOpMetricsCollector` satisfies the injection point without publishing anything. It implements the required callbacks and inherits or retains no-op behavior for the optional ones, so Ratchet runs without a metrics dependency on the classpath.

## Micrometer Integration

The `ratchet-micrometer` module provides a Micrometer adapter that publishes job metrics to any Micrometer-supported backend (Prometheus, Datadog, CloudWatch, New Relic, etc.).

### Adding the Dependency

```xml
<dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-micrometer</artifactId>
    <version>0.1.1</version>
</dependency>
```

The module uses `@Alternative @Priority(1000)` on the `MicrometerMetricsCollector` bean, which automatically overrides the default `NoOpMetricsCollector` when present on the classpath. The module also provides a fallback `SimpleMeterRegistry`, so the adapter works without further configuration. Produce your own `MeterRegistry` when you want a real backend such as Prometheus or Datadog.

### Providing a MeterRegistry

The `MicrometerMetricsCollector` injects a `MeterRegistry` via CDI. Override the fallback registry in your application when you want a specific backend:

```java
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

@ApplicationScoped
public class MetricsProducer {

    @Produces
    @Alternative
    @Priority(2000)
    @Singleton // @Singleton avoids a Weld proxy on the abstract MeterRegistry (WELD-001435)
    public MeterRegistry meterRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }
}
```

### Published Metrics

The Micrometer adapter publishes the following 23 meters:

| Metric Name | Type | Tags | Description |
|-------------|------|------|-------------|
| `ratchet.jobs.started` | Counter | `type`, `priority` | Job execution attempts started |
| `ratchet.jobs.completed` | Counter | `type` | Job execution attempts completed successfully |
| `ratchet.jobs.failed` | Counter | `type`, `family` | Failed attempts, classified as `TRANSIENT`, `TIMEOUT`, `VALIDATION`, `BUSINESS`, or `UNKNOWN` |
| `ratchet.jobs.duration` | Timer | `type` | Wall-clock duration of successful attempts |
| `ratchet.store.finalization.retries` | Counter | `type` | Transient conflicts while persisting a successful result |
| `ratchet.store.finalization.minimal_success` | Counter | `type` | Full-result persistence was exhausted and Ratchet used the minimal terminal-success write |
| `ratchet.store.finalization.stuck` | Counter | `type` | Both full and minimal finalization were exhausted; the job remains `RUNNING` for recovery |
| `ratchet.store.claim.transient_failures` | Counter | `execution_type` | Transient store conflicts on the claim path |
| `ratchet.poller.claimed.jobs` | Counter | `execution_type` | Number of jobs claimed, incremented by the claimed batch size |
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
| `ratchet.encryption.integrity.violations` | Counter | `surface` | A row marked as encrypted contained unframed plaintext. The read succeeds, but this signals a downgrade, lagging writer, or bug. |
| `ratchet.encryption.envelope.version_skew` | Counter | `version_gap` | A job used a newer envelope than this node can read. `next` is one version ahead, `multiple_versions_ahead` is more than one, and `not_newer` flags an unexpected callback. Ratchet releases valid newer jobs for an upgraded peer; a persistent rate identifies a lagging node. |

#### Tag Values

The `type` tag corresponds to `JobType` enum values:

- `SINGLE` -- One-shot background task executed once and not rescheduled
- `RECURRING` -- Cron-scheduled or interval-based jobs
- `BATCH` -- Batch-processing job comprising multiple child items
- `CHAIN` -- A sequenced chain of tasks executed in order
- `WORKFLOW` -- Workflow-driven execution with conditional branches or join semantics
- `SYSTEM` -- Scheduler-managed system work, not user-creatable

The `priority` tag corresponds to `JobPriority` enum values (`LOWEST`, `LOW`, `NORMAL`, `HIGH`, `CRITICAL`).

String-valued tags pass through `MicrometerMetricTagPolicy`. Framework-defined bounded values are retained; blank values become `UNKNOWN`, and unrecognized values become `OTHER`. Protected-surface values are derived from the `ProtectedSurface` enum, so adding a framework surface automatically updates the default policy. Application signal keys are unbounded and therefore collapse to `OTHER` by default. If you deliberately want selected signal keys or extension values as dimensions, provide a policy that allowlists them and account for the extra time series.

### Prometheus Scrape Endpoint Example

With the Prometheus registry, expose a scrape endpoint in your Jakarta REST application:

```java
import io.micrometer.prometheus.PrometheusMeterRegistry;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

@Path("/metrics")
public class MetricsEndpoint {

    @Inject
    private PrometheusMeterRegistry registry;

    @GET
    @Produces("text/plain")
    public String scrape() {
        return registry.scrape();
    }
}
```

### Grafana Dashboard Queries

Common PromQL queries for a Ratchet monitoring dashboard:

```promql
# Job throughput (started per second)
rate(ratchet_jobs_started_total[5m])

# Success rate
rate(ratchet_jobs_completed_total[5m])
  / rate(ratchet_jobs_started_total[5m])

# Failure rate by exception family
sum by (family) (rate(ratchet_jobs_failed_total[5m]))

# P95 execution time
histogram_quantile(0.95, rate(ratchet_jobs_duration_seconds_bucket[5m]))

# Jobs in flight (started minus completed+failed)
ratchet_jobs_started_total
  - ratchet_jobs_completed_total
  - ratchet_jobs_failed_total
```

## Implementing a Custom MetricsCollector

For monitoring systems without Micrometer support, or when you need custom metric shapes, implement the SPI directly. The abbreviated examples below deliberately export only the three basic job outcomes. They are partial collectors, not replacements for the full operational surface listed above. For production parity, handle every callback or delegate the callbacks you do not customize to another complete collector.

### MicroProfile Metrics Example

```java
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.ExceptionFamily;
import run.ratchet.spi.NoOpMetricsCollector;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Timer;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.time.Duration;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class MicroProfileMetricsCollector extends NoOpMetricsCollector {

    @Inject
    private MetricRegistry registry;

    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
        Counter counter = registry.counter("ratchet_jobs_started",
            new org.eclipse.microprofile.metrics.Tag("type", type.name()),
            new org.eclipse.microprofile.metrics.Tag("priority", priority.name()));
        counter.inc();
    }

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
        Counter counter = registry.counter("ratchet_jobs_completed",
            new org.eclipse.microprofile.metrics.Tag("type", type.name()));
        counter.inc();

        Timer timer = registry.timer("ratchet_jobs_duration",
            new org.eclipse.microprofile.metrics.Tag("type", type.name()));
        timer.update(Duration.ofMillis(executionTimeMs));
    }

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
        Counter counter = registry.counter("ratchet_jobs_failed",
            new org.eclipse.microprofile.metrics.Tag("type", type.name()),
            new org.eclipse.microprofile.metrics.Tag("family",
                ExceptionFamily.classify(cause).name()));
        counter.inc();
    }
}
```

### Logging-Based Metrics

For simpler deployments where structured logs feed into a log aggregation system:

```java
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.NoOpMetricsCollector;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import java.util.logging.Logger;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class LoggingMetricsCollector extends NoOpMetricsCollector {

    private static final Logger log = Logger.getLogger("ratchet.metrics");

    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
        log.info(String.format(
            "metric=job.started job_id=%s type=%s priority=%s",
            jobId, type, priority));
    }

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
        log.info(String.format(
            "metric=job.completed job_id=%s type=%s duration_ms=%d",
            jobId, type, executionTimeMs));
    }

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
        log.warning(String.format(
            "metric=job.failed job_id=%s type=%s exception=%s attempt=%d",
            jobId, type, cause.getClass().getSimpleName(), attempt));
    }
}
```

## Alerting Recommendations

Use the metrics to set up alerts for common operational issues:

| Condition | Suggested Alert |
|-----------|----------------|
| Failure rate > 10% over 5 minutes | Warning: elevated job failure rate |
| Failure rate > 50% over 5 minutes | Critical: job processing is degraded |
| P95 execution time > 2x baseline | Warning: job execution slowdown |
| No jobs started in 15 minutes (when expected) | Critical: scheduler may be stalled |
| `ratchet.jobs.failed` with a specific `family` tag spikes | Investigate the failing exception family |
| `ratchet.store.finalization.stuck` is non-zero | Page an operator; successful work is waiting for recovery |
| `ratchet.poller.breaker.state` remains `2` | Investigate store availability and claim latency |
| `ratchet.encryption.integrity.violations` is non-zero | Investigate an encryption downgrade or un-upgraded writer |
| `ratchet.encryption.envelope.version_skew` persists after a rollout | Find and upgrade the lagging node |

## Best practices

### Use tags, not metric names, for dimensionality

Prefer `ratchet.jobs.started{type=SINGLE}` over `ratchet.single_jobs.started`. Tags allow flexible aggregation and filtering in dashboards without multiplying metric names.

### Keep exception tags bounded

The Micrometer adapter classifies failures into the `ExceptionFamily` enum (`TRANSIENT`, `TIMEOUT`, `VALIDATION`, `BUSINESS`, `UNKNOWN`) for the `family` tag, so cardinality stays fixed. A custom collector that tags by exception class name can produce high-cardinality metrics if your application throws many dynamically-generated exception types. Normalize exception names in that case.

### Monitor finalization fallbacks

Alert on `ratchet.store.finalization.stuck`. Track `ratchet.store.finalization.retries` and `ratchet.store.finalization.minimal_success` as earlier warnings that terminal writes are contending or failing.

### Set baselines before alerting

Run Ratchet for a few days with metrics collection enabled to establish baseline throughput, failure rates, and execution times. Set alert thresholds relative to those baselines rather than using fixed absolute values.

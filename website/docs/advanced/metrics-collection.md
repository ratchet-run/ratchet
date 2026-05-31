---
sidebar_position: 4
title: Metrics Collection
description: Collecting job execution metrics with the MetricsCollector SPI and Micrometer integration
---

# Metrics Collection

Ratchet provides a `MetricsCollector` SPI that receives callbacks during the job execution lifecycle. The reference implementation ships with a no-op default, a ready-to-use Micrometer adapter module (`ratchet-micrometer`), and a straightforward path for building custom integrations.

## MetricsCollector SPI

The SPI defines three core lifecycle callbacks plus an optional callback-failure hook:

```java
package run.ratchet.spi;

@Incubating
public interface MetricsCollector {

    /**
     * Notifies that a job has started execution.
     *
     * @param jobId    the unique job identifier
     * @param type     the job type (SINGLE, RECURRING, BATCH, etc.)
     * @param priority the job priority level
     */
    void jobStarted(UUID jobId, JobType type, JobPriority priority);

    /**
     * Notifies that a job has completed successfully.
     *
     * @param jobId           the unique job identifier
     * @param type            the job type
     * @param executionTimeMs the execution time of the completed attempt in milliseconds
     */
    void jobCompleted(UUID jobId, JobType type, long executionTimeMs);

    /**
     * Notifies that a job has failed.
     *
     * @param jobId   the unique job identifier
     * @param type    the job type
     * @param cause   the exception that caused the failure
     * @param attempt the 1-based attempt number, including the failed attempt
     */
    void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt);

    /**
     * Notifies that an onSuccess/onFailure callback threw an exception.
     */
    default void callbackFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
        // No-op
    }
}
```

This interface is marked `@Incubating` -- additional lifecycle callbacks (retry, timeout, DLQ) may be added in future releases.

## Default No-Op Collector

When no monitoring integration is configured, the `NoOpMetricsCollector` satisfies the injection point with empty method bodies:

```java
@ApplicationScoped
public class NoOpMetricsCollector implements MetricsCollector {

    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
        // No-op
    }

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
        // No-op
    }

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
        // No-op
    }
}
```

This ensures Ratchet works out of the box without requiring a metrics dependency.

## Micrometer Integration

The `ratchet-micrometer` module provides a drop-in Micrometer adapter that publishes job metrics to any Micrometer-supported backend (Prometheus, Datadog, CloudWatch, New Relic, etc.).

### Adding the Dependency

```xml
<dependency>
    <groupId>run.ratchet</groupId>
    <artifactId>ratchet-micrometer</artifactId>
    <version>${ratchet.version}</version>
</dependency>
```

The module uses `@Alternative @Priority(1000)` on the `MicrometerMetricsCollector` bean, which automatically overrides the default `NoOpMetricsCollector` when present on the classpath. The module also provides a fallback `SimpleMeterRegistry`, so it works out of the box. Produce your own `MeterRegistry` when you want a real backend such as Prometheus or Datadog.

### Providing a MeterRegistry

The `MicrometerMetricsCollector` injects a `MeterRegistry` via CDI. Override the fallback registry in your application when you want a specific backend:

```java
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class MetricsProducer {

    @Produces
    @ApplicationScoped
    public MeterRegistry meterRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }
}
```

### Published Metrics

The Micrometer adapter publishes the following metrics:

#### Counters

| Metric Name | Tags | Description |
|-------------|------|-------------|
| `ratchet.jobs.started` | `type`, `priority` | Incremented each time a job begins execution |
| `ratchet.jobs.completed` | `type` | Incremented each time a job completes successfully |
| `ratchet.jobs.failed` | `type`, `family` | Incremented each time a job fails. The `family` tag contains the `ExceptionFamily` classification of the causing exception (`TRANSIENT`, `TIMEOUT`, `VALIDATION`, `BUSINESS`, or `UNKNOWN`). |

#### Timers

| Metric Name | Tags | Description |
|-------------|------|-------------|
| `ratchet.jobs.duration` | `type` | Records the execution time of completed jobs. Provides count, total time, max, and histogram data. |

#### Tag Values

The `type` tag corresponds to `JobType` enum values:

- `SINGLE` -- One-shot background task executed once and not rescheduled
- `RECURRING` -- Cron-scheduled or interval-based jobs
- `BATCH` -- Batch-processing job comprising multiple child items
- `CHAIN` -- A sequenced chain of tasks executed in order
- `WORKFLOW` -- Workflow-driven execution with conditional branches or join semantics
- `SYSTEM` -- Scheduler-managed system work, not user-creatable

The `priority` tag corresponds to `JobPriority` enum values (`LOWEST`, `LOW`, `NORMAL`, `HIGH`, `CRITICAL`).

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

For monitoring systems without Micrometer support, or when you need custom metric shapes, implement the SPI directly.

### MicroProfile Metrics Example

```java
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
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
public class MicroProfileMetricsCollector implements MetricsCollector {

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
            new org.eclipse.microprofile.metrics.Tag("exception",
                cause.getClass().getSimpleName()));
        counter.inc();
    }
}
```

### Logging-Based Metrics

For simpler deployments where structured logs feed into a log aggregation system:

```java
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.interceptor.Interceptor;
import java.util.logging.Logger;

@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class LoggingMetricsCollector implements MetricsCollector {

    private static final Logger log = Logger.getLogger("ratchet.metrics");

    @Override
    public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
        log.info(String.format(
            "metric=job.started job_id=%d type=%s priority=%s",
            jobId, type, priority));
    }

    @Override
    public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
        log.info(String.format(
            "metric=job.completed job_id=%d type=%s duration_ms=%d",
            jobId, type, executionTimeMs));
    }

    @Override
    public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
        log.warning(String.format(
            "metric=job.failed job_id=%d type=%s exception=%s attempt=%d",
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

## Best Practices

**Use tags for dimensionality, not metric names.** Prefer `ratchet.jobs.started{type=SINGLE}` over `ratchet.single_jobs.started`. Tags enable flexible aggregation and filtering in dashboards.

**Keep exception tags bounded.** The Micrometer adapter classifies failures into the bounded `ExceptionFamily` enum (`TRANSIENT`, `TIMEOUT`, `VALIDATION`, `BUSINESS`, `UNKNOWN`) for the `family` tag, so its cardinality stays fixed. If a custom collector instead tags by the exception's class name and your application throws many dynamically-generated exception types, this could create high-cardinality metrics. Consider normalizing exception names in that case.

**Monitor attempt counts.** A spike in `attempt > 1` failures indicates that retries are being consumed. Pair this with retry policy metrics to understand whether jobs are eventually succeeding or exhausting retries.

**Set baselines before alerting.** Run Ratchet for a few days with metrics collection enabled to establish baseline throughput, failure rates, and execution times. Set alert thresholds relative to these baselines rather than using absolute values.

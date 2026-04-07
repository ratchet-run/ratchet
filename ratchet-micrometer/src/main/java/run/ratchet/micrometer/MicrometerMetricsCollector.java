package run.ratchet.micrometer;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.MetricsCollector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.time.Duration;

/**
 * Micrometer-based {@link MetricsCollector} that publishes job execution metrics to any Micrometer
 * registry (Prometheus, Datadog, CloudWatch, etc.).
 *
 * <p>To use, add this module to your classpath. The CDI {@code @Alternative @Priority} ensures it
 * overrides the default {@code NoOpMetricsCollector} from ratchet.
 *
 * <p>Metrics published:
 *
 * <ul>
 *   <li>{@code ratchet.jobs.started} — counter, tagged by type and priority
 *   <li>{@code ratchet.jobs.completed} — counter, tagged by type
 *   <li>{@code ratchet.jobs.failed} — counter, tagged by type and exception class
 *   <li>{@code ratchet.jobs.duration} — timer, tagged by type
 *   <li>{@code ratchet.callbacks.failed} — counter, tagged by type and exception class
 * </ul>
 */
@Alternative
@Priority(1000)
@ApplicationScoped
public class MicrometerMetricsCollector implements MetricsCollector {

  private final MeterRegistry registry;

  // Required by CDI proxy. The CDI proxy never invokes business methods on this instance —
  // every real call goes to the @Inject constructor below. We still guard the field below
  // so a misconfigured deployment doesn't NPE on first use; instead it logs and no-ops.
  protected MicrometerMetricsCollector() {
    this.registry = null;
  }

  @Inject
  public MicrometerMetricsCollector(MeterRegistry registry) {
    this.registry = registry;
  }

  @Override
  public void jobStarted(long jobId, JobType type, JobPriority priority) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.jobs.started")
        .tag("type", type.name())
        .tag("priority", priority.name())
        .register(registry)
        .increment();
  }

  @Override
  public void jobCompleted(long jobId, JobType type, long executionTimeMs) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.jobs.completed")
        .tag("type", type.name())
        .register(registry)
        .increment();

    Timer.builder("ratchet.jobs.duration")
        .tag("type", type.name())
        .register(registry)
        .record(Duration.ofMillis(executionTimeMs));
  }

  @Override
  public void jobFailed(long jobId, JobType type, Throwable cause, int attempt) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.jobs.failed")
        .tag("type", type.name())
        .tag("exception", cause.getClass().getSimpleName())
        .register(registry)
        .increment();
  }

  @Override
  public void callbackFailed(long jobId, JobType type, Throwable cause, int attempt) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.callbacks.failed")
        .tag("type", type.name())
        .tag("exception", cause.getClass().getSimpleName())
        .register(registry)
        .increment();
  }
}

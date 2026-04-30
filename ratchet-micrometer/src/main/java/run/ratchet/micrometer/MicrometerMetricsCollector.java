package run.ratchet.micrometer;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.spi.ExceptionFamily;
import run.ratchet.spi.MetricsCollector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;

/**
 * Metrics published:
 *
 * <ul>
 *   <li>{@code ratchet.jobs.started} — counter, tagged by type and priority
 *   <li>{@code ratchet.jobs.completed} — counter, tagged by type
 *   <li>{@code ratchet.jobs.failed} — counter, tagged by type and exception family
 *   <li>{@code ratchet.jobs.duration} — timer, tagged by type
 *   <li>{@code ratchet.store.finalization.retries} — counter, tagged by type
 *   <li>{@code ratchet.store.finalization.minimal_success} — counter, tagged by type
 *   <li>{@code ratchet.store.finalization.stuck} — counter, tagged by type
 *   <li>{@code ratchet.store.claim.transient_failures} — counter, tagged by execution type
 *   <li>{@code ratchet.poller.claimed.jobs} — counter, tagged by execution type
 *   <li>{@code ratchet.submission.gate.rejections} — counter, tagged by execution type and gate
 *   <li>{@code ratchet.wakeup.local} — counter, tagged by source
 *   <li>{@code ratchet.wakeup.cluster.publish} — counter, tagged by transport and outcome
 *   <li>{@code ratchet.wakeup.cluster.receive} — counter, tagged by transport and outcome
 *   <li>{@code ratchet.callbacks.failed} — counter, tagged by type and exception family
 *   <li>{@code ratchet.poller.breaker.state} — gauge, tagged by breaker
 *   <li>{@code ratchet.store.operation} — timer, tagged by store, operation, and outcome
 * </ul>
 */
@Alternative
@Priority(1000)
@ApplicationScoped
public class MicrometerMetricsCollector implements MetricsCollector {

  private static final Logger log = Logger.getLogger(MicrometerMetricsCollector.class);

  private final MeterRegistry registry;
  private final Map<String, AtomicInteger> pollerBreakerStates = new ConcurrentHashMap<>();

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
  public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
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
  public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
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
  public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    if (registry == null) {
      return;
    }
    ExceptionFamily family = ExceptionFamily.classify(cause);
    Counter.builder("ratchet.jobs.failed")
        .tag("type", type.name())
        .tag("family", family.name())
        .register(registry)
        .increment();
    logRawFailure("job", jobId, type, cause, family, attempt);
  }

  @Override
  public void successFinalizationRetried(UUID jobId, JobType type) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.store.finalization.retries")
        .tag("type", type.name())
        .register(registry)
        .increment();
  }

  @Override
  public void successFinalizationMinimal(UUID jobId, JobType type) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.store.finalization.minimal_success")
        .tag("type", type.name())
        .register(registry)
        .increment();
  }

  @Override
  public void successFinalizationStuck(UUID jobId, JobType type) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.store.finalization.stuck")
        .tag("type", type.name())
        .register(registry)
        .increment();
  }

  @Override
  public void claimTransientFailure(String executionType) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.store.claim.transient_failures")
        .tag("execution_type", executionType)
        .register(registry)
        .increment();
  }

  @Override
  public void jobsClaimed(String executionType, int claimedCount) {
    if (registry == null || claimedCount <= 0) {
      return;
    }
    Counter.builder("ratchet.poller.claimed.jobs")
        .tag("execution_type", executionType)
        .register(registry)
        .increment(claimedCount);
  }

  @Override
  public void gateRejected(String executionType, String gateStatus) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.submission.gate.rejections")
        .tag("execution_type", executionType)
        .tag("gate_status", gateStatus)
        .register(registry)
        .increment();
  }

  @Override
  public void localWakeup(String source) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.wakeup.local").tag("source", source).register(registry).increment();
  }

  @Override
  public void clusterWakeupPublished(String transport, String outcome) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.wakeup.cluster.publish")
        .tag("transport", transport)
        .tag("outcome", outcome)
        .register(registry)
        .increment();
  }

  @Override
  public void clusterWakeupReceived(String transport, String outcome) {
    if (registry == null) {
      return;
    }
    Counter.builder("ratchet.wakeup.cluster.receive")
        .tag("transport", transport)
        .tag("outcome", outcome)
        .register(registry)
        .increment();
  }

  @Override
  public void callbackFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    if (registry == null) {
      return;
    }
    ExceptionFamily family = ExceptionFamily.classify(cause);
    Counter.builder("ratchet.callbacks.failed")
        .tag("type", type.name())
        .tag("family", family.name())
        .register(registry)
        .increment();
    logRawFailure("callback", jobId, type, cause, family, attempt);
  }

  @Override
  public void storeOperation(String store, String operation, String outcome, long durationNanos) {
    if (registry == null) {
      return;
    }
    Timer.builder("ratchet.store.operation")
        .tag("store", store)
        .tag("operation", operation)
        .tag("outcome", outcome)
        .register(registry)
        .record(Duration.ofNanos(durationNanos));
  }

  @Override
  public void pollerBreakerState(String breakerName, String state) {
    if (registry == null || breakerName == null || breakerName.isBlank()) {
      return;
    }
    AtomicInteger gaugeValue =
        pollerBreakerStates.computeIfAbsent(
            breakerName,
            key -> {
              AtomicInteger stateValue = new AtomicInteger();
              Gauge.builder("ratchet.poller.breaker.state", stateValue, AtomicInteger::get)
                  .tag("breaker", key)
                  .register(registry);
              return stateValue;
            });
    gaugeValue.set(toBreakerStateValue(state));
  }

  private void logRawFailure(
      String context,
      UUID jobId,
      JobType type,
      Throwable cause,
      ExceptionFamily family,
      int attempt) {
    String className = cause != null ? cause.getClass().getName() : "null";
    log.warnf(
        "%s failure recorded for job %s (type=%s, family=%s, attempt=%d, exception=%s)",
        context, jobId, type.name(), family.name(), attempt, className);
  }

  private int toBreakerStateValue(String state) {
    if ("OPEN".equals(state)) {
      return 2;
    }
    if ("HALF_OPEN".equals(state)) {
      return 1;
    }
    return 0;
  }
}

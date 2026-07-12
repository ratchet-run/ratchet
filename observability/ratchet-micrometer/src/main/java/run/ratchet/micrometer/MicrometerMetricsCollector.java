/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.Logger;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.SignalDecision;
import run.ratchet.spi.ExceptionFamily;
import run.ratchet.spi.MetricsCollector;

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
 *   <li>{@code ratchet.execution.target.fallback} — counter, tagged by requested and effective
 *       execution targets
 *   <li>{@code ratchet.wakeup.cluster.publish} — counter, tagged by transport and outcome
 *   <li>{@code ratchet.wakeup.cluster.receive} — counter, tagged by transport and outcome
 *   <li>{@code ratchet.callbacks.failed} — counter, tagged by type and exception family
 *   <li>{@code ratchet.signal.waiting} — counter, tagged by type
 *   <li>{@code ratchet.signal.delivered} — counter, tagged by type and outcome
 *   <li>{@code ratchet.signal.timed_out} — counter, tagged by type
 *   <li>{@code ratchet.signal.cancelled} — counter, tagged by type
 *   <li>{@code ratchet.poller.breaker.state} — gauge, tagged by breaker; values are {@code 0} for
 *       closed/unknown, {@code 1} for half-open, and {@code 2} for open
 *   <li>{@code ratchet.store.operation} — timer, tagged by store, operation, and outcome
 *   <li>{@code ratchet.encryption.integrity.violations} — counter, tagged by protected surface
 *   <li>{@code ratchet.encryption.envelope.version_skew} — counter, tagged by bounded version gap
 *       ({@code next}, {@code multiple_versions_ahead}, or {@code not_newer})
 * </ul>
 *
 * <p>The signal counters also carry a {@code signal_key} tag, but the default {@link
 * MicrometerMetricTagPolicy} has no allowlist entry for it, so every key collapses to {@code OTHER}
 * (or {@code UNKNOWN} when blank). Signal keys are unbounded application strings; a deployment that
 * wants the key as a real dimension must add it to the policy allowlist and accept the cardinality
 * cost.
 *
 * @apiNote The no-arg constructor on this class is a CDI proxying artefact (Weld / OWB require a
 *     non-final, no-arg-constructable concrete class to generate the normal-scope client proxy) and
 *     is NOT a supported public extension point. It is package-private to keep it out of the public
 *     API surface. Direct subclassing is not part of the public API contract; deployments that need
 *     custom behaviour SHOULD install an {@code @Alternative} bean implementing {@link
 *     MetricsCollector} rather than extending this class.
 */
@Alternative
@Priority(1000)
@ApplicationScoped
public class MicrometerMetricsCollector implements MetricsCollector {

  private static final Logger log = Logger.getLogger(MicrometerMetricsCollector.class);
  private static final MicrometerMetricTagPolicy DEFAULT_TAG_POLICY =
      MicrometerMetricTagPolicy.defaultPolicy();
  private static final int BREAKER_STATE_CLOSED_OR_UNKNOWN = 0;
  private static final int BREAKER_STATE_HALF_OPEN = 1;
  private static final int BREAKER_STATE_OPEN = 2;

  private final MeterRegistry registry;
  private final MicrometerMetricTagPolicy tagPolicy;
  private final Map<MeterKey, Counter> counters = new ConcurrentHashMap<>();
  private final Map<MeterKey, Timer> timers = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> pollerBreakerStates = new ConcurrentHashMap<>();

  // Required by CDI proxy. The CDI proxy never invokes business methods on this instance —
  // every real call goes to the @Inject constructor below. We still guard the field below
  // so a misconfigured deployment doesn't NPE on first use; instead it logs and no-ops.
  MicrometerMetricsCollector() {
    this.registry = null;
    this.tagPolicy = DEFAULT_TAG_POLICY;
  }

  @Inject
  public MicrometerMetricsCollector(
      MeterRegistry registry, Instance<MicrometerMetricTagPolicy> tagPolicy) {
    this(
        registry,
        tagPolicy.isResolvable() ? DEFAULT_TAG_POLICY.and(tagPolicy.get()) : DEFAULT_TAG_POLICY);
  }

  public MicrometerMetricsCollector(MeterRegistry registry) {
    this(registry, DEFAULT_TAG_POLICY);
  }

  public MicrometerMetricsCollector(MeterRegistry registry, MicrometerMetricTagPolicy tagPolicy) {
    this.registry = registry;
    this.tagPolicy =
        DEFAULT_TAG_POLICY.and(Objects.requireNonNull(tagPolicy, "tagPolicy must not be null"));
  }

  @Override
  public void jobStarted(UUID jobId, JobType type, JobPriority priority) {
    if (registry == null) {
      return;
    }
    counter("ratchet.jobs.started", "type", type.name(), "priority", priority.name()).increment();
  }

  @Override
  public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {
    if (registry == null) {
      return;
    }
    counter("ratchet.jobs.completed", "type", type.name()).increment();
    timer("ratchet.jobs.duration", "type", type.name()).record(Duration.ofMillis(executionTimeMs));
  }

  @Override
  public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    if (registry == null) {
      return;
    }
    ExceptionFamily family = ExceptionFamily.classify(cause);
    counter("ratchet.jobs.failed", "type", type.name(), "family", family.name()).increment();
    logRawFailure("job", jobId, type, cause, family, attempt);
  }

  @Override
  public void successFinalizationRetried(UUID jobId, JobType type) {
    if (registry == null) {
      return;
    }
    counter("ratchet.store.finalization.retries", "type", type.name()).increment();
  }

  @Override
  public void successFinalizationMinimal(UUID jobId, JobType type) {
    if (registry == null) {
      return;
    }
    counter("ratchet.store.finalization.minimal_success", "type", type.name()).increment();
  }

  @Override
  public void successFinalizationStuck(UUID jobId, JobType type) {
    if (registry == null) {
      return;
    }
    counter("ratchet.store.finalization.stuck", "type", type.name()).increment();
  }

  @Override
  public void claimTransientFailure(String executionType) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.store.claim.transient_failures",
            "execution_type",
            tag("execution_type", executionType))
        .increment();
  }

  @Override
  public void jobsClaimed(String executionType, int claimedCount) {
    if (registry == null || claimedCount <= 0) {
      return;
    }
    counter("ratchet.poller.claimed.jobs", "execution_type", tag("execution_type", executionType))
        .increment(claimedCount);
  }

  @Override
  public void gateRejected(String executionType, String gateStatus) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.submission.gate.rejections",
            "execution_type",
            tag("execution_type", executionType),
            "gate_status",
            tag("gate_status", gateStatus))
        .increment();
  }

  @Override
  public void localWakeup(String source) {
    if (registry == null) {
      return;
    }
    counter("ratchet.wakeup.local", "source", tag("source", source)).increment();
  }

  @Override
  public void executionTargetFallback(String requested, String effective) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.execution.target.fallback",
            "requested_target",
            tag("requested_target", requested),
            "effective_target",
            tag("effective_target", effective))
        .increment();
  }

  @Override
  public void clusterWakeupPublished(String transport, String outcome) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.wakeup.cluster.publish",
            "transport",
            tag("transport", transport),
            "outcome",
            tag("outcome", outcome))
        .increment();
  }

  @Override
  public void clusterWakeupReceived(String transport, String outcome) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.wakeup.cluster.receive",
            "transport",
            tag("transport", transport),
            "outcome",
            tag("outcome", outcome))
        .increment();
  }

  @Override
  public void callbackFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    if (registry == null) {
      return;
    }
    ExceptionFamily family = ExceptionFamily.classify(cause);
    counter("ratchet.callbacks.failed", "type", type.name(), "family", family.name()).increment();
    logRawFailure("callback", jobId, type, cause, family, attempt);
  }

  @Override
  public void signalWaiting(UUID jobId, JobType type, String signalKey) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.signal.waiting",
            "type",
            type.name(),
            "signal_key",
            tag("signal_key", signalKey))
        .increment();
  }

  @Override
  public void signalDelivered(
      UUID jobId, JobType type, String signalKey, SignalDecision.Outcome outcome) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.signal.delivered",
            "type",
            type.name(),
            "signal_key",
            tag("signal_key", signalKey),
            "outcome",
            outcome != null ? outcome.name() : "UNKNOWN")
        .increment();
  }

  @Override
  public void signalTimedOut(UUID jobId, JobType type, String signalKey) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.signal.timed_out",
            "type",
            type.name(),
            "signal_key",
            tag("signal_key", signalKey))
        .increment();
  }

  @Override
  public void signalCancelled(UUID jobId, JobType type, String signalKey) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.signal.cancelled",
            "type",
            type.name(),
            "signal_key",
            tag("signal_key", signalKey))
        .increment();
  }

  @Override
  public void storeOperation(String store, String operation, String outcome, long durationNanos) {
    if (registry == null) {
      return;
    }
    timer(
            "ratchet.store.operation",
            "store",
            tag("store", store),
            "operation",
            tag("operation", operation),
            "outcome",
            tag("outcome", outcome))
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
                  .tag("breaker", tag("breaker", key))
                  .register(registry);
              return stateValue;
            });
    gaugeValue.set(toBreakerStateValue(state));
  }

  @Override
  public void encryptionIntegrityViolation(UUID jobId, String surface) {
    if (registry == null) {
      return;
    }
    counter("ratchet.encryption.integrity.violations", "surface", tag("surface", surface))
        .increment();
  }

  @Override
  public void encryptionEnvelopeVersionSkew(UUID jobId, int version, int maxSupportedVersion) {
    if (registry == null) {
      return;
    }
    counter(
            "ratchet.encryption.envelope.version_skew",
            "version_gap",
            tag("version_gap", envelopeVersionGap(version, maxSupportedVersion)))
        .increment();
  }

  private static String envelopeVersionGap(int version, int maxSupportedVersion) {
    long gap = (long) version - maxSupportedVersion;
    if (gap == 1L) {
      return "next";
    }
    return gap > 1L ? "multiple_versions_ahead" : "not_newer";
  }

  private Counter counter(String name, String... tags) {
    MeterKey key = MeterKey.of(name, tags);
    return counters.computeIfAbsent(key, ignored -> registerCounter(key));
  }

  private Timer timer(String name, String... tags) {
    MeterKey key = MeterKey.of(name, tags);
    return timers.computeIfAbsent(key, ignored -> registerTimer(key));
  }

  private Counter registerCounter(MeterKey key) {
    Counter.Builder builder = Counter.builder(key.name());
    applyTags(builder, key.tags());
    return builder.register(registry);
  }

  private Timer registerTimer(MeterKey key) {
    Timer.Builder builder = Timer.builder(key.name());
    applyTags(builder, key.tags());
    return builder.register(registry);
  }

  private static void applyTags(Counter.Builder builder, List<String> tags) {
    for (int i = 0; i < tags.size(); i += 2) {
      builder.tag(tags.get(i), tags.get(i + 1));
    }
  }

  private static void applyTags(Timer.Builder builder, List<String> tags) {
    for (int i = 0; i < tags.size(); i += 2) {
      builder.tag(tags.get(i), tags.get(i + 1));
    }
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
      return BREAKER_STATE_OPEN;
    }
    if ("HALF_OPEN".equals(state)) {
      return BREAKER_STATE_HALF_OPEN;
    }
    return BREAKER_STATE_CLOSED_OR_UNKNOWN;
  }

  private String tag(String tagName, String value) {
    return tagPolicy.metricTagValue(tagName, value);
  }

  private record MeterKey(String name, List<String> tags) {

    private MeterKey {
      tags = List.copyOf(Objects.requireNonNull(tags, "tags must not be null"));
      if (tags.size() % 2 != 0) {
        throw new IllegalArgumentException("Micrometer tags must be supplied as name/value pairs");
      }
      name = Objects.requireNonNull(name, "name must not be null");
    }

    private static MeterKey of(String name, String... tags) {
      return new MeterKey(name, List.of(tags));
    }
  }
}

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
package run.ratchet.api;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Immutable CDI-producible runtime options for Ratchet.
 *
 * <p>Declared as a non-final class (not a record) so CDI normal scopes such as
 * {@code @ApplicationScoped} can generate a client proxy around an application's {@code @Produces}
 * bean. Instances are still immutable — all fields are {@code final} and set only through {@link
 * Builder}. Use {@link #builder()} or {@link #defaults()} to construct.
 */
@SuppressWarnings("ClassCanBeRecord")
public class RatchetOptions {

  private final PollingOptions polling;
  private final ExecutionOptions execution;
  private final NodeOptions node;
  private final RecurringOptions recurring;
  private final RetryBufferOptions retryBuffer;
  private final TimeoutOptions timeout;
  private final MaintenanceOptions maintenance;
  private final NotificationOptions notifications;
  private final SchemaOptions schema;
  private final PayloadOptions payload;
  private final MetricsOptions metrics;
  private final SecurityOptions security;
  private final StoreOptions store;
  private final CircuitBreakerOptions circuitBreaker;

  /**
   * No-arg constructor used only by CDI to generate the client proxy subclass. Sets all fields to
   * {@code null}; the proxy never invokes these fields because every method call is intercepted and
   * delegated to the real {@code @Produces} bean instance. Do not invoke directly — use {@link
   * #builder()} or {@link #defaults()}.
   */
  protected RatchetOptions() {
    this.polling = null;
    this.execution = null;
    this.node = null;
    this.recurring = null;
    this.retryBuffer = null;
    this.timeout = null;
    this.maintenance = null;
    this.notifications = null;
    this.schema = null;
    this.payload = null;
    this.metrics = null;
    this.security = null;
    this.store = null;
    this.circuitBreaker = null;
  }

  public RatchetOptions(
      PollingOptions polling,
      ExecutionOptions execution,
      NodeOptions node,
      RecurringOptions recurring,
      RetryBufferOptions retryBuffer,
      TimeoutOptions timeout,
      MaintenanceOptions maintenance,
      NotificationOptions notifications,
      SchemaOptions schema,
      PayloadOptions payload,
      MetricsOptions metrics,
      SecurityOptions security,
      StoreOptions store,
      CircuitBreakerOptions circuitBreaker) {
    this.polling = Objects.requireNonNull(polling, "polling must not be null");
    this.execution = Objects.requireNonNull(execution, "execution must not be null");
    this.node = Objects.requireNonNull(node, "node must not be null");
    this.recurring = Objects.requireNonNull(recurring, "recurring must not be null");
    this.retryBuffer = Objects.requireNonNull(retryBuffer, "retryBuffer must not be null");
    this.timeout = Objects.requireNonNull(timeout, "timeout must not be null");
    this.maintenance = Objects.requireNonNull(maintenance, "maintenance must not be null");
    this.notifications = Objects.requireNonNull(notifications, "notifications must not be null");
    this.schema = Objects.requireNonNull(schema, "schema must not be null");
    this.payload = Objects.requireNonNull(payload, "payload must not be null");
    this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    this.security = Objects.requireNonNull(security, "security must not be null");
    this.store = Objects.requireNonNull(store, "store must not be null");
    this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
  }

  public static RatchetOptions defaults() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  private static Map<String, Integer> defaultConcurrency() {
    Map<String, Integer> defaults = new HashMap<>();
    // Keys are JobExecutionType.name() values, but ratchet-api intentionally does not depend on
    // the store module that owns that enum.
    defaults.put("SINGLE", 20);
    defaults.put("RECURRING", 5);
    defaults.put("BATCH_CHILD", 30);
    defaults.put("BATCH_PARENT", 2);
    defaults.put("CHAIN_STEP", 10);
    defaults.put("DLQ_ALERT", 2);
    defaults.put("WORKFLOW_BRANCH", 10);
    defaults.put("WORKFLOW_JOIN", 10);
    return defaults;
  }

  private static Map<CircuitBreakerProfile, CircuitBreakerProfileBuilder>
      defaultCircuitBreakerProfiles() {
    Map<CircuitBreakerProfile, CircuitBreakerProfileBuilder> profiles =
        new EnumMap<>(CircuitBreakerProfile.class);
    profiles.put(CircuitBreakerProfile.DEFAULT, circuitBreakerProfile(50.0f, 100, 30000L, 3, 5));
    profiles.put(CircuitBreakerProfile.FAST, circuitBreakerProfile(50.0f, 20, 10000L, 2, 3));
    profiles.put(CircuitBreakerProfile.CRITICAL, circuitBreakerProfile(75.0f, 200, 60000L, 5, 10));
    profiles.put(
        CircuitBreakerProfile.EXTERNAL_API, circuitBreakerProfile(60.0f, 50, 60000L, 3, 5));
    profiles.put(CircuitBreakerProfile.CLAIM_PATH, circuitBreakerProfile(50.0f, 20, 5000L, 1, 5));
    return profiles;
  }

  private static CircuitBreakerProfileBuilder circuitBreakerProfile(
      float failureRateThreshold,
      int slidingWindowSize,
      long waitDurationMs,
      int permittedCallsInHalfOpen,
      int minimumCalls) {
    CircuitBreakerProfileBuilder builder = new CircuitBreakerProfileBuilder();
    builder.failureRateThreshold(failureRateThreshold);
    builder.slidingWindowSize(slidingWindowSize);
    builder.waitDurationMs(waitDurationMs);
    builder.permittedCallsInHalfOpen(permittedCallsInHalfOpen);
    builder.minimumCalls(minimumCalls);
    return builder;
  }

  private static String normalizeKey(String key) {
    String normalized = requireText("key", key).trim().replace('-', '_').toUpperCase(Locale.ROOT);
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("key must not be blank");
    }
    return normalized;
  }

  private static String requireText(String name, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value.trim();
  }

  private static int atLeast(String name, int value, int minInclusive) {
    if (value < minInclusive) {
      throw new IllegalArgumentException(name + " must be at least " + minInclusive);
    }
    return value;
  }

  private static String requireNonBlank(String name, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String blankToNull(String value) {
    return (value == null || value.isBlank()) ? null : value.trim();
  }

  private static long atLeast(String name, long value, long minInclusive) {
    if (value < minInclusive) {
      throw new IllegalArgumentException(name + " must be at least " + minInclusive);
    }
    return value;
  }

  private static void requireNotGreater(
      String lowerName, long lowerValue, String upperName, long upperValue) {
    if (lowerValue > upperValue) {
      throw new IllegalArgumentException(lowerName + " must not exceed " + upperName);
    }
  }

  /** Returns the polling configuration. */
  public PollingOptions polling() {
    return polling;
  }

  /** Returns the execution configuration. */
  public ExecutionOptions execution() {
    return execution;
  }

  /** Returns the node configuration. */
  public NodeOptions node() {
    return node;
  }

  /** Returns the recurring job configuration. */
  public RecurringOptions recurring() {
    return recurring;
  }

  /** Returns the retry buffer configuration. */
  public RetryBufferOptions retryBuffer() {
    return retryBuffer;
  }

  /** Returns the timeout configuration. */
  public TimeoutOptions timeout() {
    return timeout;
  }

  /** Returns the maintenance configuration. */
  public MaintenanceOptions maintenance() {
    return maintenance;
  }

  /** Returns the notification configuration. */
  public NotificationOptions notifications() {
    return notifications;
  }

  /** Returns the schema migration configuration. */
  public SchemaOptions schema() {
    return schema;
  }

  /** Returns the payload configuration. */
  public PayloadOptions payload() {
    return payload;
  }

  /** Returns the metrics configuration. */
  public MetricsOptions metrics() {
    return metrics;
  }

  /** Returns the security configuration. */
  public SecurityOptions security() {
    return security;
  }

  /** Returns the store configuration. */
  public StoreOptions store() {
    return store;
  }

  /** Returns the circuit breaker configuration. */
  public CircuitBreakerOptions circuitBreaker() {
    return circuitBreaker;
  }

  public enum IsolationCheckMode {
    WARN,
    FAIL,
    DISABLE
  }

  /**
   * Selects which configured executor pool a job runs on by default when the job itself specifies
   * no target. {@link #PLATFORM} is always available; {@link #VIRTUAL} requires a configured
   * virtual executor and otherwise falls back to platform.
   */
  public enum ThreadingMode {
    PLATFORM(ExecutorTargets.PLATFORM),
    VIRTUAL(ExecutorTargets.VIRTUAL);

    private final String target;

    ThreadingMode(String target) {
      this.target = target;
    }

    /** Returns the reserved execution-target name this mode resolves to. */
    public String target() {
      return target;
    }
  }

  /**
   * Poller cadence and claim-loop tuning.
   *
   * @param batchSize maximum number of jobs claimed per poller tick
   * @param burstDelayMs delay in milliseconds between consecutive ticks while work was available on
   *     the previous tick
   * @param minDelayMs minimum idle delay in milliseconds applied by the backoff scheduler
   * @param maxDelayMs maximum idle delay in milliseconds applied by the backoff scheduler; must be
   *     {@code >= minDelayMs}
   * @param deepIdleDelayMs delay in milliseconds applied once the deep-idle threshold is reached
   * @param deepIdleThresholdMs idle duration in milliseconds after which {@code deepIdleDelayMs}
   *     replaces the normal backoff
   * @param idleThreshold number of consecutive empty ticks before backoff begins ramping
   * @param claimHeadroomFactor headroom multiplier applied to free-slot calculations; {@code 0}
   *     disables the headroom adjustment
   */
  public record PollingOptions(
      int batchSize,
      long burstDelayMs,
      long minDelayMs,
      long maxDelayMs,
      long deepIdleDelayMs,
      long deepIdleThresholdMs,
      int idleThreshold,
      int claimHeadroomFactor) {}

  /**
   * Execution-pool wiring: target executor JNDI names, queue size, and per-execution-type
   * concurrency / virtual-thread / rate caps.
   *
   * @param defaultThreadingMode default threading mode applied when a job pins no execution target;
   *     defaults to {@link ThreadingMode#PLATFORM}
   * @param queueSize bounded queue length used by the in-process fallback executor
   * @param maxConcurrency map of execution-type name to maximum concurrent executions; missing keys
   *     fall back to the per-type defaults
   * @param virtualThreadLimits map of execution-type name to virtual-thread cap; missing keys mean
   *     no virtual-thread cap is applied
   * @param rateLimitsPerMinute map of execution-type name to per-minute rate limit; missing keys
   *     mean no rate limit is applied
   * @param jobExecutorJndi JNDI name of the {@code ManagedExecutorService} that runs jobs
   * @param scheduledExecutorJndi JNDI name of the {@code ManagedScheduledExecutorService} used for
   *     scheduled work
   * @param virtualExecutorJndi JNDI name of the {@code ManagedExecutorService} used for
   *     virtual-thread execution; when blank, virtual-targeted jobs fall back to the platform pool
   * @param virtualCounterAccounting {@code true} to use lock-free counter accounting for the
   *     virtual pool instead of the default semaphore bound
   */
  public record ExecutionOptions(
      ThreadingMode defaultThreadingMode,
      int queueSize,
      Map<String, Integer> maxConcurrency,
      Map<String, Integer> virtualThreadLimits,
      Map<String, Integer> rateLimitsPerMinute,
      String jobExecutorJndi,
      String scheduledExecutorJndi,
      String virtualExecutorJndi,
      boolean virtualCounterAccounting) {

    public ExecutionOptions {
      maxConcurrency = Map.copyOf(maxConcurrency);
      virtualThreadLimits = Map.copyOf(virtualThreadLimits);
      rateLimitsPerMinute = Map.copyOf(rateLimitsPerMinute);
      jobExecutorJndi = requireNonBlank("jobExecutorJndi", jobExecutorJndi);
      scheduledExecutorJndi = requireNonBlank("scheduledExecutorJndi", scheduledExecutorJndi);
      defaultThreadingMode =
          defaultThreadingMode == null ? ThreadingMode.PLATFORM : defaultThreadingMode;
      virtualExecutorJndi = blankToNull(virtualExecutorJndi);
    }

    /**
     * Returns true when a virtual executor pool is configured. A virtual-targeted job falls back to
     * the platform pool when this is false.
     */
    public boolean hasVirtualExecutor() {
      return virtualExecutorJndi != null;
    }

    public int maxConcurrency(String executionTypeName, int defaultValue) {
      return maxConcurrency.getOrDefault(normalizeKey(executionTypeName), defaultValue);
    }

    public int virtualThreadLimit(String executionTypeName, int defaultValue) {
      return virtualThreadLimits.getOrDefault(normalizeKey(executionTypeName), defaultValue);
    }

    public int rateLimitPerMinute(String executionTypeName) {
      return rateLimitsPerMinute.getOrDefault(normalizeKey(executionTypeName), 0);
    }
  }

  /**
   * Node identity, heartbeat cadence, and tag-affinity selection for this scheduler instance.
   *
   * @param nodeId explicit node id; when {@code null} or blank an id is generated at startup
   * @param heartbeatIntervalSeconds interval at which the node reports liveness to the store
   * @param orphanGraceSeconds grace period in seconds before a missed-heartbeat node is considered
   *     dead and its in-flight jobs become eligible for reclaim
   * @param orphanScanIntervalMinutes interval in minutes between orphan-reclaim scans
   * @param dynamicHeartbeatEnabled {@code true} to allow the heartbeat interval to adapt to
   *     observed cluster activity
   * @param requireTags job tags that must be present on the node for a job to be claimed here;
   *     empty list disables the require-list constraint
   * @param excludeTags job tags that, when present on a job, exclude that job from this node
   */
  public record NodeOptions(
      String nodeId,
      long heartbeatIntervalSeconds,
      long orphanGraceSeconds,
      long orphanScanIntervalMinutes,
      boolean dynamicHeartbeatEnabled,
      List<String> requireTags,
      List<String> excludeTags) {

    public NodeOptions {
      requireTags = List.copyOf(requireTags == null ? List.of() : requireTags);
      excludeTags = List.copyOf(excludeTags == null ? List.of() : excludeTags);
    }

    public Optional<String> explicitNodeId() {
      return nodeId == null || nodeId.isBlank() ? Optional.empty() : Optional.of(nodeId);
    }

    public NodeTagFilter tagFilter() {
      return new NodeTagFilter(requireTags, excludeTags);
    }
  }

  /**
   * Tuning for the recurring-job materializer.
   *
   * @param batchLimit maximum number of recurring jobs materialized per scheduler tick
   * @param pollMs nominal poll interval in milliseconds between materialization runs
   * @param maxPollMs maximum poll interval in milliseconds when no recurring work is due; must be
   *     {@code >= pollMs}
   * @param startupGraceSeconds delay in seconds before the materializer begins its first run after
   *     startup, giving the cluster time to converge on recurring registrations
   * @param convergenceWindowSeconds rolling window in seconds during which competing recurring
   *     registrations are reconciled; {@code 0} disables convergence reconciliation
   */
  public record RecurringOptions(
      int batchLimit,
      long pollMs,
      long maxPollMs,
      long startupGraceSeconds,
      long convergenceWindowSeconds) {}

  /**
   * Tuning for the in-memory retry buffer that batches failed-job retry inserts.
   *
   * @param drainIntervalMs interval in milliseconds between buffer drains to the store
   */
  public record RetryBufferOptions(long drainIntervalMs) {}

  /**
   * Job-execution timeout tuning.
   *
   * @param softTimeoutPercent percentage (1..99) of the configured SLA at which a soft-timeout
   *     warning is emitted
   * @param defaultSlaSeconds default execution SLA in seconds applied to jobs that do not declare
   *     their own
   * @param signalTimeoutBatchSize maximum number of WAITING jobs scanned per signal-timeout tick
   */
  public record TimeoutOptions(
      int softTimeoutPercent, long defaultSlaSeconds, int signalTimeoutBatchSize) {}

  /**
   * Background-maintenance schedules: DLQ purge, job archive, log purge.
   *
   * @param dlqPurgeEnabled {@code true} to run the DLQ purge job
   * @param dlqPurgeCron cron expression controlling the DLQ purge cadence
   * @param dlqPurgeDays age in days at which DLQ entries become eligible for purge
   * @param jobArchiveEnabled {@code true} to run the job-archive job
   * @param jobArchiveCron cron expression controlling the job-archive cadence
   * @param jobRetentionDays age in days at which terminal jobs become eligible for archive
   * @param jobArchiveBatchSize maximum number of jobs archived per archive pass
   * @param logPurgeEnabled {@code true} to run the execution-log purge job
   * @param logPurgeCron cron expression controlling the log-purge cadence
   * @param logRetentionDays age in days at which execution-log rows become eligible for purge
   */
  public record MaintenanceOptions(
      boolean dlqPurgeEnabled,
      String dlqPurgeCron,
      long dlqPurgeDays,
      boolean jobArchiveEnabled,
      String jobArchiveCron,
      long jobRetentionDays,
      int jobArchiveBatchSize,
      boolean logPurgeEnabled,
      String logPurgeCron,
      long logRetentionDays) {}

  /**
   * Notification-channel routing for DLQ and timeout alerts. Channel strings are interpreted by the
   * application's notification adapter (Slack-style channels are the convention).
   *
   * @param enabled {@code true} to publish notifications at all
   * @param dlqAlertChannel channel identifier for DLQ alerts
   * @param timeoutAlertChannel channel identifier for timeout alerts
   */
  public record NotificationOptions(
      boolean enabled, String dlqAlertChannel, String timeoutAlertChannel) {}

  /**
   * Schema-migration configuration for SQL stores. See {@code SchemaMigrator} for behavior; this
   * record controls whether migration runs and where the dialect-specific scripts are loaded from.
   *
   * @param autoMigrate {@code true} to run the migrator from the schema-migration lifecycle hook
   * @param migrationDialect explicit dialect identifier (for example {@code mysql} or {@code
   *     postgresql}); empty string asks the store to autodetect
   * @param migrationPrefix classpath prefix under which dialect-specific migration scripts are
   *     resolved
   */
  public record SchemaOptions(
      boolean autoMigrate, String migrationDialect, String migrationPrefix) {}

  /**
   * Limits on job payload and persisted result sizes.
   *
   * @param maxPayloadKb maximum job payload size in kilobytes
   * @param maxResultBytes maximum persisted job-result size in bytes
   */
  public record PayloadOptions(int maxPayloadKb, long maxResultBytes) {}

  /**
   * Metrics emission configuration.
   *
   * @param clustering clustering-strategy identifier passed to the metrics collector (for example
   *     {@code none}); values are lowercased
   */
  public record MetricsOptions(String clustering) {}

  /**
   * Security toggles for the scheduler runtime.
   *
   * @param allowEmptyClassPolicy {@code true} to permit running without a configured {@code
   *     ClassPolicy}; {@code false} (default) fails fast at startup when no policy is bound
   * @param redactEmails {@code true} to redact email-shaped strings from observability output
   * @param maskPayloads {@code true} to mask sensitive fields in structured payloads returned from
   *     a read API — the {@code params} map and trace context on a job detail, plus the serialized
   *     job result; {@code false} (default) leaves them unmasked. Map masking is key-based and
   *     result masking walks the serialized JSON; free-text fields such as {@code lastError} are
   *     not masked. The durable store payload is never affected.
   */
  public record SecurityOptions(
      boolean allowEmptyClassPolicy, boolean redactEmails, boolean maskPayloads) {}

  /**
   * Store-layer tuning.
   *
   * @param isolationCheckMode action taken when the configured transaction isolation level differs
   *     from the store's recommended level
   * @param priorityBoostIntervalMinutes interval in minutes at which long-waiting pending jobs are
   *     promoted to a higher effective priority; {@code 0} disables the boost
   */
  public record StoreOptions(
      IsolationCheckMode isolationCheckMode, int priorityBoostIntervalMinutes) {}

  /**
   * Circuit-breaker configuration container; holds the per-profile thresholds keyed by {@link
   * CircuitBreakerProfile}.
   *
   * @param enabled {@code true} to enable circuit-breaker enforcement; when {@code false} all calls
   *     pass through unmonitored
   * @param profiles per-profile thresholds; defensively copied at construction
   */
  @Incubating
  public record CircuitBreakerOptions(
      boolean enabled, Map<CircuitBreakerProfile, CircuitBreakerProfileOptions> profiles) {

    public CircuitBreakerOptions {
      profiles = Map.copyOf(profiles);
    }

    /**
     * Returns the configured options for a profile.
     *
     * @param profile profile to look up
     * @return configured options, or {@code null} if no options were configured for the profile
     */
    public @Nullable CircuitBreakerProfileOptions profile(CircuitBreakerProfile profile) {
      return profiles.get(profile);
    }
  }

  /**
   * Thresholds for a single circuit-breaker profile.
   *
   * @param failureRateThreshold failure-rate percentage (0.0..100.0) at which the breaker
   *     transitions from CLOSED to OPEN
   * @param slidingWindowSize size of the sliding window used to compute the failure rate
   * @param waitDurationMs duration in milliseconds the breaker stays OPEN before transitioning to
   *     HALF_OPEN
   * @param permittedCallsInHalfOpen number of trial calls allowed while in HALF_OPEN
   * @param minimumCalls minimum number of recorded calls required before the failure rate is
   *     evaluated
   */
  @Incubating
  public record CircuitBreakerProfileOptions(
      float failureRateThreshold,
      int slidingWindowSize,
      long waitDurationMs,
      int permittedCallsInHalfOpen,
      int minimumCalls) {}

  public static final class Builder {
    private final PollingBuilder polling = new PollingBuilder();
    private final ExecutionBuilder execution = new ExecutionBuilder();
    private final NodeBuilder node = new NodeBuilder();
    private final RecurringBuilder recurring = new RecurringBuilder();
    private final RetryBufferBuilder retryBuffer = new RetryBufferBuilder();
    private final TimeoutBuilder timeout = new TimeoutBuilder();
    private final MaintenanceBuilder maintenance = new MaintenanceBuilder();
    private final NotificationBuilder notifications = new NotificationBuilder();
    private final SchemaBuilder schema = new SchemaBuilder();
    private final PayloadBuilder payload = new PayloadBuilder();
    private final MetricsBuilder metrics = new MetricsBuilder();
    private final SecurityBuilder security = new SecurityBuilder();
    private final StoreBuilder store = new StoreBuilder();
    private final CircuitBreakerBuilder circuitBreaker = new CircuitBreakerBuilder();

    private Builder() {}

    public Builder polling(Consumer<PollingBuilder> customizer) {
      customizer.accept(polling);
      return this;
    }

    public Builder execution(Consumer<ExecutionBuilder> customizer) {
      customizer.accept(execution);
      return this;
    }

    public Builder node(Consumer<NodeBuilder> customizer) {
      customizer.accept(node);
      return this;
    }

    public Builder recurring(Consumer<RecurringBuilder> customizer) {
      customizer.accept(recurring);
      return this;
    }

    public Builder retryBuffer(Consumer<RetryBufferBuilder> customizer) {
      customizer.accept(retryBuffer);
      return this;
    }

    public Builder timeout(Consumer<TimeoutBuilder> customizer) {
      customizer.accept(timeout);
      return this;
    }

    public Builder maintenance(Consumer<MaintenanceBuilder> customizer) {
      customizer.accept(maintenance);
      return this;
    }

    public Builder notifications(Consumer<NotificationBuilder> customizer) {
      customizer.accept(notifications);
      return this;
    }

    public Builder schema(Consumer<SchemaBuilder> customizer) {
      customizer.accept(schema);
      return this;
    }

    public Builder payload(Consumer<PayloadBuilder> customizer) {
      customizer.accept(payload);
      return this;
    }

    public Builder metrics(Consumer<MetricsBuilder> customizer) {
      customizer.accept(metrics);
      return this;
    }

    public Builder security(Consumer<SecurityBuilder> customizer) {
      customizer.accept(security);
      return this;
    }

    public Builder store(Consumer<StoreBuilder> customizer) {
      customizer.accept(store);
      return this;
    }

    @Incubating
    public Builder circuitBreaker(Consumer<CircuitBreakerBuilder> customizer) {
      customizer.accept(circuitBreaker);
      return this;
    }

    public RatchetOptions build() {
      return new RatchetOptions(
          polling.build(),
          execution.build(),
          node.build(),
          recurring.build(),
          retryBuffer.build(),
          timeout.build(),
          maintenance.build(),
          notifications.build(),
          schema.build(),
          payload.build(),
          metrics.build(),
          security.build(),
          store.build(),
          circuitBreaker.build());
    }
  }

  public static final class PollingBuilder {
    private int batchSize = 50;
    private long burstDelayMs = 500L;
    private long minDelayMs = 2000L;
    private long maxDelayMs = 10000L;
    private long deepIdleDelayMs = 30000L;
    private long deepIdleThresholdMs = 60000L;
    private int idleThreshold = 3;
    private int claimHeadroomFactor = 0;

    private PollingBuilder() {}

    public PollingBuilder batchSize(int batchSize) {
      this.batchSize = atLeast("batchSize", batchSize, 1);
      return this;
    }

    public PollingBuilder burstDelayMs(long burstDelayMs) {
      this.burstDelayMs = atLeast("burstDelayMs", burstDelayMs, 0L);
      return this;
    }

    public PollingBuilder minDelayMs(long minDelayMs) {
      this.minDelayMs = atLeast("minDelayMs", minDelayMs, 0L);
      return this;
    }

    public PollingBuilder maxDelayMs(long maxDelayMs) {
      this.maxDelayMs = atLeast("maxDelayMs", maxDelayMs, 1L);
      return this;
    }

    public PollingBuilder deepIdleDelayMs(long deepIdleDelayMs) {
      this.deepIdleDelayMs = atLeast("deepIdleDelayMs", deepIdleDelayMs, 0L);
      return this;
    }

    public PollingBuilder deepIdleThresholdMs(long deepIdleThresholdMs) {
      this.deepIdleThresholdMs = atLeast("deepIdleThresholdMs", deepIdleThresholdMs, 0L);
      return this;
    }

    public PollingBuilder idleThreshold(int idleThreshold) {
      this.idleThreshold = atLeast("idleThreshold", idleThreshold, 0);
      return this;
    }

    public PollingBuilder claimHeadroomFactor(int claimHeadroomFactor) {
      this.claimHeadroomFactor = atLeast("claimHeadroomFactor", claimHeadroomFactor, 0);
      return this;
    }

    private PollingOptions build() {
      requireNotGreater("minDelayMs", minDelayMs, "maxDelayMs", maxDelayMs);
      return new PollingOptions(
          batchSize,
          burstDelayMs,
          minDelayMs,
          maxDelayMs,
          deepIdleDelayMs,
          deepIdleThresholdMs,
          idleThreshold,
          claimHeadroomFactor);
    }
  }

  public static final class ExecutionBuilder {
    private final Map<String, Integer> maxConcurrency = defaultConcurrency();
    private final Map<String, Integer> virtualThreadLimits = new HashMap<>();
    private final Map<String, Integer> rateLimitsPerMinute = new HashMap<>();
    private ThreadingMode defaultThreadingMode = ThreadingMode.PLATFORM;
    private int queueSize = 100;
    private String jobExecutorJndi = "java:comp/DefaultManagedExecutorService";
    private String scheduledExecutorJndi = "java:comp/DefaultManagedScheduledExecutorService";
    private String virtualExecutorJndi;
    private boolean virtualCounterAccounting;

    private ExecutionBuilder() {}

    /**
     * Sets the pool a job runs on when it specifies no target of its own. Defaults to {@link
     * ThreadingMode#PLATFORM}. Selecting {@link ThreadingMode#VIRTUAL} with no virtual executor
     * configured falls back to platform.
     */
    public ExecutionBuilder defaultThreadingMode(ThreadingMode defaultThreadingMode) {
      this.defaultThreadingMode =
          defaultThreadingMode == null ? ThreadingMode.PLATFORM : defaultThreadingMode;
      return this;
    }

    /**
     * Sets the JNDI name of an additional {@code ManagedExecutorService} that backs the virtual
     * pool. Absent (null or blank) means no virtual pool exists and virtual-targeted jobs fall back
     * to platform. As with {@link #jobExecutorJndi(String)}, whether the executor actually runs on
     * virtual threads is the container's decision.
     */
    public ExecutionBuilder virtualExecutorJndi(String virtualExecutorJndi) {
      this.virtualExecutorJndi = virtualExecutorJndi;
      return this;
    }

    /**
     * Opts the virtual pool into lock-free counter accounting instead of the default semaphore
     * bound. Only safe when the deployer has verified the executor is genuinely virtual-thread
     * backed; on a container that backs it with a small platform pool, counter accounting admits
     * far more work than the executor can run. Defaults to semaphore accounting.
     */
    public ExecutionBuilder virtualCounterAccounting(boolean virtualCounterAccounting) {
      this.virtualCounterAccounting = virtualCounterAccounting;
      return this;
    }

    /**
     * Sets the JNDI name of the {@code ManagedExecutorService} that runs jobs. Defaults to the
     * container's {@code java:comp/DefaultManagedExecutorService}. Point this at an executor your
     * application declares (e.g. an EE 11 {@code @ManagedExecutorDefinition(name =
     * "java:app/concurrent/MyVirtualExecutor", virtual = true)}); jobs then run on that executor.
     *
     * <p>Whether those jobs run on <em>virtual</em> threads is the container's decision: {@code
     * virtual = true} is a request a runtime may ignore (Eclipse GlassFish 8 honors it; WildFly 40
     * does not yet, so jobs run on platform threads there). Jakarta exposes no API to verify this
     * at runtime, so Ratchet can neither warn nor guarantee it.
     */
    public ExecutionBuilder jobExecutorJndi(String jobExecutorJndi) {
      this.jobExecutorJndi = requireNonBlank("jobExecutorJndi", jobExecutorJndi);
      return this;
    }

    /**
     * Sets the JNDI name of the {@code ManagedScheduledExecutorService} used for scheduled work.
     * Defaults to the container's {@code java:comp/DefaultManagedScheduledExecutorService}.
     */
    public ExecutionBuilder scheduledExecutorJndi(String scheduledExecutorJndi) {
      this.scheduledExecutorJndi = requireNonBlank("scheduledExecutorJndi", scheduledExecutorJndi);
      return this;
    }

    public ExecutionBuilder queueSize(int queueSize) {
      this.queueSize = atLeast("queueSize", queueSize, 0);
      return this;
    }

    public ExecutionBuilder maxConcurrency(String executionTypeName, int maxConcurrency) {
      this.maxConcurrency.put(
          normalizeKey(executionTypeName), atLeast("maxConcurrency", maxConcurrency, 0));
      return this;
    }

    public ExecutionBuilder virtualThreadLimit(String executionTypeName, int limit) {
      this.virtualThreadLimits.put(normalizeKey(executionTypeName), atLeast("limit", limit, 0));
      return this;
    }

    public ExecutionBuilder rateLimitPerMinute(String executionTypeName, int limit) {
      this.rateLimitsPerMinute.put(normalizeKey(executionTypeName), atLeast("limit", limit, 0));
      return this;
    }

    private ExecutionOptions build() {
      return new ExecutionOptions(
          defaultThreadingMode,
          queueSize,
          maxConcurrency,
          virtualThreadLimits,
          rateLimitsPerMinute,
          jobExecutorJndi,
          scheduledExecutorJndi,
          virtualExecutorJndi,
          virtualCounterAccounting);
    }
  }

  public static final class NodeBuilder {
    private String nodeId;
    private long heartbeatIntervalSeconds = 10L;
    private long orphanGraceSeconds = 60L;
    private long orphanScanIntervalMinutes = 5L;
    private boolean dynamicHeartbeatEnabled = true;
    private List<String> requireTags = List.of();
    private List<String> excludeTags = List.of();

    private NodeBuilder() {}

    public NodeBuilder nodeId(String nodeId) {
      this.nodeId = requireText("nodeId", nodeId);
      return this;
    }

    public NodeBuilder clearNodeId() {
      this.nodeId = null;
      return this;
    }

    public NodeBuilder heartbeatIntervalSeconds(long heartbeatIntervalSeconds) {
      this.heartbeatIntervalSeconds =
          atLeast("heartbeatIntervalSeconds", heartbeatIntervalSeconds, 1L);
      return this;
    }

    public NodeBuilder orphanGraceSeconds(long orphanGraceSeconds) {
      this.orphanGraceSeconds = atLeast("orphanGraceSeconds", orphanGraceSeconds, 0L);
      return this;
    }

    public NodeBuilder orphanScanIntervalMinutes(long orphanScanIntervalMinutes) {
      this.orphanScanIntervalMinutes =
          atLeast("orphanScanIntervalMinutes", orphanScanIntervalMinutes, 1L);
      return this;
    }

    public NodeBuilder dynamicHeartbeatEnabled(boolean dynamicHeartbeatEnabled) {
      this.dynamicHeartbeatEnabled = dynamicHeartbeatEnabled;
      return this;
    }

    public NodeBuilder requireTags(String... tags) {
      this.requireTags = List.of(tags);
      return this;
    }

    public NodeBuilder excludeTags(String... tags) {
      this.excludeTags = List.of(tags);
      return this;
    }

    private NodeOptions build() {
      return new NodeOptions(
          nodeId,
          heartbeatIntervalSeconds,
          orphanGraceSeconds,
          orphanScanIntervalMinutes,
          dynamicHeartbeatEnabled,
          requireTags,
          excludeTags);
    }
  }

  public static final class RecurringBuilder {
    private int batchLimit = 20;
    private long pollMs = 1000L;
    private long maxPollMs = 60000L;
    private long startupGraceSeconds = 60L;
    private long convergenceWindowSeconds = 0L;

    private RecurringBuilder() {}

    public RecurringBuilder batchLimit(int batchLimit) {
      this.batchLimit = atLeast("batchLimit", batchLimit, 1);
      return this;
    }

    public RecurringBuilder pollMs(long pollMs) {
      this.pollMs = atLeast("pollMs", pollMs, 1L);
      return this;
    }

    public RecurringBuilder maxPollMs(long maxPollMs) {
      this.maxPollMs = atLeast("maxPollMs", maxPollMs, 1L);
      return this;
    }

    public RecurringBuilder startupGraceSeconds(long startupGraceSeconds) {
      this.startupGraceSeconds = atLeast("startupGraceSeconds", startupGraceSeconds, 0L);
      return this;
    }

    public RecurringBuilder convergenceWindowSeconds(long convergenceWindowSeconds) {
      this.convergenceWindowSeconds =
          atLeast("convergenceWindowSeconds", convergenceWindowSeconds, 0L);
      return this;
    }

    private RecurringOptions build() {
      requireNotGreater("pollMs", pollMs, "maxPollMs", maxPollMs);
      return new RecurringOptions(
          batchLimit, pollMs, maxPollMs, startupGraceSeconds, convergenceWindowSeconds);
    }
  }

  public static final class RetryBufferBuilder {
    private long drainIntervalMs = 1000L;

    private RetryBufferBuilder() {}

    public RetryBufferBuilder drainIntervalMs(long drainIntervalMs) {
      this.drainIntervalMs = atLeast("drainIntervalMs", drainIntervalMs, 50L);
      return this;
    }

    private RetryBufferOptions build() {
      return new RetryBufferOptions(drainIntervalMs);
    }
  }

  public static final class TimeoutBuilder {
    private int softTimeoutPercent = 80;
    private long defaultSlaSeconds = 1800L;
    private int signalTimeoutBatchSize = 500;

    private TimeoutBuilder() {}

    public TimeoutBuilder softTimeoutPercent(int softTimeoutPercent) {
      if (softTimeoutPercent <= 0 || softTimeoutPercent >= 100) {
        throw new IllegalArgumentException("softTimeoutPercent must be between 1 and 99");
      }
      this.softTimeoutPercent = softTimeoutPercent;
      return this;
    }

    public TimeoutBuilder defaultSlaSeconds(long defaultSlaSeconds) {
      this.defaultSlaSeconds = atLeast("defaultSlaSeconds", defaultSlaSeconds, 1L);
      return this;
    }

    public TimeoutBuilder signalTimeoutBatchSize(int signalTimeoutBatchSize) {
      this.signalTimeoutBatchSize = atLeast("signalTimeoutBatchSize", signalTimeoutBatchSize, 1);
      return this;
    }

    private TimeoutOptions build() {
      return new TimeoutOptions(softTimeoutPercent, defaultSlaSeconds, signalTimeoutBatchSize);
    }
  }

  public static final class MaintenanceBuilder {
    private boolean dlqPurgeEnabled = true;
    private String dlqPurgeCron = "0 0 2 * * ?";
    private long dlqPurgeDays = 90L;
    private boolean jobArchiveEnabled = true;
    private String jobArchiveCron = "0 0 1 * * ?";
    private long jobRetentionDays = 90L;
    private int jobArchiveBatchSize = 1000;
    private boolean logPurgeEnabled = true;
    private String logPurgeCron = "0 30 2 * * ?";
    private long logRetentionDays = 30L;

    private MaintenanceBuilder() {}

    public MaintenanceBuilder dlqPurgeEnabled(boolean dlqPurgeEnabled) {
      this.dlqPurgeEnabled = dlqPurgeEnabled;
      return this;
    }

    public MaintenanceBuilder dlqPurgeCron(String dlqPurgeCron) {
      this.dlqPurgeCron = requireText("dlqPurgeCron", dlqPurgeCron);
      return this;
    }

    public MaintenanceBuilder dlqPurgeDays(long dlqPurgeDays) {
      this.dlqPurgeDays = atLeast("dlqPurgeDays", dlqPurgeDays, 0L);
      return this;
    }

    public MaintenanceBuilder jobArchiveEnabled(boolean jobArchiveEnabled) {
      this.jobArchiveEnabled = jobArchiveEnabled;
      return this;
    }

    public MaintenanceBuilder jobArchiveCron(String jobArchiveCron) {
      this.jobArchiveCron = requireText("jobArchiveCron", jobArchiveCron);
      return this;
    }

    public MaintenanceBuilder jobRetentionDays(long jobRetentionDays) {
      this.jobRetentionDays = atLeast("jobRetentionDays", jobRetentionDays, 0L);
      return this;
    }

    public MaintenanceBuilder jobArchiveBatchSize(int jobArchiveBatchSize) {
      this.jobArchiveBatchSize = atLeast("jobArchiveBatchSize", jobArchiveBatchSize, 1);
      return this;
    }

    public MaintenanceBuilder logPurgeEnabled(boolean logPurgeEnabled) {
      this.logPurgeEnabled = logPurgeEnabled;
      return this;
    }

    public MaintenanceBuilder logPurgeCron(String logPurgeCron) {
      this.logPurgeCron = requireText("logPurgeCron", logPurgeCron);
      return this;
    }

    public MaintenanceBuilder logRetentionDays(long logRetentionDays) {
      this.logRetentionDays = atLeast("logRetentionDays", logRetentionDays, 0L);
      return this;
    }

    private MaintenanceOptions build() {
      return new MaintenanceOptions(
          dlqPurgeEnabled,
          dlqPurgeCron,
          dlqPurgeDays,
          jobArchiveEnabled,
          jobArchiveCron,
          jobRetentionDays,
          jobArchiveBatchSize,
          logPurgeEnabled,
          logPurgeCron,
          logRetentionDays);
    }
  }

  public static final class NotificationBuilder {
    private boolean enabled = true;
    private String dlqAlertChannel = "#job-scheduler-dlq";
    private String timeoutAlertChannel = "#ops-alerts";

    private NotificationBuilder() {}

    public NotificationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public NotificationBuilder dlqAlertChannel(String dlqAlertChannel) {
      this.dlqAlertChannel = requireText("dlqAlertChannel", dlqAlertChannel);
      return this;
    }

    public NotificationBuilder timeoutAlertChannel(String timeoutAlertChannel) {
      this.timeoutAlertChannel = requireText("timeoutAlertChannel", timeoutAlertChannel);
      return this;
    }

    private NotificationOptions build() {
      return new NotificationOptions(enabled, dlqAlertChannel, timeoutAlertChannel);
    }
  }

  public static final class SchemaBuilder {
    private boolean autoMigrate;
    private String migrationDialect = "";
    private String migrationPrefix = "ddl/migrations";

    private SchemaBuilder() {}

    public SchemaBuilder autoMigrate(boolean autoMigrate) {
      this.autoMigrate = autoMigrate;
      return this;
    }

    public SchemaBuilder migrationDialect(String migrationDialect) {
      this.migrationDialect = migrationDialect == null ? "" : migrationDialect.trim();
      return this;
    }

    public SchemaBuilder migrationPrefix(String migrationPrefix) {
      this.migrationPrefix = requireText("migrationPrefix", migrationPrefix);
      return this;
    }

    private SchemaOptions build() {
      return new SchemaOptions(autoMigrate, migrationDialect, migrationPrefix);
    }
  }

  public static final class PayloadBuilder {
    private int maxPayloadKb = 100;
    private long maxResultBytes = 65536L;

    private PayloadBuilder() {}

    public PayloadBuilder maxPayloadKb(int maxPayloadKb) {
      this.maxPayloadKb = atLeast("maxPayloadKb", maxPayloadKb, 1);
      return this;
    }

    public PayloadBuilder maxResultBytes(long maxResultBytes) {
      this.maxResultBytes = atLeast("maxResultBytes", maxResultBytes, 0L);
      return this;
    }

    private PayloadOptions build() {
      return new PayloadOptions(maxPayloadKb, maxResultBytes);
    }
  }

  public static final class MetricsBuilder {
    private String clustering = "none";

    private MetricsBuilder() {}

    public MetricsBuilder clustering(String clustering) {
      this.clustering = requireText("clustering", clustering).toLowerCase(Locale.ROOT);
      return this;
    }

    private MetricsOptions build() {
      return new MetricsOptions(clustering);
    }
  }

  public static final class SecurityBuilder {
    private boolean allowEmptyClassPolicy;
    private boolean redactEmails = true;
    private boolean maskPayloads;

    private SecurityBuilder() {}

    public SecurityBuilder allowEmptyClassPolicy(boolean allowEmptyClassPolicy) {
      this.allowEmptyClassPolicy = allowEmptyClassPolicy;
      return this;
    }

    public SecurityBuilder redactEmails(boolean redactEmails) {
      this.redactEmails = redactEmails;
      return this;
    }

    /**
     * Masks sensitive fields in structured payloads returned from a read API — the {@code params}
     * map and trace context on a job detail, plus the serialized job result — when {@code true};
     * {@code false} (default) leaves them unmasked. Map masking is key-based and result masking
     * walks the serialized JSON; free-text fields such as {@code lastError} are not masked. The
     * durable store payload is never affected.
     */
    public SecurityBuilder maskPayloads(boolean maskPayloads) {
      this.maskPayloads = maskPayloads;
      return this;
    }

    private SecurityOptions build() {
      return new SecurityOptions(allowEmptyClassPolicy, redactEmails, maskPayloads);
    }
  }

  public static final class StoreBuilder {
    private IsolationCheckMode isolationCheckMode = IsolationCheckMode.FAIL;
    private int priorityBoostIntervalMinutes = 15;

    private StoreBuilder() {}

    public StoreBuilder isolationCheckMode(IsolationCheckMode isolationCheckMode) {
      this.isolationCheckMode =
          Objects.requireNonNull(isolationCheckMode, "isolationCheckMode must not be null");
      return this;
    }

    public StoreBuilder priorityBoostIntervalMinutes(int priorityBoostIntervalMinutes) {
      this.priorityBoostIntervalMinutes =
          atLeast("priorityBoostIntervalMinutes", priorityBoostIntervalMinutes, 0);
      return this;
    }

    private StoreOptions build() {
      return new StoreOptions(isolationCheckMode, priorityBoostIntervalMinutes);
    }
  }

  @Incubating
  public static final class CircuitBreakerBuilder {
    private final Map<CircuitBreakerProfile, CircuitBreakerProfileBuilder> profiles =
        defaultCircuitBreakerProfiles();
    private boolean enabled = true;

    private CircuitBreakerBuilder() {}

    public CircuitBreakerBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public CircuitBreakerBuilder profile(
        CircuitBreakerProfile profile, Consumer<CircuitBreakerProfileBuilder> customizer) {
      CircuitBreakerProfileBuilder builder =
          profiles.computeIfAbsent(
              Objects.requireNonNull(profile, "profile must not be null"),
              ignored -> new CircuitBreakerProfileBuilder());
      customizer.accept(builder);
      return this;
    }

    private CircuitBreakerOptions build() {
      Map<CircuitBreakerProfile, CircuitBreakerProfileOptions> built =
          new EnumMap<>(CircuitBreakerProfile.class);
      for (Map.Entry<CircuitBreakerProfile, CircuitBreakerProfileBuilder> entry :
          profiles.entrySet()) {
        built.put(entry.getKey(), entry.getValue().build());
      }
      return new CircuitBreakerOptions(enabled, built);
    }
  }

  @Incubating
  public static final class CircuitBreakerProfileBuilder {
    private float failureRateThreshold = 50.0f;
    private int slidingWindowSize = 100;
    private long waitDurationMs = 30000L;
    private int permittedCallsInHalfOpen = 3;
    private int minimumCalls = 5;

    private CircuitBreakerProfileBuilder() {}

    public CircuitBreakerProfileBuilder failureRateThreshold(float failureRateThreshold) {
      if (!Float.isFinite(failureRateThreshold)
          || failureRateThreshold < 0.0f
          || failureRateThreshold > 100.0f) {
        throw new IllegalArgumentException(
            "failureRateThreshold must be a finite value between 0 and 100");
      }
      this.failureRateThreshold = failureRateThreshold;
      return this;
    }

    public CircuitBreakerProfileBuilder slidingWindowSize(int slidingWindowSize) {
      this.slidingWindowSize = atLeast("slidingWindowSize", slidingWindowSize, 1);
      return this;
    }

    public CircuitBreakerProfileBuilder waitDurationMs(long waitDurationMs) {
      this.waitDurationMs = atLeast("waitDurationMs", waitDurationMs, 0L);
      return this;
    }

    public CircuitBreakerProfileBuilder permittedCallsInHalfOpen(int permittedCallsInHalfOpen) {
      this.permittedCallsInHalfOpen =
          atLeast("permittedCallsInHalfOpen", permittedCallsInHalfOpen, 1);
      return this;
    }

    public CircuitBreakerProfileBuilder minimumCalls(int minimumCalls) {
      this.minimumCalls = atLeast("minimumCalls", minimumCalls, 1);
      return this;
    }

    private CircuitBreakerProfileOptions build() {
      return new CircuitBreakerProfileOptions(
          failureRateThreshold,
          slidingWindowSize,
          waitDurationMs,
          permittedCallsInHalfOpen,
          minimumCalls);
    }
  }
}

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

  private static long atLeast(String name, long value, long minInclusive) {
    if (value < minInclusive) {
      throw new IllegalArgumentException(name + " must be at least " + minInclusive);
    }
    return value;
  }

  public PollingOptions polling() {
    return polling;
  }

  public ExecutionOptions execution() {
    return execution;
  }

  public NodeOptions node() {
    return node;
  }

  public RecurringOptions recurring() {
    return recurring;
  }

  public RetryBufferOptions retryBuffer() {
    return retryBuffer;
  }

  public TimeoutOptions timeout() {
    return timeout;
  }

  public MaintenanceOptions maintenance() {
    return maintenance;
  }

  public NotificationOptions notifications() {
    return notifications;
  }

  public SchemaOptions schema() {
    return schema;
  }

  public PayloadOptions payload() {
    return payload;
  }

  public MetricsOptions metrics() {
    return metrics;
  }

  public SecurityOptions security() {
    return security;
  }

  public StoreOptions store() {
    return store;
  }

  public CircuitBreakerOptions circuitBreaker() {
    return circuitBreaker;
  }

  public enum IsolationCheckMode {
    WARN,
    FAIL,
    DISABLE
  }

  public record PollingOptions(
      int batchSize,
      long burstDelayMs,
      long minDelayMs,
      long maxDelayMs,
      long deepIdleDelayMs,
      long deepIdleThresholdMs,
      int idleThreshold,
      int claimHeadroomFactor) {}

  public record ExecutionOptions(
      boolean useVirtualThreads,
      int queueSize,
      Map<String, Integer> maxConcurrency,
      Map<String, Integer> virtualThreadLimits,
      Map<String, Integer> rateLimitsPerMinute) {

    public ExecutionOptions {
      maxConcurrency = Map.copyOf(maxConcurrency);
      virtualThreadLimits = Map.copyOf(virtualThreadLimits);
      rateLimitsPerMinute = Map.copyOf(rateLimitsPerMinute);
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

  public record RecurringOptions(
      int batchLimit,
      long pollMs,
      long maxPollMs,
      long startupGraceSeconds,
      long convergenceWindowSeconds) {}

  public record RetryBufferOptions(long drainIntervalMs) {}

  public record TimeoutOptions(
      int softTimeoutPercent, long defaultSlaSeconds, int signalTimeoutBatchSize) {}

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

  public record NotificationOptions(
      boolean enabled, String dlqAlertChannel, String timeoutAlertChannel) {}

  public record SchemaOptions(
      boolean autoMigrate, String migrationDialect, String migrationPrefix) {}

  public record PayloadOptions(int maxPayloadKb, long maxResultBytes) {}

  public record MetricsOptions(String clustering) {}

  public record SecurityOptions(boolean allowEmptyClassPolicy, boolean redactEmails) {}

  public record StoreOptions(
      IsolationCheckMode isolationCheckMode, int priorityBoostIntervalMinutes) {}

  public record CircuitBreakerOptions(
      boolean enabled, Map<CircuitBreakerProfile, CircuitBreakerProfileOptions> profiles) {

    public CircuitBreakerOptions {
      profiles = Map.copyOf(profiles);
    }

    public CircuitBreakerProfileOptions profile(CircuitBreakerProfile profile) {
      return profiles.get(profile);
    }
  }

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
    private boolean useVirtualThreads;
    private int queueSize = 100;

    private ExecutionBuilder() {}

    public ExecutionBuilder useVirtualThreads(boolean useVirtualThreads) {
      this.useVirtualThreads = useVirtualThreads;
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
          useVirtualThreads, queueSize, maxConcurrency, virtualThreadLimits, rateLimitsPerMinute);
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

    private SecurityBuilder() {}

    public SecurityBuilder allowEmptyClassPolicy(boolean allowEmptyClassPolicy) {
      this.allowEmptyClassPolicy = allowEmptyClassPolicy;
      return this;
    }

    public SecurityBuilder redactEmails(boolean redactEmails) {
      this.redactEmails = redactEmails;
      return this;
    }

    private SecurityOptions build() {
      return new SecurityOptions(allowEmptyClassPolicy, redactEmails);
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

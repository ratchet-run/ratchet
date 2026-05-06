package run.ratchet.api;

import java.util.ArrayList;
import java.util.List;
import run.ratchet.spi.RatchetConfigKey;
import run.ratchet.spi.RatchetConfigSource;

/** Builds immutable {@link RatchetOptions} from typed configuration keys. */
public final class RatchetOptionsFactory {

  private RatchetOptionsFactory() {}

  static RatchetOptions from(DefaultRatchetConfig config) {
    return RatchetOptions.builder()
        .polling(
            polling ->
                polling
                    .batchSize(config.get(RatchetConfigKeys.POLLER_BATCH_SIZE))
                    .burstDelayMs(config.get(RatchetConfigKeys.POLLER_BURST_DELAY_MS))
                    .minDelayMs(config.get(RatchetConfigKeys.POLLER_MIN_DELAY_MS))
                    .maxDelayMs(config.get(RatchetConfigKeys.POLLER_MAX_DELAY_MS))
                    .deepIdleDelayMs(config.get(RatchetConfigKeys.POLLER_DEEP_IDLE_DELAY_MS))
                    .deepIdleThresholdMs(
                        config.get(RatchetConfigKeys.POLLER_DEEP_IDLE_THRESHOLD_MS))
                    .idleThreshold(config.get(RatchetConfigKeys.POLLER_IDLE_THRESHOLD))
                    .claimHeadroomFactor(
                        config.get(RatchetConfigKeys.POLLER_CLAIM_HEADROOM_FACTOR)))
        .execution(execution -> configureExecution(config, execution))
        .node(
            node -> {
              config
                  .raw(RatchetConfigKeys.NODE_ID)
                  .filter(raw -> !raw.isBlank())
                  .ifPresent(node::nodeId);
              node.heartbeatIntervalSeconds(
                      config.get(RatchetConfigKeys.NODE_HEARTBEAT_INTERVAL_SECONDS))
                  .orphanGraceSeconds(config.get(RatchetConfigKeys.NODE_ORPHAN_GRACE_SECONDS))
                  .orphanScanIntervalMinutes(
                      config.get(RatchetConfigKeys.ORPHAN_SCAN_INTERVAL_MINUTES))
                  .dynamicHeartbeatEnabled(config.get(RatchetConfigKeys.DYNAMIC_HEARTBEAT_ENABLED));
            })
        .recurring(
            recurring ->
                recurring
                    .batchLimit(config.get(RatchetConfigKeys.RECURRING_BATCH_LIMIT))
                    .pollMs(config.get(RatchetConfigKeys.RECURRING_POLL_MS))
                    .maxPollMs(config.get(RatchetConfigKeys.RECURRING_MAX_POLL_MS))
                    .startupGraceSeconds(
                        config.get(RatchetConfigKeys.RECURRING_STARTUP_GRACE_SECONDS))
                    .convergenceWindowSeconds(
                        config.get(RatchetConfigKeys.RECURRING_CONVERGENCE_WINDOW_SECONDS)))
        .retryBuffer(
            retryBuffer ->
                retryBuffer.drainIntervalMs(
                    config.get(RatchetConfigKeys.RETRY_BUFFER_DRAIN_INTERVAL_MS)))
        .timeout(
            timeout ->
                timeout
                    .softTimeoutPercent(config.get(RatchetConfigKeys.SOFT_TIMEOUT_PERCENT))
                    .defaultSlaSeconds(config.get(RatchetConfigKeys.WORKER_DEFAULT_SLA))
                    .signalTimeoutBatchSize(
                        config.get(RatchetConfigKeys.SIGNAL_TIMEOUT_BATCH_SIZE)))
        .maintenance(
            maintenance ->
                maintenance
                    .dlqPurgeEnabled(config.get(RatchetConfigKeys.DLQ_PURGE_ENABLED))
                    .dlqPurgeCron(config.get(RatchetConfigKeys.DLQ_PURGE_CRON))
                    .dlqPurgeDays(config.get(RatchetConfigKeys.DLQ_PURGE_DAYS))
                    .jobArchiveEnabled(config.get(RatchetConfigKeys.JOB_ARCHIVE_ENABLED))
                    .jobArchiveCron(config.get(RatchetConfigKeys.JOB_ARCHIVE_CRON))
                    .jobRetentionDays(config.get(RatchetConfigKeys.JOB_RETENTION_DAYS))
                    .jobArchiveBatchSize(config.get(RatchetConfigKeys.JOB_ARCHIVE_BATCH_SIZE))
                    .logPurgeEnabled(config.get(RatchetConfigKeys.LOG_PURGE_ENABLED))
                    .logPurgeCron(config.get(RatchetConfigKeys.LOG_PURGE_CRON))
                    .logRetentionDays(config.get(RatchetConfigKeys.LOG_RETENTION_DAYS)))
        .notifications(
            notifications ->
                notifications
                    .slackNotificationsEnabled(
                        config.get(RatchetConfigKeys.SLACK_NOTIFICATIONS_ENABLED))
                    .slackDlqChannel(config.get(RatchetConfigKeys.SLACK_DLQ_CHANNEL))
                    .slackTimeoutChannel(config.get(RatchetConfigKeys.SLACK_TIMEOUT_CHANNEL)))
        .schema(
            schema ->
                schema
                    .autoMigrate(config.get(RatchetConfigKeys.SCHEMA_AUTO_MIGRATE))
                    .migrationDialect(config.get(RatchetConfigKeys.SCHEMA_MIGRATION_DIALECT))
                    .migrationPrefix(config.get(RatchetConfigKeys.SCHEMA_MIGRATION_PREFIX)))
        .payload(
            payload ->
                payload
                    .maxPayloadKb(config.get(RatchetConfigKeys.MAX_PAYLOAD_KB))
                    .maxResultBytes(config.get(RatchetConfigKeys.MAX_RESULT_BYTES)))
        .metrics(metrics -> metrics.clustering(config.get(RatchetConfigKeys.METRICS_CLUSTERING)))
        .security(
            security ->
                security
                    .allowEmptyClassPolicy(config.get(RatchetConfigKeys.ALLOW_EMPTY_CLASS_POLICY))
                    .redactEmails(config.get(RatchetConfigKeys.REDACT_EMAILS)))
        .store(
            store ->
                store
                    .isolationCheckMode(config.get(RatchetConfigKeys.ISOLATION_CHECK_MODE))
                    .priorityBoostIntervalMinutes(
                        config.get(RatchetConfigKeys.PRIORITY_BOOST_INTERVAL_MINUTES)))
        .circuitBreaker(
            circuitBreaker -> {
              circuitBreaker.enabled(config.get(RatchetConfigKeys.CIRCUIT_BREAKER_ENABLED));
              for (CircuitBreakerProfile profile : CircuitBreakerProfile.values()) {
                configureCircuitBreakerProfile(config, circuitBreaker, profile);
              }
            })
        .build();
  }

  /**
   * Builds {@link RatchetOptions} by reading the ambient configuration chain (MicroProfile Config
   * when present, then environment variables), optionally overlaid with caller-supplied {@link
   * RatchetConfigSource} instances.
   *
   * <p>Intended for use inside a CDI producer method:
   *
   * <pre>{@code
   * @Produces
   * @ApplicationScoped
   * public RatchetOptions ratchetOptions() {
   *   return RatchetOptionsFactory.fromEnvironment();
   * }
   * }</pre>
   *
   * <p>Calling with no arguments reads exclusively from MicroProfile Config and environment
   * variables, applying compiled-in defaults for keys absent from those sources. Caller-supplied
   * sources take precedence over the ambient chain.
   *
   * <p>The surrounding producer method should be {@code @ApplicationScoped} so sources are read
   * once at bootstrap rather than on every injection.
   *
   * @param additional optional overlay sources consulted before MicroProfile Config / env vars
   * @return the fully-populated, immutable {@link RatchetOptions}
   */
  public static RatchetOptions fromEnvironment(RatchetConfigSource... additional) {
    List<RatchetConfigSource> sources = new ArrayList<>();
    for (RatchetConfigSource source : additional) {
      if (source != null) {
        sources.add(source);
      }
    }
    MicroProfileRatchetConfigSource.create().ifPresent(sources::add);
    sources.add(new EnvironmentRatchetConfigSource());
    return from(new DefaultRatchetConfig(sources));
  }

  private static void configureExecution(
      DefaultRatchetConfig config, RatchetOptions.ExecutionBuilder execution) {
    execution
        .useVirtualThreads(config.get(RatchetConfigKeys.WORKER_USE_VIRTUAL_THREADS))
        .queueSize(config.get(RatchetConfigKeys.THREAD_POOL_QUEUE_SIZE))
        .maxConcurrency("SINGLE", config.get(RatchetConfigKeys.THREAD_POOL_SIZE_SINGLE))
        .maxConcurrency("RECURRING", config.get(RatchetConfigKeys.THREAD_POOL_SIZE_RECURRING))
        .maxConcurrency("BATCH_CHILD", config.get(RatchetConfigKeys.THREAD_POOL_SIZE_BATCH_CHILD))
        .maxConcurrency("BATCH_PARENT", config.get(RatchetConfigKeys.THREAD_POOL_SIZE_BATCH_PARENT))
        .maxConcurrency("CHAIN_STEP", config.get(RatchetConfigKeys.THREAD_POOL_SIZE_CHAIN))
        .maxConcurrency("DLQ_ALERT", config.get(RatchetConfigKeys.THREAD_POOL_SIZE_DLQ_ALERT))
        .maxConcurrency(
            "WORKFLOW_BRANCH", config.get(RatchetConfigKeys.THREAD_POOL_SIZE_WORKFLOW_BRANCH))
        .maxConcurrency(
            "WORKFLOW_JOIN", config.get(RatchetConfigKeys.THREAD_POOL_SIZE_WORKFLOW_JOIN));

    for (String type :
        new String[] {
          "SINGLE",
          "RECURRING",
          "BATCH_CHILD",
          "BATCH_PARENT",
          "CHAIN_STEP",
          "DLQ_ALERT",
          "WORKFLOW_BRANCH",
          "WORKFLOW_JOIN"
        }) {
      int virtualThreadLimit = config.get(RatchetConfigKeys.virtualThreadLimit(type));
      if (virtualThreadLimit > 0) {
        execution.virtualThreadLimit(type, virtualThreadLimit);
      }
      int rateLimit = config.get(RatchetConfigKeys.rateLimitPerMinute(type));
      if (rateLimit > 0) {
        execution.rateLimitPerMinute(type, rateLimit);
      }
    }
  }

  private static void configureCircuitBreakerProfile(
      DefaultRatchetConfig config,
      RatchetOptions.CircuitBreakerBuilder circuitBreaker,
      CircuitBreakerProfile profile) {
    RatchetOptions.CircuitBreakerProfileOptions defaults =
        RatchetOptions.defaults().circuitBreaker().profile(profile);
    String profileName = profile.name();
    circuitBreaker.profile(
        profile,
        builder ->
            builder
                .failureRateThreshold(
                    config.get(
                        RatchetConfigKeys.circuitBreakerFailureRate(
                            profileName, defaults.failureRateThreshold())))
                .slidingWindowSize(
                    config.get(
                        RatchetConfigKeys.circuitBreakerWindowSize(
                            profileName, defaults.slidingWindowSize())))
                .waitDurationMs(waitDurationMs(config, profileName, defaults.waitDurationMs()))
                .slowCallThresholdMs(
                    config.get(
                        RatchetConfigKeys.circuitBreakerSlowCallMs(
                            profileName, defaults.slowCallThresholdMs())))
                .permittedCallsInHalfOpen(
                    config.get(
                        RatchetConfigKeys.circuitBreakerHalfOpenCalls(
                            profileName, defaults.permittedCallsInHalfOpen())))
                .minimumCalls(
                    config.get(
                        RatchetConfigKeys.circuitBreakerMinimumCalls(
                            profileName, defaults.minimumCalls()))));
  }

  private static long waitDurationMs(
      DefaultRatchetConfig config, String profileName, long defaultValue) {
    RatchetConfigKey<Long> waitMs =
        RatchetConfigKeys.circuitBreakerWaitMs(profileName, defaultValue);
    return config.get(waitMs);
  }
}

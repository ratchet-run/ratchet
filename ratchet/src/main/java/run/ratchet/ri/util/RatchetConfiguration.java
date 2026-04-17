package run.ratchet.ri.util;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import org.jboss.logging.Logger;

/**
 * CDI-injectable configuration bean for the Ratchet job scheduler. Reads configuration values from
 * environment variables (checked first) and system properties (checked second), applying default
 * values where neither is set or the value is invalid.
 *
 * <p>This bean replaces the former static {@code SchedulerConfig} utility class with an injectable,
 * testable configuration source.
 */
@ApplicationScoped
public class RatchetConfiguration {

  private static final Logger log = Logger.getLogger(RatchetConfiguration.class);

  /**
   * Env var pairs (preferred, legacy) for every numeric configuration value. Used by {@link
   * #validateNumericEnvVars()} to emit a startup WARN for any env var that is present but does not
   * parse as a non-negative integer. Keep in sync with every {@code parseIntOrDefault} / {@code
   * parseLongOrDefault} getter.
   */
  private static final List<String[]> NUMERIC_ENV_VARS =
      List.of(
          new String[] {"RATCHET_CB_DEFAULT_FAILURE_RATE", "SCHEDULER_CB_DEFAULT_FAILURE_RATE"},
          new String[] {"RATCHET_CB_DEFAULT_WAIT_SECONDS", "SCHEDULER_CB_DEFAULT_WAIT_SECONDS"},
          new String[] {"RATCHET_CB_DEFAULT_WINDOW_SIZE", "SCHEDULER_CB_DEFAULT_WINDOW_SIZE"},
          new String[] {"RATCHET_CB_EXTERNAL_FAILURE_RATE", "SCHEDULER_CB_EXTERNAL_FAILURE_RATE"},
          new String[] {"RATCHET_CB_EXTERNAL_WAIT_SECONDS", "SCHEDULER_CB_EXTERNAL_WAIT_SECONDS"},
          new String[] {
            "RATCHET_IDEMPOTENCY_RETRY_MAX_ATTEMPTS", "SCHEDULER_IDEMPOTENCY_RETRY_MAX_ATTEMPTS"
          },
          new String[] {
            "RATCHET_IDEMPOTENCY_RETRY_INITIAL_DELAY_MS",
            "SCHEDULER_IDEMPOTENCY_RETRY_INITIAL_DELAY_MS"
          },
          new String[] {
            "RATCHET_IDEMPOTENCY_RETRY_MAX_DELAY_MS", "SCHEDULER_IDEMPOTENCY_RETRY_MAX_DELAY_MS"
          },
          new String[] {"RATCHET_DLQ_PURGE_DAYS", "DLQ_PURGE_DAYS"},
          new String[] {"RATCHET_JOB_ARCHIVE_BATCH_SIZE", "SCHEDULER_JOB_ARCHIVE_BATCH_SIZE"},
          new String[] {"RATCHET_JOB_RETENTION_DAYS", "SCHEDULER_JOB_RETENTION_DAYS"},
          new String[] {"RATCHET_LOG_RETENTION_DAYS", "LOG_RETENTION_DAYS"},
          new String[] {"RATCHET_MAX_PAYLOAD_KB", "SCHEDULER_MAX_PAYLOAD_KB"},
          new String[] {
            "RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS", "NODE_HEARTBEAT_INTERVAL_SECONDS"
          },
          new String[] {"RATCHET_NODE_ORPHAN_GRACE_SECONDS", "NODE_ORPHAN_GRACE_SECONDS"},
          new String[] {
            "RATCHET_ORPHAN_SCAN_INTERVAL_MINUTES", "SCHEDULER_ORPHAN_SCAN_INTERVAL_MINUTES"
          },
          new String[] {
            "RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES", "SCHEDULER_PRIORITY_BOOST_INTERVAL_MINUTES"
          },
          new String[] {"RATCHET_POLLER_BATCH_SIZE", "POLLER_BATCH_SIZE"},
          new String[] {"RATCHET_POLLER_BURST_DELAY_MS", "POLLER_BURST_DELAY_MS"},
          new String[] {"RATCHET_POLLER_DEEP_IDLE_DELAY_MS", "POLLER_DEEP_IDLE_DELAY_MS"},
          new String[] {"RATCHET_POLLER_DEEP_IDLE_THRESHOLD_MS", "POLLER_DEEP_IDLE_THRESHOLD_MS"},
          new String[] {"RATCHET_POLLER_IDLE_THRESHOLD", "POLLER_IDLE_THRESHOLD"},
          new String[] {"RATCHET_POLLER_MAX_DELAY_MS", "POLLER_MAX_DELAY_MS"},
          new String[] {"RATCHET_POLLER_MIN_DELAY_MS", "POLLER_MIN_DELAY_MS"},
          new String[] {
            "RATCHET_POLLER_CLAIM_HEADROOM_FACTOR", "SCHEDULER_POLLER_CLAIM_HEADROOM_FACTOR"
          },
          new String[] {
            "RATCHET_RETRY_BUFFER_DRAIN_INTERVAL_MS", "SCHEDULER_RETRY_BUFFER_DRAIN_INTERVAL_MS"
          },
          new String[] {"RATCHET_RECURRING_BATCH_LIMIT", "RECURRING_BATCH_LIMIT"},
          new String[] {"RATCHET_RECURRING_MAX_POLL_MS", "RECURRING_MAX_POLL_MS"},
          new String[] {"RATCHET_RECURRING_POLL_MS", "RECURRING_POLL_MS"},
          new String[] {"RATCHET_SOFT_TIMEOUT_PERCENT", "SCHEDULER_SOFT_TIMEOUT_PERCENT"},
          new String[] {"RATCHET_THREAD_POOL_QUEUE_SIZE", "SCHEDULER_THREAD_POOL_QUEUE_SIZE"},
          new String[] {
            "RATCHET_THREAD_POOL_SIZE_BATCH_CHILD", "SCHEDULER_THREAD_POOL_SIZE_BATCH_CHILD"
          },
          new String[] {
            "RATCHET_THREAD_POOL_SIZE_BATCH_PARENT", "SCHEDULER_THREAD_POOL_SIZE_BATCH_PARENT"
          },
          new String[] {"RATCHET_THREAD_POOL_SIZE_CHAIN", "SCHEDULER_THREAD_POOL_SIZE_CHAIN"},
          new String[] {"RATCHET_THREAD_POOL_SIZE_DEFAULT", "SCHEDULER_THREAD_POOL_SIZE_DEFAULT"},
          new String[] {"RATCHET_THREAD_POOL_SIZE_DLQ", "SCHEDULER_THREAD_POOL_SIZE_DLQ"},
          new String[] {
            "RATCHET_THREAD_POOL_SIZE_RECURRING", "SCHEDULER_THREAD_POOL_SIZE_RECURRING"
          },
          new String[] {"RATCHET_THREAD_POOL_SIZE_SINGLE", "SCHEDULER_THREAD_POOL_SIZE_SINGLE"},
          new String[] {"RATCHET_WORKER_DEFAULT_SLA", "WORKER_DEFAULT_SLA"});

  private final String cbDefaultFailureRate;
  private final String cbDefaultWaitSeconds;
  private final String cbDefaultWindowSize;
  private final String cbExternalFailureRate;
  private final String cbExternalWaitSeconds;
  private final String circuitBreakerEnabled;

  private final String idempotencyRetryMaxAttempts;
  private final String idempotencyRetryInitialDelayMs;
  private final String idempotencyRetryMaxDelayMs;

  private final String dlqPurgeCron;
  private final String dlqPurgeDays;
  private final String dlqPurgeEnabled;
  private final String dynamicHeartbeatEnabled;
  private final String jobArchiverCron;
  private final String jobArchiveBatchSize;
  private final String jobArchiveEnabled;
  private final String jobRetentionDays;
  private final String logPurgeEnabled;
  private final String logPurgerCron;
  private final String logRetentionDays;
  private final String maxPayloadKb;
  private final String metricsClustering;

  private final String nodeHeartbeatIntervalSeconds;
  private final String nodeOrphanGraceSeconds;
  private final String orphanScanIntervalMinutes;

  private final String priorityBoostIntervalMinutes;

  private final String pollerBatchSize;
  private final String pollerBurstDelayMs;
  private final String pollerDeepIdleDelayMs;
  private final String pollerDeepIdleThresholdMs;
  private final String pollerIdleThreshold;
  private final String pollerMaxDelayMs;
  private final String pollerMinDelayMs;
  private final String pollerClaimHeadroomFactor;
  private final String retryBufferDrainIntervalMs;

  private final String recurringBatchLimit;
  private final String recurringMaxPollMs;
  private final String recurringPollMs;

  private final String slackDlqChannel;
  private final String slackNotificationsEnabled;
  private final String slackTimeoutChannel;
  private final String softTimeoutPercent;

  private final String threadPoolQueueSize;
  private final String threadPoolSizeBatchChild;
  private final String threadPoolSizeBatchParent;
  private final String threadPoolSizeChain;
  private final String threadPoolSizeDefault;
  private final String threadPoolSizeDlq;
  private final String threadPoolSizeRecurring;
  private final String threadPoolSizeSingle;

  private final String workerDefaultSla;
  private final String workerUseVirtualThreads;

  public RatchetConfiguration() {
    this.cbDefaultFailureRate =
        getEnvWithFallback(
            "RATCHET_CB_DEFAULT_FAILURE_RATE", "SCHEDULER_CB_DEFAULT_FAILURE_RATE", "50");
    this.cbDefaultWaitSeconds =
        getEnvWithFallback(
            "RATCHET_CB_DEFAULT_WAIT_SECONDS", "SCHEDULER_CB_DEFAULT_WAIT_SECONDS", "30");
    this.cbDefaultWindowSize =
        getEnvWithFallback(
            "RATCHET_CB_DEFAULT_WINDOW_SIZE", "SCHEDULER_CB_DEFAULT_WINDOW_SIZE", "100");
    this.cbExternalFailureRate =
        getEnvWithFallback(
            "RATCHET_CB_EXTERNAL_FAILURE_RATE", "SCHEDULER_CB_EXTERNAL_FAILURE_RATE", "60");
    this.cbExternalWaitSeconds =
        getEnvWithFallback(
            "RATCHET_CB_EXTERNAL_WAIT_SECONDS", "SCHEDULER_CB_EXTERNAL_WAIT_SECONDS", "60");
    this.circuitBreakerEnabled =
        getEnvWithFallback(
            "RATCHET_CIRCUIT_BREAKER_ENABLED", "SCHEDULER_CIRCUIT_BREAKER_ENABLED", "true");

    this.idempotencyRetryMaxAttempts =
        getEnvWithFallback(
            "RATCHET_IDEMPOTENCY_RETRY_MAX_ATTEMPTS",
            "SCHEDULER_IDEMPOTENCY_RETRY_MAX_ATTEMPTS",
            "3");
    this.idempotencyRetryInitialDelayMs =
        getEnvWithFallback(
            "RATCHET_IDEMPOTENCY_RETRY_INITIAL_DELAY_MS",
            "SCHEDULER_IDEMPOTENCY_RETRY_INITIAL_DELAY_MS",
            "50");
    this.idempotencyRetryMaxDelayMs =
        getEnvWithFallback(
            "RATCHET_IDEMPOTENCY_RETRY_MAX_DELAY_MS",
            "SCHEDULER_IDEMPOTENCY_RETRY_MAX_DELAY_MS",
            "500");

    this.dlqPurgeCron =
        getEnvWithFallback("RATCHET_DLQ_PURGE_CRON", "SCHEDULER_DLQ_PURGE_CRON", "0 0 2 * * ?");
    this.dlqPurgeDays = getEnvWithFallback("RATCHET_DLQ_PURGE_DAYS", "DLQ_PURGE_DAYS", "90");
    this.dlqPurgeEnabled =
        getEnvWithFallback("RATCHET_DLQ_PURGE_ENABLED", "SCHEDULER_DLQ_PURGE_ENABLED", "true");
    this.dynamicHeartbeatEnabled =
        getEnvWithFallback(
            "RATCHET_DYNAMIC_HEARTBEAT_ENABLED", "SCHEDULER_DYNAMIC_HEARTBEAT_ENABLED", "true");
    this.jobArchiverCron =
        getEnvWithFallback(
            "RATCHET_JOB_ARCHIVER_CRON", "SCHEDULER_JOB_ARCHIVER_CRON", "0 0 1 * * ?");
    this.jobArchiveBatchSize =
        getEnvWithFallback(
            "RATCHET_JOB_ARCHIVE_BATCH_SIZE", "SCHEDULER_JOB_ARCHIVE_BATCH_SIZE", "1000");
    this.jobArchiveEnabled =
        getEnvWithFallback("RATCHET_JOB_ARCHIVE_ENABLED", "SCHEDULER_JOB_ARCHIVE_ENABLED", "true");
    this.jobRetentionDays =
        getEnvWithFallback("RATCHET_JOB_RETENTION_DAYS", "SCHEDULER_JOB_RETENTION_DAYS", "90");
    this.logPurgeEnabled =
        getEnvWithFallback("RATCHET_LOG_PURGE_ENABLED", "SCHEDULER_LOG_PURGE_ENABLED", "true");
    this.logPurgerCron =
        getEnvWithFallback("RATCHET_LOG_PURGER_CRON", "LOG_PURGER_CRON", "0 30 2 * * ?");
    this.logRetentionDays =
        getEnvWithFallback("RATCHET_LOG_RETENTION_DAYS", "LOG_RETENTION_DAYS", "30");
    this.maxPayloadKb =
        getEnvWithFallback("RATCHET_MAX_PAYLOAD_KB", "SCHEDULER_MAX_PAYLOAD_KB", "100");
    this.metricsClustering =
        getEnvWithFallback("RATCHET_METRICS_CLUSTERING", "SCHEDULER_METRICS_CLUSTERING", "none");

    this.nodeHeartbeatIntervalSeconds =
        getEnvWithFallback(
            "RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS", "NODE_HEARTBEAT_INTERVAL_SECONDS", "10");
    this.nodeOrphanGraceSeconds =
        getEnvWithFallback("RATCHET_NODE_ORPHAN_GRACE_SECONDS", "NODE_ORPHAN_GRACE_SECONDS", "60");
    this.orphanScanIntervalMinutes =
        getEnvWithFallback(
            "RATCHET_ORPHAN_SCAN_INTERVAL_MINUTES", "SCHEDULER_ORPHAN_SCAN_INTERVAL_MINUTES", "5");

    this.priorityBoostIntervalMinutes =
        getEnvWithFallback(
            "RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES",
            "SCHEDULER_PRIORITY_BOOST_INTERVAL_MINUTES",
            "15");

    this.pollerBatchSize =
        getEnvWithFallback("RATCHET_POLLER_BATCH_SIZE", "POLLER_BATCH_SIZE", "50");
    this.pollerBurstDelayMs =
        getEnvWithFallback("RATCHET_POLLER_BURST_DELAY_MS", "POLLER_BURST_DELAY_MS", "500");
    this.pollerDeepIdleDelayMs =
        getEnvWithFallback(
            "RATCHET_POLLER_DEEP_IDLE_DELAY_MS", "POLLER_DEEP_IDLE_DELAY_MS", "30000");
    this.pollerDeepIdleThresholdMs =
        getEnvWithFallback(
            "RATCHET_POLLER_DEEP_IDLE_THRESHOLD_MS", "POLLER_DEEP_IDLE_THRESHOLD_MS", "60000");
    this.pollerIdleThreshold =
        getEnvWithFallback("RATCHET_POLLER_IDLE_THRESHOLD", "POLLER_IDLE_THRESHOLD", "3");
    this.pollerMaxDelayMs =
        getEnvWithFallback("RATCHET_POLLER_MAX_DELAY_MS", "POLLER_MAX_DELAY_MS", "10000");
    this.pollerMinDelayMs =
        getEnvWithFallback("RATCHET_POLLER_MIN_DELAY_MS", "POLLER_MIN_DELAY_MS", "2000");
    this.pollerClaimHeadroomFactor =
        getEnvWithFallback(
            "RATCHET_POLLER_CLAIM_HEADROOM_FACTOR",
            "SCHEDULER_POLLER_CLAIM_HEADROOM_FACTOR",
            "0");
    this.retryBufferDrainIntervalMs =
        getEnvWithFallback(
            "RATCHET_RETRY_BUFFER_DRAIN_INTERVAL_MS",
            "SCHEDULER_RETRY_BUFFER_DRAIN_INTERVAL_MS",
            "1000");

    this.recurringBatchLimit =
        getEnvWithFallback("RATCHET_RECURRING_BATCH_LIMIT", "RECURRING_BATCH_LIMIT", "20");
    this.recurringMaxPollMs =
        getEnvWithFallback("RATCHET_RECURRING_MAX_POLL_MS", "RECURRING_MAX_POLL_MS", "60000");
    this.recurringPollMs =
        getEnvWithFallback("RATCHET_RECURRING_POLL_MS", "RECURRING_POLL_MS", "1000");

    this.slackDlqChannel =
        getEnvWithFallback(
            "RATCHET_SLACK_DLQ_CHANNEL", "SCHEDULER_SLACK_DLQ_CHANNEL", "#job-scheduler-dlq");
    this.slackNotificationsEnabled =
        getEnvWithFallback(
            "RATCHET_SLACK_NOTIFICATIONS_ENABLED", "SCHEDULER_SLACK_NOTIFICATIONS_ENABLED", "true");
    this.slackTimeoutChannel =
        getEnvWithFallback(
            "RATCHET_SLACK_TIMEOUT_CHANNEL", "SCHEDULER_SLACK_TIMEOUT_CHANNEL", "#ops-alerts");
    this.softTimeoutPercent =
        getEnvWithFallback("RATCHET_SOFT_TIMEOUT_PERCENT", "SCHEDULER_SOFT_TIMEOUT_PERCENT", "80");

    this.threadPoolQueueSize =
        getEnvWithFallback(
            "RATCHET_THREAD_POOL_QUEUE_SIZE", "SCHEDULER_THREAD_POOL_QUEUE_SIZE", "100");
    this.threadPoolSizeBatchChild =
        getEnvWithFallback(
            "RATCHET_THREAD_POOL_SIZE_BATCH_CHILD", "SCHEDULER_THREAD_POOL_SIZE_BATCH_CHILD", "30");
    this.threadPoolSizeBatchParent =
        getEnvWithFallback(
            "RATCHET_THREAD_POOL_SIZE_BATCH_PARENT",
            "SCHEDULER_THREAD_POOL_SIZE_BATCH_PARENT",
            "2");
    this.threadPoolSizeChain =
        getEnvWithFallback(
            "RATCHET_THREAD_POOL_SIZE_CHAIN", "SCHEDULER_THREAD_POOL_SIZE_CHAIN", "10");
    this.threadPoolSizeDefault =
        getEnvWithFallback(
            "RATCHET_THREAD_POOL_SIZE_DEFAULT", "SCHEDULER_THREAD_POOL_SIZE_DEFAULT", "10");
    this.threadPoolSizeDlq =
        getEnvWithFallback("RATCHET_THREAD_POOL_SIZE_DLQ", "SCHEDULER_THREAD_POOL_SIZE_DLQ", "2");
    this.threadPoolSizeRecurring =
        getEnvWithFallback(
            "RATCHET_THREAD_POOL_SIZE_RECURRING", "SCHEDULER_THREAD_POOL_SIZE_RECURRING", "5");
    this.threadPoolSizeSingle =
        getEnvWithFallback(
            "RATCHET_THREAD_POOL_SIZE_SINGLE", "SCHEDULER_THREAD_POOL_SIZE_SINGLE", "20");

    this.workerDefaultSla =
        getEnvWithFallback("RATCHET_WORKER_DEFAULT_SLA", "WORKER_DEFAULT_SLA", "1800");
    this.workerUseVirtualThreads =
        getEnvWithFallback(
            "RATCHET_WORKER_USE_VIRTUAL_THREADS", "WORKER_USE_VIRTUAL_THREADS", "false");
  }

  /**
   * Emits a startup WARN for any numeric env var that is present but unparseable as a non-negative
   * integer. Does not change fallback behavior — silent default on invalid input is intentional.
   * The WARN gives operators a visible signal that their configuration is not taking effect.
   */
  @PostConstruct
  void validateNumericEnvVars() {
    for (String[] pair : NUMERIC_ENV_VARS) {
      String preferred = pair[0];
      String legacy = pair[1];
      String raw = System.getenv(preferred);
      String source = preferred;
      if (raw == null || raw.isEmpty()) {
        raw = System.getenv(legacy);
        source = legacy;
      }
      if (raw == null || raw.isEmpty()) {
        raw = System.getProperty(preferred);
        source = preferred;
        if (raw == null || raw.isEmpty()) {
          raw = System.getProperty(legacy);
          source = legacy;
        }
      }
      if (raw != null && !raw.isEmpty() && !raw.matches("\\d+")) {
        log.warnf(
            "Ratchet configuration: env var %s=%s is not a valid non-negative integer; using"
                + " built-in default.",
            source, raw);
      }
    }
  }

  private static String getEnvOrDefault(String key, String defaultValue) {
    String value = System.getenv(key);
    if (value != null && !value.isEmpty()) {
      return value;
    }
    value = System.getProperty(key);
    return (value != null && !value.isEmpty()) ? value : defaultValue;
  }

  private static String getEnvWithFallback(String preferred, String legacy, String defaultValue) {
    String value = getEnvOrDefault(preferred, null);
    if (value != null) {
      return value;
    }
    return getEnvOrDefault(legacy, defaultValue);
  }

  private static int parseIntOrDefault(String envValue, int defaultValue) {
    if (envValue == null || envValue.isEmpty() || !envValue.matches("\\d+")) {
      return defaultValue;
    }
    return Integer.parseInt(envValue);
  }

  private static long parseLongOrDefault(String envValue, long defaultValue) {
    if (envValue == null || envValue.isEmpty() || !envValue.matches("\\d+")) {
      return defaultValue;
    }
    return Long.parseLong(envValue);
  }

  public String getDlqPurgeCron() {
    return dlqPurgeCron;
  }

  public Long getDlqPurgeDays() {
    return parseLongOrDefault(dlqPurgeDays, 90L);
  }

  public boolean isDlqPurgeEnabled() {
    return Boolean.parseBoolean(dlqPurgeEnabled);
  }

  public Integer getIdempotencyRetryInitialDelayMs() {
    return parseIntOrDefault(idempotencyRetryInitialDelayMs, 50);
  }

  public Integer getIdempotencyRetryMaxAttempts() {
    return parseIntOrDefault(idempotencyRetryMaxAttempts, 3);
  }

  public Integer getIdempotencyRetryMaxDelayMs() {
    return parseIntOrDefault(idempotencyRetryMaxDelayMs, 500);
  }

  public Integer getJobArchiveBatchSize() {
    return parseIntOrDefault(jobArchiveBatchSize, 1000);
  }

  public String getJobArchiverCron() {
    return jobArchiverCron;
  }

  public Long getJobRetentionDays() {
    return parseLongOrDefault(jobRetentionDays, 90L);
  }

  public boolean isLogPurgeEnabled() {
    return Boolean.parseBoolean(logPurgeEnabled);
  }

  public String getLogPurgeCron() {
    return logPurgerCron;
  }

  public Long getLogRetentionDays() {
    return parseLongOrDefault(logRetentionDays, 30L);
  }

  public Integer getMaxPayloadKB() {
    return parseIntOrDefault(maxPayloadKb, 100);
  }

  public String getMetricsClustering() {
    return metricsClustering != null ? metricsClustering.toLowerCase() : "none";
  }

  public Long getNodeHeartbeatIntervalSeconds() {
    return parseLongOrDefault(nodeHeartbeatIntervalSeconds, 60L);
  }

  public Long getNodeOrphanGraceSeconds() {
    return parseLongOrDefault(nodeOrphanGraceSeconds, 60L);
  }

  public Long getOrphanScanIntervalMinutes() {
    return parseLongOrDefault(orphanScanIntervalMinutes, 5L);
  }

  public Integer getPriorityBoostIntervalMinutes() {
    return parseIntOrDefault(priorityBoostIntervalMinutes, 15);
  }

  public Integer getPollerBatchSize() {
    return parseIntOrDefault(pollerBatchSize, 50);
  }

  public Long getPollerBurstDelayMs() {
    return parseLongOrDefault(pollerBurstDelayMs, 500L);
  }

  public Long getPollerDeepIdleDelayMs() {
    return parseLongOrDefault(pollerDeepIdleDelayMs, 30000L);
  }

  public Long getPollerDeepIdleThresholdMs() {
    return parseLongOrDefault(pollerDeepIdleThresholdMs, 60000L);
  }

  public Integer getPollerIdleThreshold() {
    return parseIntOrDefault(pollerIdleThreshold, 3);
  }

  public Long getPollerMaxDelayMs() {
    return parseLongOrDefault(pollerMaxDelayMs, 10000L);
  }

  public Long getPollerMinDelayMs() {
    return parseLongOrDefault(pollerMinDelayMs, 2000L);
  }

  public Integer getPollerClaimHeadroomFactor() {
    return parseIntOrDefault(pollerClaimHeadroomFactor, 0);
  }

  public Long getRetryBufferDrainIntervalMs() {
    return parseLongOrDefault(retryBufferDrainIntervalMs, 1000L);
  }

  public Integer getRecurringBatchLimit() {
    return parseIntOrDefault(recurringBatchLimit, 20);
  }

  public Long getRecurringMaxPollMs() {
    return parseLongOrDefault(recurringMaxPollMs, 60000L);
  }

  public Long getRecurringPollMs() {
    return parseLongOrDefault(recurringPollMs, 1000L);
  }

  public String getSlackDlqChannel() {
    return slackDlqChannel != null && !slackDlqChannel.isEmpty()
        ? slackDlqChannel
        : "#job-scheduler-dlq";
  }

  public String getSlackTimeoutChannel() {
    return slackTimeoutChannel != null && !slackTimeoutChannel.isEmpty()
        ? slackTimeoutChannel
        : "#ops-alerts";
  }

  public Integer getSoftTimeoutPercent() {
    int percent = parseIntOrDefault(softTimeoutPercent, 80);
    return (percent > 0 && percent < 100) ? percent : 80;
  }

  public Integer getThreadPoolQueueSize() {
    return parseIntOrDefault(threadPoolQueueSize, 100);
  }

  public Integer getThreadPoolSizeBatchChild() {
    return parseIntOrDefault(threadPoolSizeBatchChild, 30);
  }

  public Integer getThreadPoolSizeBatchParent() {
    return parseIntOrDefault(threadPoolSizeBatchParent, 2);
  }

  public Integer getThreadPoolSizeChain() {
    return parseIntOrDefault(threadPoolSizeChain, 10);
  }

  public Integer getThreadPoolSizeDefault() {
    return parseIntOrDefault(threadPoolSizeDefault, 10);
  }

  public Integer getThreadPoolSizeDlq() {
    return parseIntOrDefault(threadPoolSizeDlq, 2);
  }

  public Integer getThreadPoolSizeRecurring() {
    return parseIntOrDefault(threadPoolSizeRecurring, 5);
  }

  public Integer getThreadPoolSizeSingle() {
    return parseIntOrDefault(threadPoolSizeSingle, 20);
  }

  public Long getWorkerDefaultSLA() {
    return parseLongOrDefault(workerDefaultSla, 1800L);
  }

  public boolean isCircuitBreakerEnabled() {
    return Boolean.parseBoolean(circuitBreakerEnabled);
  }

  public boolean isDynamicHeartbeatEnabled() {
    return Boolean.parseBoolean(dynamicHeartbeatEnabled);
  }

  public boolean isJobArchiveEnabled() {
    return Boolean.parseBoolean(jobArchiveEnabled);
  }

  public boolean isSlackNotificationsEnabled() {
    return Boolean.parseBoolean(slackNotificationsEnabled);
  }

  public boolean isWorkerUseVirtualThreads() {
    if (workerUseVirtualThreads == null || workerUseVirtualThreads.isEmpty()) {
      return false;
    }
    return Boolean.parseBoolean(workerUseVirtualThreads);
  }

  /**
   * Returns the virtual thread limit for the given job execution type. Reads from
   * RATCHET_VIRTUAL_THREAD_LIMIT_{type} with fallback to VIRTUAL_THREAD_LIMIT_{type}.
   */
  public int getVirtualThreadLimit(String jobTypeName, int defaultLimit) {
    String value =
        getEnvWithFallback(
            "RATCHET_VIRTUAL_THREAD_LIMIT_" + jobTypeName,
            "VIRTUAL_THREAD_LIMIT_" + jobTypeName,
            null);
    if (value != null && !value.isBlank()) {
      try {
        return Integer.parseInt(value.trim());
      } catch (NumberFormatException e) {
        // fall through to default
      }
    }
    return defaultLimit;
  }
}

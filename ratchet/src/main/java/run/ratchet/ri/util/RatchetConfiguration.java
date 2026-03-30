package run.ratchet.ri.util;

import jakarta.enterprise.context.ApplicationScoped;

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

  // ============================================================================
  // Circuit Breaker Configuration
  // ============================================================================

  private final String cbDefaultFailureRate;
  private final String cbDefaultWaitSeconds;
  private final String cbDefaultWindowSize;
  private final String cbExternalFailureRate;
  private final String cbExternalWaitSeconds;
  private final String circuitBreakerEnabled;

  // ============================================================================
  // Idempotency Retry Configuration
  // ============================================================================

  private final String idempotencyRetryMaxAttempts;
  private final String idempotencyRetryInitialDelayMs;
  private final String idempotencyRetryMaxDelayMs;

  // ============================================================================
  // Data Retention and Archiving Configuration
  // ============================================================================

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

  // ============================================================================
  // Node Health and Heartbeat Configuration
  // ============================================================================

  private final String nodeHeartbeatIntervalSeconds;
  private final String nodeOrphanGraceSeconds;
  private final String orphanScanIntervalMinutes;

  // ============================================================================
  // Priority Boosting Configuration
  // ============================================================================

  private final String priorityBoostIntervalMinutes;

  // ============================================================================
  // Poller Configuration
  // ============================================================================

  private final String pollerBatchSize;
  private final String pollerBurstDelayMs;
  private final String pollerDeepIdleDelayMs;
  private final String pollerDeepIdleThresholdMs;
  private final String pollerIdleThreshold;
  private final String pollerMaxDelayMs;
  private final String pollerMinDelayMs;

  // ============================================================================
  // Recurring Job Configuration
  // ============================================================================

  private final String recurringBatchLimit;
  private final String recurringMaxPollMs;
  private final String recurringPollMs;

  // ============================================================================
  // Slack Notification Configuration
  // ============================================================================

  private final String slackDlqChannel;
  private final String slackNotificationsEnabled;
  private final String slackTimeoutChannel;
  private final String softTimeoutPercent;

  // ============================================================================
  // Thread Pool Configuration
  // ============================================================================

  private final String threadPoolQueueSize;
  private final String threadPoolSizeBatchChild;
  private final String threadPoolSizeBatchParent;
  private final String threadPoolSizeChain;
  private final String threadPoolSizeDefault;
  private final String threadPoolSizeDlq;
  private final String threadPoolSizeRecurring;
  private final String threadPoolSizeSingle;

  // ============================================================================
  // Worker Configuration
  // ============================================================================

  private final String workerDefaultSla;
  private final String workerUseVirtualThreads;

  /**
   * Creates a new configuration instance, reading values from environment variables and system
   * properties. Environment variables are checked first; if not set, system properties are checked;
   * if neither is set, the default value is used.
   *
   * <p>This public no-arg constructor also serves as the CDI-required constructor. CDI proxies
   * subclass the bean and delegate all method calls to the contextual instance, so the values
   * initialized in a proxy's super-constructor are never accessed.
   */
  public RatchetConfiguration() {
    // Circuit Breaker
    this.cbDefaultFailureRate = getEnvOrDefault("SCHEDULER_CB_DEFAULT_FAILURE_RATE", "50");
    this.cbDefaultWaitSeconds = getEnvOrDefault("SCHEDULER_CB_DEFAULT_WAIT_SECONDS", "30");
    this.cbDefaultWindowSize = getEnvOrDefault("SCHEDULER_CB_DEFAULT_WINDOW_SIZE", "100");
    this.cbExternalFailureRate = getEnvOrDefault("SCHEDULER_CB_EXTERNAL_FAILURE_RATE", "60");
    this.cbExternalWaitSeconds = getEnvOrDefault("SCHEDULER_CB_EXTERNAL_WAIT_SECONDS", "60");
    this.circuitBreakerEnabled = getEnvOrDefault("SCHEDULER_CIRCUIT_BREAKER_ENABLED", "true");

    // Idempotency Retry
    this.idempotencyRetryMaxAttempts =
        getEnvOrDefault("SCHEDULER_IDEMPOTENCY_RETRY_MAX_ATTEMPTS", "3");
    this.idempotencyRetryInitialDelayMs =
        getEnvOrDefault("SCHEDULER_IDEMPOTENCY_RETRY_INITIAL_DELAY_MS", "50");
    this.idempotencyRetryMaxDelayMs =
        getEnvOrDefault("SCHEDULER_IDEMPOTENCY_RETRY_MAX_DELAY_MS", "500");

    // Data Retention and Archiving
    this.dlqPurgeCron = getEnvOrDefault("SCHEDULER_DLQ_PURGE_CRON", "0 0 2 * * ?");
    this.dlqPurgeDays = getEnvOrDefault("DLQ_PURGE_DAYS", "90");
    this.dlqPurgeEnabled = getEnvOrDefault("SCHEDULER_DLQ_PURGE_ENABLED", "true");
    this.dynamicHeartbeatEnabled = getEnvOrDefault("SCHEDULER_DYNAMIC_HEARTBEAT_ENABLED", "true");
    this.jobArchiverCron = getEnvOrDefault("SCHEDULER_JOB_ARCHIVER_CRON", "0 0 1 * * ?");
    this.jobArchiveBatchSize = getEnvOrDefault("SCHEDULER_JOB_ARCHIVE_BATCH_SIZE", "1000");
    this.jobArchiveEnabled = getEnvOrDefault("SCHEDULER_JOB_ARCHIVE_ENABLED", "true");
    this.jobRetentionDays = getEnvOrDefault("SCHEDULER_JOB_RETENTION_DAYS", "90");
    this.logPurgeEnabled = getEnvOrDefault("SCHEDULER_LOG_PURGE_ENABLED", "true");
    this.logPurgerCron = getEnvOrDefault("LOG_PURGER_CRON", "0 30 2 * * ?");
    this.logRetentionDays = getEnvOrDefault("LOG_RETENTION_DAYS", "30");
    this.maxPayloadKb = getEnvOrDefault("SCHEDULER_MAX_PAYLOAD_KB", "100");
    this.metricsClustering = getEnvOrDefault("SCHEDULER_METRICS_CLUSTERING", "none");

    // Node Health and Heartbeat
    this.nodeHeartbeatIntervalSeconds = getEnvOrDefault("NODE_HEARTBEAT_INTERVAL_SECONDS", "10");
    this.nodeOrphanGraceSeconds = getEnvOrDefault("NODE_ORPHAN_GRACE_SECONDS", "60");
    this.orphanScanIntervalMinutes = getEnvOrDefault("SCHEDULER_ORPHAN_SCAN_INTERVAL_MINUTES", "5");

    // Priority Boosting
    this.priorityBoostIntervalMinutes =
        getEnvOrDefault("SCHEDULER_PRIORITY_BOOST_INTERVAL_MINUTES", "15");

    // Poller
    this.pollerBatchSize = getEnvOrDefault("POLLER_BATCH_SIZE", "50");
    this.pollerBurstDelayMs = getEnvOrDefault("POLLER_BURST_DELAY_MS", "500");
    this.pollerDeepIdleDelayMs = getEnvOrDefault("POLLER_DEEP_IDLE_DELAY_MS", "30000");
    this.pollerDeepIdleThresholdMs = getEnvOrDefault("POLLER_DEEP_IDLE_THRESHOLD_MS", "60000");
    this.pollerIdleThreshold = getEnvOrDefault("POLLER_IDLE_THRESHOLD", "3");
    this.pollerMaxDelayMs = getEnvOrDefault("POLLER_MAX_DELAY_MS", "10000");
    this.pollerMinDelayMs = getEnvOrDefault("POLLER_MIN_DELAY_MS", "2000");

    // Recurring Job
    this.recurringBatchLimit = getEnvOrDefault("RECURRING_BATCH_LIMIT", "20");
    this.recurringMaxPollMs = getEnvOrDefault("RECURRING_MAX_POLL_MS", "60000");
    this.recurringPollMs = getEnvOrDefault("RECURRING_POLL_MS", "1000");

    // Slack Notification
    this.slackDlqChannel = getEnvOrDefault("SCHEDULER_SLACK_DLQ_CHANNEL", "#job-scheduler-dlq");
    this.slackNotificationsEnabled =
        getEnvOrDefault("SCHEDULER_SLACK_NOTIFICATIONS_ENABLED", "true");
    this.slackTimeoutChannel = getEnvOrDefault("SCHEDULER_SLACK_TIMEOUT_CHANNEL", "#ops-alerts");
    this.softTimeoutPercent = getEnvOrDefault("SCHEDULER_SOFT_TIMEOUT_PERCENT", "80");

    // Thread Pool
    this.threadPoolQueueSize = getEnvOrDefault("SCHEDULER_THREAD_POOL_QUEUE_SIZE", "100");
    this.threadPoolSizeBatchChild = getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_BATCH_CHILD", "30");
    this.threadPoolSizeBatchParent =
        getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_BATCH_PARENT", "2");
    this.threadPoolSizeChain = getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_CHAIN", "10");
    this.threadPoolSizeDefault = getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_DEFAULT", "10");
    this.threadPoolSizeDlq = getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_DLQ", "2");
    this.threadPoolSizeRecurring = getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_RECURRING", "5");
    this.threadPoolSizeSingle = getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_SINGLE", "20");

    // Worker
    this.workerDefaultSla = getEnvOrDefault("WORKER_DEFAULT_SLA", "1800");
    this.workerUseVirtualThreads = getEnvOrDefault("WORKER_USE_VIRTUAL_THREADS", "false");
  }

  /* ─────────────────────── env helpers ─────────────────────── */

  private static String getEnvOrDefault(String key, String defaultValue) {
    String value = System.getenv(key);
    if (value != null && !value.isEmpty()) {
      return value;
    }
    value = System.getProperty(key);
    return (value != null && !value.isEmpty()) ? value : defaultValue;
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

  /* ─────────────────────── accessor methods ─────────────────────── */

  public String getDlqPurgeCron() {
    return dlqPurgeCron != null && !dlqPurgeCron.isEmpty() ? dlqPurgeCron : "0 0 2 * * ?";
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
    return jobArchiverCron != null && !jobArchiverCron.isEmpty() ? jobArchiverCron : "0 0 1 * * ?";
  }

  public Long getJobRetentionDays() {
    return parseLongOrDefault(jobRetentionDays, 90L);
  }

  public boolean isLogPurgeEnabled() {
    return Boolean.parseBoolean(logPurgeEnabled);
  }

  public String getLogPurgeCron() {
    return logPurgerCron != null && !logPurgerCron.isEmpty() ? logPurgerCron : "0 30 2 * * ?";
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
    return workersUseVirtualThreads();
  }

  public boolean workersUseVirtualThreads() {
    if (workerUseVirtualThreads == null || workerUseVirtualThreads.isEmpty()) {
      return false;
    }
    return Boolean.parseBoolean(workerUseVirtualThreads);
  }
}

package run.ratchet.ri.util;

/**
 * Utility class providing configuration parameters for job processing workflows. This class
 * retrieves configuration values from environment variables, applying default values where these
 * variables are not set or invalid. The configuration is used to fine-tune system behaviors such as
 * poller delays, worker SLA, log retention, and more.
 *
 * <p>The provided methods allow access to these configuration values with appropriate type
 * conversions and fallback default handling.
 */
public final class SchedulerConfig {

  // ============================================================================
  // Circuit Breaker Configuration
  // ============================================================================

  public static final String CB_DEFAULT_FAILURE_RATE =
      getEnvOrDefault("SCHEDULER_CB_DEFAULT_FAILURE_RATE", "50");

  public static final String CB_DEFAULT_WAIT_SECONDS =
      getEnvOrDefault("SCHEDULER_CB_DEFAULT_WAIT_SECONDS", "30");

  public static final String CB_DEFAULT_WINDOW_SIZE =
      getEnvOrDefault("SCHEDULER_CB_DEFAULT_WINDOW_SIZE", "100");

  public static final String CB_EXTERNAL_FAILURE_RATE =
      getEnvOrDefault("SCHEDULER_CB_EXTERNAL_FAILURE_RATE", "60");

  public static final String CB_EXTERNAL_WAIT_SECONDS =
      getEnvOrDefault("SCHEDULER_CB_EXTERNAL_WAIT_SECONDS", "60");

  public static final String CIRCUIT_BREAKER_ENABLED =
      getEnvOrDefault("SCHEDULER_CIRCUIT_BREAKER_ENABLED", "true");

  // ============================================================================
  // Idempotency Retry Configuration
  // ============================================================================

  public static final String IDEMPOTENCY_RETRY_MAX_ATTEMPTS =
      getEnvOrDefault("SCHEDULER_IDEMPOTENCY_RETRY_MAX_ATTEMPTS", "3");

  public static final String IDEMPOTENCY_RETRY_INITIAL_DELAY_MS =
      getEnvOrDefault("SCHEDULER_IDEMPOTENCY_RETRY_INITIAL_DELAY_MS", "50");

  public static final String IDEMPOTENCY_RETRY_MAX_DELAY_MS =
      getEnvOrDefault("SCHEDULER_IDEMPOTENCY_RETRY_MAX_DELAY_MS", "500");

  // ============================================================================
  // Data Retention and Archiving Configuration
  // ============================================================================

  public static final String DLQ_PURGE_DAYS = getEnvOrDefault("DLQ_PURGE_DAYS", "90");

  public static final String DYNAMIC_HEARTBEAT_ENABLED =
      getEnvOrDefault("SCHEDULER_DYNAMIC_HEARTBEAT_ENABLED", "true");

  public static final String JOB_ARCHIVER_CRON =
      getEnvOrDefault("SCHEDULER_JOB_ARCHIVER_CRON", "0 1 * * *");

  public static final String JOB_ARCHIVE_BATCH_SIZE =
      getEnvOrDefault("SCHEDULER_JOB_ARCHIVE_BATCH_SIZE", "1000");

  public static final String JOB_ARCHIVE_ENABLED =
      getEnvOrDefault("SCHEDULER_JOB_ARCHIVE_ENABLED", "true");

  public static final String JOB_RETENTION_DAYS =
      getEnvOrDefault("SCHEDULER_JOB_RETENTION_DAYS", "90");

  public static final String LOG_PURGER_CRON = getEnvOrDefault("LOG_PURGER_CRON", "30 2 * * *");

  public static final String LOG_RETENTION_DAYS = getEnvOrDefault("LOG_RETENTION_DAYS", "30");

  public static final String MAX_PAYLOAD_KB = getEnvOrDefault("SCHEDULER_MAX_PAYLOAD_KB", "100");

  public static final String METRICS_CLUSTERING =
      getEnvOrDefault("SCHEDULER_METRICS_CLUSTERING", "none");

  // ============================================================================
  // Node Health and Heartbeat Configuration
  // ============================================================================

  public static final String NODE_HEARTBEAT_INTERVAL_SECONDS =
      getEnvOrDefault("NODE_HEARTBEAT_INTERVAL_SECONDS", "10");

  public static final String NODE_ORPHAN_GRACE_SECONDS =
      getEnvOrDefault("NODE_ORPHAN_GRACE_SECONDS", "60");

  // ============================================================================
  // Priority Boosting Configuration
  // ============================================================================

  public static final String PRIORITY_BOOST_INTERVAL_MINUTES =
      getEnvOrDefault("SCHEDULER_PRIORITY_BOOST_INTERVAL_MINUTES", "15");

  // ============================================================================
  // Poller Configuration
  // ============================================================================

  public static final String POLLER_BATCH_SIZE = getEnvOrDefault("POLLER_BATCH_SIZE", "50");

  public static final String POLLER_BURST_DELAY_MS =
      getEnvOrDefault("POLLER_BURST_DELAY_MS", "500");

  public static final String POLLER_DEEP_IDLE_DELAY_MS =
      getEnvOrDefault("POLLER_DEEP_IDLE_DELAY_MS", "30000");

  public static final String POLLER_DEEP_IDLE_THRESHOLD_MS =
      getEnvOrDefault("POLLER_DEEP_IDLE_THRESHOLD_MS", "60000");

  public static final String POLLER_IDLE_THRESHOLD = getEnvOrDefault("POLLER_IDLE_THRESHOLD", "3");

  public static final String POLLER_MAX_DELAY_MS = getEnvOrDefault("POLLER_MAX_DELAY_MS", "10000");

  public static final String POLLER_MIN_DELAY_MS = getEnvOrDefault("POLLER_MIN_DELAY_MS", "2000");

  // ============================================================================
  // Recurring Job Configuration
  // ============================================================================

  public static final String RECURRING_BATCH_LIMIT = getEnvOrDefault("RECURRING_BATCH_LIMIT", "20");

  public static final String RECURRING_MAX_POLL_MS =
      getEnvOrDefault("RECURRING_MAX_POLL_MS", "60000");

  public static final String RECURRING_POLL_MS = getEnvOrDefault("RECURRING_POLL_MS", "1000");

  // ============================================================================
  // Slack Notification Configuration
  // ============================================================================

  public static final String SLACK_DLQ_CHANNEL =
      getEnvOrDefault("SCHEDULER_SLACK_DLQ_CHANNEL", "#job-scheduler-dlq");

  public static final String SLACK_NOTIFICATIONS_ENABLED =
      getEnvOrDefault("SCHEDULER_SLACK_NOTIFICATIONS_ENABLED", "true");

  public static final String SLACK_TIMEOUT_CHANNEL =
      getEnvOrDefault("SCHEDULER_SLACK_TIMEOUT_CHANNEL", "#ops-alerts");

  public static final String SOFT_TIMEOUT_PERCENT =
      getEnvOrDefault("SCHEDULER_SOFT_TIMEOUT_PERCENT", "80");

  // ============================================================================
  // Thread Pool Configuration
  // ============================================================================

  public static final String THREAD_POOL_QUEUE_SIZE =
      getEnvOrDefault("SCHEDULER_THREAD_POOL_QUEUE_SIZE", "100");

  public static final String THREAD_POOL_SIZE_BATCH_CHILD =
      getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_BATCH_CHILD", "30");

  public static final String THREAD_POOL_SIZE_BATCH_PARENT =
      getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_BATCH_PARENT", "2");

  public static final String THREAD_POOL_SIZE_CHAIN =
      getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_CHAIN", "10");

  public static final String THREAD_POOL_SIZE_DEFAULT =
      getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_DEFAULT", "10");

  public static final String THREAD_POOL_SIZE_DLQ =
      getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_DLQ", "2");

  public static final String THREAD_POOL_SIZE_RECURRING =
      getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_RECURRING", "5");

  public static final String THREAD_POOL_SIZE_SINGLE =
      getEnvOrDefault("SCHEDULER_THREAD_POOL_SIZE_SINGLE", "20");

  // ============================================================================
  // Worker Configuration
  // ============================================================================

  public static final String WORKER_DEFAULT_SLA = getEnvOrDefault("WORKER_DEFAULT_SLA", "1800");

  public static final String WORKER_USE_VIRTUAL_THREADS =
      getEnvOrDefault("WORKER_USE_VIRTUAL_THREADS", "false");

  private SchedulerConfig() {
    throw new UnsupportedOperationException("Utility class - do not instantiate");
  }

  /* ─────────────────────── env helpers ─────────────────────── */

  private static String getEnvOrDefault(String key, String defaultValue) {
    String value = System.getenv(key);
    return (value != null && !value.isEmpty()) ? value : defaultValue;
  }

  private static int getEnvOrDefaultInt(String envValue, int defaultValue) {
    if (envValue == null || envValue.isEmpty() || !envValue.matches("\\d+")) {
      return defaultValue;
    }
    return Integer.parseInt(envValue);
  }

  private static long getEnvOrDefaultLong(String envValue, long defaultValue) {
    if (envValue == null || envValue.isEmpty() || !envValue.matches("\\d+")) {
      return defaultValue;
    }
    return Long.parseLong(envValue);
  }

  /* ─────────────────────── accessor methods ─────────────────────── */

  public static Long getDlqPurgeDays() {
    return getEnvOrDefaultLong(DLQ_PURGE_DAYS, 90L);
  }

  public static Integer getIdempotencyRetryInitialDelayMs() {
    return getEnvOrDefaultInt(IDEMPOTENCY_RETRY_INITIAL_DELAY_MS, 50);
  }

  public static Integer getIdempotencyRetryMaxAttempts() {
    return getEnvOrDefaultInt(IDEMPOTENCY_RETRY_MAX_ATTEMPTS, 3);
  }

  public static Integer getIdempotencyRetryMaxDelayMs() {
    return getEnvOrDefaultInt(IDEMPOTENCY_RETRY_MAX_DELAY_MS, 500);
  }

  public static Integer getJobArchiveBatchSize() {
    return getEnvOrDefaultInt(JOB_ARCHIVE_BATCH_SIZE, 1000);
  }

  public static String getJobArchiverCron() {
    return JOB_ARCHIVER_CRON != null && !JOB_ARCHIVER_CRON.isEmpty()
        ? JOB_ARCHIVER_CRON
        : "0 1 * * *";
  }

  public static Long getJobRetentionDays() {
    return getEnvOrDefaultLong(JOB_RETENTION_DAYS, 90L);
  }

  public static Long getLogRetentionDays() {
    return getEnvOrDefaultLong(LOG_RETENTION_DAYS, 30L);
  }

  public static Integer getMaxPayloadKB() {
    return getEnvOrDefaultInt(MAX_PAYLOAD_KB, 100);
  }

  public static String getMetricsClustering() {
    return METRICS_CLUSTERING != null ? METRICS_CLUSTERING.toLowerCase() : "none";
  }

  public static Long getNodeHeartbeatIntervalSeconds() {
    return getEnvOrDefaultLong(NODE_HEARTBEAT_INTERVAL_SECONDS, 60L);
  }

  public static Long getNodeOrphanGraceSeconds() {
    return getEnvOrDefaultLong(NODE_ORPHAN_GRACE_SECONDS, 60L);
  }

  public static Integer getPriorityBoostIntervalMinutes() {
    return getEnvOrDefaultInt(PRIORITY_BOOST_INTERVAL_MINUTES, 15);
  }

  public static Integer getPollerBatchSize() {
    return getEnvOrDefaultInt(POLLER_BATCH_SIZE, 50);
  }

  public static Long getPollerBurstDelayMs() {
    return getEnvOrDefaultLong(POLLER_BURST_DELAY_MS, 500L);
  }

  public static Long getPollerDeepIdleDelayMs() {
    return getEnvOrDefaultLong(POLLER_DEEP_IDLE_DELAY_MS, 30000L);
  }

  public static Long getPollerDeepIdleThresholdMs() {
    return getEnvOrDefaultLong(POLLER_DEEP_IDLE_THRESHOLD_MS, 60000L);
  }

  public static Integer getPollerIdleThreshold() {
    return getEnvOrDefaultInt(POLLER_IDLE_THRESHOLD, 3);
  }

  public static Long getPollerMaxDelayMs() {
    return getEnvOrDefaultLong(POLLER_MAX_DELAY_MS, 10000L);
  }

  public static Long getPollerMinDelayMs() {
    return getEnvOrDefaultLong(POLLER_MIN_DELAY_MS, 2000L);
  }

  public static Integer getRecurringBatchLimit() {
    return getEnvOrDefaultInt(RECURRING_BATCH_LIMIT, 20);
  }

  public static Long getRecurringMaxPollMs() {
    return getEnvOrDefaultLong(RECURRING_MAX_POLL_MS, 60000L);
  }

  public static Long getRecurringPollMs() {
    return getEnvOrDefaultLong(RECURRING_POLL_MS, 1000L);
  }

  public static String getSlackDlqChannel() {
    return SLACK_DLQ_CHANNEL != null && !SLACK_DLQ_CHANNEL.isEmpty()
        ? SLACK_DLQ_CHANNEL
        : "#job-scheduler-dlq";
  }

  public static String getSlackTimeoutChannel() {
    return SLACK_TIMEOUT_CHANNEL != null && !SLACK_TIMEOUT_CHANNEL.isEmpty()
        ? SLACK_TIMEOUT_CHANNEL
        : "#ops-alerts";
  }

  public static Integer getSoftTimeoutPercent() {
    int percent = getEnvOrDefaultInt(SOFT_TIMEOUT_PERCENT, 80);
    return (percent > 0 && percent < 100) ? percent : 80;
  }

  public static Integer getThreadPoolQueueSize() {
    return getEnvOrDefaultInt(THREAD_POOL_QUEUE_SIZE, 100);
  }

  public static Integer getThreadPoolSizeBatchChild() {
    return getEnvOrDefaultInt(THREAD_POOL_SIZE_BATCH_CHILD, 30);
  }

  public static Integer getThreadPoolSizeBatchParent() {
    return getEnvOrDefaultInt(THREAD_POOL_SIZE_BATCH_PARENT, 2);
  }

  public static Integer getThreadPoolSizeChain() {
    return getEnvOrDefaultInt(THREAD_POOL_SIZE_CHAIN, 10);
  }

  public static Integer getThreadPoolSizeDefault() {
    return getEnvOrDefaultInt(THREAD_POOL_SIZE_DEFAULT, 10);
  }

  public static Integer getThreadPoolSizeDlq() {
    return getEnvOrDefaultInt(THREAD_POOL_SIZE_DLQ, 2);
  }

  public static Integer getThreadPoolSizeRecurring() {
    return getEnvOrDefaultInt(THREAD_POOL_SIZE_RECURRING, 5);
  }

  public static Integer getThreadPoolSizeSingle() {
    return getEnvOrDefaultInt(THREAD_POOL_SIZE_SINGLE, 20);
  }

  public static Long getWorkerDefaultSLA() {
    return getEnvOrDefaultLong(WORKER_DEFAULT_SLA, 1800L);
  }

  public static boolean isCircuitBreakerEnabled() {
    return Boolean.parseBoolean(CIRCUIT_BREAKER_ENABLED);
  }

  public static boolean isDynamicHeartbeatEnabled() {
    return Boolean.parseBoolean(DYNAMIC_HEARTBEAT_ENABLED);
  }

  public static boolean isJobArchiveEnabled() {
    return Boolean.parseBoolean(JOB_ARCHIVE_ENABLED);
  }

  public static boolean isSlackNotificationsEnabled() {
    return Boolean.parseBoolean(SLACK_NOTIFICATIONS_ENABLED);
  }

  public static boolean isWorkerUseVirtualThreads() {
    return workersUseVirtualThreads();
  }

  public static boolean workersUseVirtualThreads() {
    if (WORKER_USE_VIRTUAL_THREADS == null || WORKER_USE_VIRTUAL_THREADS.isEmpty()) {
      return false;
    }
    return Boolean.parseBoolean(WORKER_USE_VIRTUAL_THREADS);
  }
}

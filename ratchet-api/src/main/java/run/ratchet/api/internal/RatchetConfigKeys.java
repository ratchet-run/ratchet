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
package run.ratchet.api.internal;

import java.util.Locale;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.RatchetConfigKey;

/**
 * Catalog of typed configuration keys consumed by {@link run.ratchet.api.RatchetOptionsFactory}.
 *
 * <p><b>Framework-internal:</b> applications must not depend on this class. The key names and
 * environment variable mappings exposed here are framework wiring and may change between releases;
 * configure Ratchet via {@link run.ratchet.api.RatchetOptions} or the canonical property names
 * instead.
 *
 * @since 0.1.0
 */
public final class RatchetConfigKeys {

  public static final RatchetConfigKey<Integer> POLLER_BATCH_SIZE =
      intKey("ratchet.poller.batch-size", "RATCHET_POLLER_BATCH_SIZE", 50, 1);
  public static final RatchetConfigKey<Long> POLLER_BURST_DELAY_MS =
      longKey("ratchet.poller.burst-delay-ms", "RATCHET_POLLER_BURST_DELAY_MS", 500L, 0L);
  public static final RatchetConfigKey<Long> POLLER_MIN_DELAY_MS =
      longKey("ratchet.poller.min-delay-ms", "RATCHET_POLLER_MIN_DELAY_MS", 2000L, 0L);
  public static final RatchetConfigKey<Long> POLLER_MAX_DELAY_MS =
      longKey("ratchet.poller.max-delay-ms", "RATCHET_POLLER_MAX_DELAY_MS", 10000L, 1L);
  public static final RatchetConfigKey<Long> POLLER_DEEP_IDLE_DELAY_MS =
      longKey("ratchet.poller.deep-idle-delay-ms", "RATCHET_POLLER_DEEP_IDLE_DELAY_MS", 30000L, 0L);
  public static final RatchetConfigKey<Long> POLLER_DEEP_IDLE_THRESHOLD_MS =
      longKey(
          "ratchet.poller.deep-idle-threshold-ms",
          "RATCHET_POLLER_DEEP_IDLE_THRESHOLD_MS",
          60000L,
          0L);
  public static final RatchetConfigKey<Integer> POLLER_IDLE_THRESHOLD =
      intKey("ratchet.poller.idle-threshold", "RATCHET_POLLER_IDLE_THRESHOLD", 3, 0);
  public static final RatchetConfigKey<Integer> POLLER_CLAIM_HEADROOM_FACTOR =
      intKey("ratchet.poller.claim-headroom-factor", "RATCHET_POLLER_CLAIM_HEADROOM_FACTOR", 0, 0);

  public static final RatchetConfigKey<RatchetOptions.ThreadingMode> WORKER_DEFAULT_THREADING_MODE =
      new RatchetConfigKey<>(
          "ratchet.worker.default-threading-mode",
          "RATCHET_WORKER_DEFAULT_THREADING_MODE",
          RatchetOptions.ThreadingMode.PLATFORM,
          raw -> RatchetOptions.ThreadingMode.valueOf(raw.trim().toUpperCase(Locale.ROOT)));
  public static final RatchetConfigKey<String> WORKER_JOB_EXECUTOR_JNDI =
      stringKey(
          "ratchet.worker.job-executor-jndi",
          "RATCHET_WORKER_JOB_EXECUTOR_JNDI",
          "java:comp/DefaultManagedExecutorService");
  public static final RatchetConfigKey<String> WORKER_SCHEDULED_EXECUTOR_JNDI =
      stringKey(
          "ratchet.worker.scheduled-executor-jndi",
          "RATCHET_WORKER_SCHEDULED_EXECUTOR_JNDI",
          "java:comp/DefaultManagedScheduledExecutorService");
  public static final RatchetConfigKey<String> COORDINATOR_THREAD_FACTORY_JNDI =
      stringKey(
          "ratchet.coordinator.thread-factory-jndi",
          "RATCHET_COORDINATOR_THREAD_FACTORY_JNDI",
          "java:comp/DefaultManagedThreadFactory");
  public static final RatchetConfigKey<String> WORKER_VIRTUAL_EXECUTOR_JNDI =
      stringKey("ratchet.worker.virtual-executor-jndi", "RATCHET_WORKER_VIRTUAL_EXECUTOR_JNDI", "");
  public static final RatchetConfigKey<Boolean> WORKER_VIRTUAL_COUNTER_ACCOUNTING =
      boolKey(
          "ratchet.worker.virtual-counter-accounting",
          "RATCHET_WORKER_VIRTUAL_COUNTER_ACCOUNTING",
          false);
  public static final RatchetConfigKey<Integer> THREAD_POOL_QUEUE_SIZE =
      intKey("ratchet.thread-pool.queue-size", "RATCHET_THREAD_POOL_QUEUE_SIZE", 100, 0);
  public static final RatchetConfigKey<Integer> THREAD_POOL_SIZE_SINGLE =
      intKey("ratchet.thread-pool.size.single", "RATCHET_THREAD_POOL_SIZE_SINGLE", 20, 0);
  public static final RatchetConfigKey<Integer> THREAD_POOL_SIZE_RECURRING =
      intKey("ratchet.thread-pool.size.recurring", "RATCHET_THREAD_POOL_SIZE_RECURRING", 5, 0);
  public static final RatchetConfigKey<Integer> THREAD_POOL_SIZE_BATCH_CHILD =
      intKey("ratchet.thread-pool.size.batch-child", "RATCHET_THREAD_POOL_SIZE_BATCH_CHILD", 30, 0);
  public static final RatchetConfigKey<Integer> THREAD_POOL_SIZE_BATCH_PARENT =
      intKey(
          "ratchet.thread-pool.size.batch-parent", "RATCHET_THREAD_POOL_SIZE_BATCH_PARENT", 2, 0);
  public static final RatchetConfigKey<Integer> THREAD_POOL_SIZE_CHAIN =
      intKey("ratchet.thread-pool.size.chain-step", "RATCHET_THREAD_POOL_SIZE_CHAIN", 10, 0);
  public static final RatchetConfigKey<Integer> THREAD_POOL_SIZE_DLQ_ALERT =
      intKey("ratchet.thread-pool.size.dlq-alert", "RATCHET_THREAD_POOL_SIZE_DLQ_ALERT", 2, 0);
  public static final RatchetConfigKey<Integer> THREAD_POOL_SIZE_WORKFLOW_BRANCH =
      intKey(
          "ratchet.thread-pool.size.workflow-branch",
          "RATCHET_THREAD_POOL_SIZE_WORKFLOW_BRANCH",
          10,
          0);
  public static final RatchetConfigKey<Integer> THREAD_POOL_SIZE_WORKFLOW_JOIN =
      intKey(
          "ratchet.thread-pool.size.workflow-join",
          "RATCHET_THREAD_POOL_SIZE_WORKFLOW_JOIN",
          10,
          0);

  public static final RatchetConfigKey<String> NODE_ID =
      stringKey("ratchet.node.id", "RATCHET_NODE_ID", "");
  public static final RatchetConfigKey<Long> NODE_HEARTBEAT_INTERVAL_SECONDS =
      longKey(
          "ratchet.node.heartbeat-interval-seconds",
          "RATCHET_NODE_HEARTBEAT_INTERVAL_SECONDS",
          10L,
          1L);
  public static final RatchetConfigKey<Long> NODE_ORPHAN_GRACE_SECONDS =
      longKey("ratchet.node.orphan-grace-seconds", "RATCHET_NODE_ORPHAN_GRACE_SECONDS", 60L, 0L);
  public static final RatchetConfigKey<Long> ORPHAN_SCAN_INTERVAL_MINUTES =
      longKey(
          "ratchet.node.orphan-scan-interval-minutes",
          "RATCHET_NODE_ORPHAN_SCAN_INTERVAL_MINUTES",
          5L,
          1L);
  public static final RatchetConfigKey<Boolean> DYNAMIC_HEARTBEAT_ENABLED =
      boolKey("ratchet.node.dynamic-heartbeat-enabled", "RATCHET_DYNAMIC_HEARTBEAT_ENABLED", true);

  public static final RatchetConfigKey<Integer> RECURRING_BATCH_LIMIT =
      intKey("ratchet.recurring.batch-limit", "RATCHET_RECURRING_BATCH_LIMIT", 20, 1);
  public static final RatchetConfigKey<Long> RECURRING_POLL_MS =
      longKey("ratchet.recurring.poll-ms", "RATCHET_RECURRING_POLL_MS", 1000L, 1L);
  public static final RatchetConfigKey<Long> RECURRING_MAX_POLL_MS =
      longKey("ratchet.recurring.max-poll-ms", "RATCHET_RECURRING_MAX_POLL_MS", 60000L, 1L);
  public static final RatchetConfigKey<Long> RECURRING_STARTUP_GRACE_SECONDS =
      longKey(
          "ratchet.recurring.startup-grace-seconds",
          "RATCHET_RECURRING_STARTUP_GRACE_SECONDS",
          60L,
          0L);
  public static final RatchetConfigKey<Long> RECURRING_CONVERGENCE_WINDOW_SECONDS =
      longKey(
          "ratchet.recurring.convergence-window-seconds",
          "RATCHET_RECURRING_CONVERGENCE_WINDOW_SECONDS",
          0L,
          0L);

  public static final RatchetConfigKey<Long> RETRY_BUFFER_DRAIN_INTERVAL_MS =
      longKey(
          "ratchet.retry-buffer.drain-interval-ms",
          "RATCHET_RETRY_BUFFER_DRAIN_INTERVAL_MS",
          1000L,
          50L);
  public static final RatchetConfigKey<Integer> SOFT_TIMEOUT_PERCENT =
      new RatchetConfigKey<>(
          "ratchet.timeout.soft-timeout-percent",
          "RATCHET_SOFT_TIMEOUT_PERCENT",
          80,
          raw -> {
            int percent = Integer.parseInt(raw);
            if (percent <= 0 || percent >= 100) {
              throw new IllegalArgumentException("soft timeout percent must be 1-99");
            }
            return percent;
          });
  public static final RatchetConfigKey<Long> WORKER_DEFAULT_SLA =
      longKey("ratchet.timeout.default-sla-seconds", "RATCHET_WORKER_DEFAULT_SLA", 1800L, 1L);
  public static final RatchetConfigKey<Integer> SIGNAL_TIMEOUT_BATCH_SIZE =
      intKey(
          "ratchet.timeout.signal-timeout-batch-size", "RATCHET_SIGNAL_TIMEOUT_BATCH_SIZE", 500, 1);

  public static final RatchetConfigKey<Boolean> DLQ_PURGE_ENABLED =
      boolKey("ratchet.dlq.purge-enabled", "RATCHET_DLQ_PURGE_ENABLED", true);
  public static final RatchetConfigKey<String> DLQ_PURGE_CRON =
      stringKey("ratchet.dlq.purge-cron", "RATCHET_DLQ_PURGE_CRON", "0 0 2 * * ?");
  public static final RatchetConfigKey<Long> DLQ_PURGE_DAYS =
      longKey("ratchet.dlq.purge-days", "RATCHET_DLQ_PURGE_DAYS", 90L, 0L);
  public static final RatchetConfigKey<Boolean> JOB_ARCHIVE_ENABLED =
      boolKey("ratchet.jobs.archive-enabled", "RATCHET_JOB_ARCHIVE_ENABLED", true);
  public static final RatchetConfigKey<String> JOB_ARCHIVE_CRON =
      stringKey("ratchet.jobs.archive-cron", "RATCHET_JOB_ARCHIVE_CRON", "0 0 1 * * ?");
  public static final RatchetConfigKey<Long> JOB_RETENTION_DAYS =
      longKey("ratchet.jobs.retention-days", "RATCHET_JOB_RETENTION_DAYS", 90L, 0L);
  public static final RatchetConfigKey<Integer> JOB_ARCHIVE_BATCH_SIZE =
      intKey("ratchet.jobs.archive-batch-size", "RATCHET_JOB_ARCHIVE_BATCH_SIZE", 1000, 1);
  public static final RatchetConfigKey<Boolean> LOG_PURGE_ENABLED =
      boolKey("ratchet.logs.purge-enabled", "RATCHET_LOG_PURGE_ENABLED", true);
  public static final RatchetConfigKey<String> LOG_PURGE_CRON =
      stringKey("ratchet.logs.purge-cron", "RATCHET_LOG_PURGE_CRON", "0 30 2 * * ?");
  public static final RatchetConfigKey<Long> LOG_RETENTION_DAYS =
      longKey("ratchet.logs.retention-days", "RATCHET_LOG_RETENTION_DAYS", 30L, 0L);

  public static final RatchetConfigKey<Boolean> NOTIFICATIONS_ENABLED =
      boolKey("ratchet.notifications.enabled", "RATCHET_NOTIFICATIONS_ENABLED", true);
  public static final RatchetConfigKey<String> DLQ_ALERT_CHANNEL =
      stringKey(
          "ratchet.notifications.dlq-alert-channel",
          "RATCHET_DLQ_ALERT_CHANNEL",
          "#job-scheduler-dlq");
  public static final RatchetConfigKey<String> TIMEOUT_ALERT_CHANNEL =
      stringKey(
          "ratchet.notifications.timeout-alert-channel",
          "RATCHET_TIMEOUT_ALERT_CHANNEL",
          "#ops-alerts");

  public static final RatchetConfigKey<Boolean> SCHEMA_AUTO_MIGRATE =
      boolKey("ratchet.schema.auto-migrate", "RATCHET_SCHEMA_AUTO_MIGRATE", false);
  public static final RatchetConfigKey<String> SCHEMA_MIGRATION_DIALECT =
      stringKey("ratchet.schema.migration-dialect", "RATCHET_SCHEMA_MIGRATION_DIALECT", "");
  public static final RatchetConfigKey<String> SCHEMA_MIGRATION_PREFIX =
      stringKey(
          "ratchet.schema.migration-prefix", "RATCHET_SCHEMA_MIGRATION_PREFIX", "ddl/migrations");

  public static final RatchetConfigKey<Integer> MAX_PAYLOAD_KB =
      intKey("ratchet.payload.max-payload-kb", "RATCHET_MAX_PAYLOAD_KB", 100, 1);
  public static final RatchetConfigKey<Long> MAX_RESULT_BYTES =
      longKey("ratchet.jobs.max-result-bytes", "RATCHET_JOB_RESULT_MAX_BYTES", 65536L, 0L);
  public static final RatchetConfigKey<String> METRICS_CLUSTERING =
      stringKey("ratchet.metrics.clustering", "RATCHET_METRICS_CLUSTERING", "none");
  public static final RatchetConfigKey<Boolean> ALLOW_EMPTY_CLASS_POLICY =
      boolKey("ratchet.allow-empty-class-policy", "RATCHET_ALLOW_EMPTY_CLASS_POLICY", false);
  public static final RatchetConfigKey<Boolean> REDACT_EMAILS =
      boolKey("ratchet.security.redact-emails", "RATCHET_REDACT_EMAILS", true);
  public static final RatchetConfigKey<Boolean> MASK_PAYLOADS =
      boolKey("ratchet.security.mask-payloads", "RATCHET_MASK_PAYLOADS", false);
  public static final RatchetConfigKey<RatchetOptions.IsolationCheckMode> ISOLATION_CHECK_MODE =
      new RatchetConfigKey<>(
          "ratchet.isolation-check",
          "RATCHET_ISOLATION_CHECK_MODE",
          RatchetOptions.IsolationCheckMode.FAIL,
          raw -> RatchetOptions.IsolationCheckMode.valueOf(raw.trim().toUpperCase(Locale.ROOT)));
  public static final RatchetConfigKey<Integer> PRIORITY_BOOST_INTERVAL_MINUTES =
      intKey(
          "ratchet.priority-boost-interval-minutes",
          "RATCHET_PRIORITY_BOOST_INTERVAL_MINUTES",
          15,
          0);
  public static final RatchetConfigKey<Boolean> CIRCUIT_BREAKER_ENABLED =
      boolKey("ratchet.circuit-breaker.enabled", "RATCHET_CIRCUIT_BREAKER_ENABLED", true);

  private RatchetConfigKeys() {}

  public static RatchetConfigKey<Integer> virtualThreadLimit(String type) {
    String normalized = type.toUpperCase(Locale.ROOT);
    return intKey(
        "ratchet.virtual-thread-limit." + normalized.toLowerCase(Locale.ROOT).replace('_', '-'),
        "RATCHET_VIRTUAL_THREAD_LIMIT_" + normalized,
        0,
        0);
  }

  public static RatchetConfigKey<Integer> rateLimitPerMinute(String type) {
    String normalized = type.toUpperCase(Locale.ROOT);
    return intKey(
        "ratchet.rate-limit-per-minute." + normalized.toLowerCase(Locale.ROOT).replace('_', '-'),
        "RATCHET_RATE_LIMIT_PER_MINUTE_" + normalized,
        0,
        0);
  }

  public static RatchetConfigKey<Float> circuitBreakerFailureRate(
      String profile, float defaultValue) {
    String segment = profileSegment(profile);
    return RatchetConfigKey.floatingRange(
        "ratchet.circuit-breaker." + propertySegment(profile) + ".failure-rate",
        "RATCHET_CB_" + segment + "_FAILURE_RATE",
        defaultValue,
        0.0f,
        100.0f);
  }

  public static RatchetConfigKey<Integer> circuitBreakerWindowSize(
      String profile, int defaultValue) {
    String segment = profileSegment(profile);
    return intKey(
        "ratchet.circuit-breaker." + propertySegment(profile) + ".window-size",
        "RATCHET_CB_" + segment + "_WINDOW_SIZE",
        defaultValue,
        1);
  }

  public static RatchetConfigKey<Long> circuitBreakerWaitMs(String profile, long defaultValue) {
    String segment = profileSegment(profile);
    return longKey(
        "ratchet.circuit-breaker." + propertySegment(profile) + ".wait-ms",
        "RATCHET_CB_" + segment + "_WAIT_MS",
        defaultValue,
        0L);
  }

  public static RatchetConfigKey<Integer> circuitBreakerHalfOpenCalls(
      String profile, int defaultValue) {
    String segment = profileSegment(profile);
    return intKey(
        "ratchet.circuit-breaker." + propertySegment(profile) + ".half-open-calls",
        "RATCHET_CB_" + segment + "_HALF_OPEN_CALLS",
        defaultValue,
        1);
  }

  public static RatchetConfigKey<Integer> circuitBreakerMinimumCalls(
      String profile, int defaultValue) {
    String segment = profileSegment(profile);
    return intKey(
        "ratchet.circuit-breaker." + propertySegment(profile) + ".minimum-calls",
        "RATCHET_CB_" + segment + "_MIN_CALLS",
        defaultValue,
        1);
  }

  private static RatchetConfigKey<Boolean> boolKey(String name, String env, boolean defaultValue) {
    return RatchetConfigKey.bool(name, env, defaultValue);
  }

  private static RatchetConfigKey<Integer> intKey(
      String name, String env, int defaultValue, int minInclusive) {
    return RatchetConfigKey.integerAtLeast(name, env, defaultValue, minInclusive);
  }

  private static RatchetConfigKey<Long> longKey(
      String name, String env, long defaultValue, long minInclusive) {
    return RatchetConfigKey.longAtLeast(name, env, defaultValue, minInclusive);
  }

  private static RatchetConfigKey<String> stringKey(String name, String env, String defaultValue) {
    return RatchetConfigKey.string(name, env, defaultValue);
  }

  private static String profileSegment(String profile) {
    return profile;
  }

  private static String propertySegment(String profile) {
    return profile.toLowerCase(Locale.ROOT).replace('_', '-');
  }
}

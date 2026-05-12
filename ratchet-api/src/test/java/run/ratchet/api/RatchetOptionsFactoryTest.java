package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.RatchetConfigSource;

class RatchetOptionsFactoryTest {

  @Test
  void usesEnvironmentVariableNameBeforePropertyNameWithinSource() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(
                Map.of("ratchet.poller.batch-size", "456"),
                Map.of("RATCHET_POLLER_BATCH_SIZE", "123")));

    assertEquals(123, options.polling().batchSize());
  }

  @Test
  void mapsNodeOrphanScanIntervalEnvironmentVariable() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(
                Map.of(), Map.of("RATCHET_NODE_ORPHAN_SCAN_INTERVAL_MINUTES", "17")));

    assertEquals(17L, options.node().orphanScanIntervalMinutes());
  }

  @Test
  void fallsBackToPropertyNameWhenEnvironmentVariableNameIsMissing() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(
                Map.of("ratchet.worker.use-virtual-threads", "true"), Map.of()));

    assertTrue(options.execution().useVirtualThreads());
  }

  @Test
  void usesFirstSourceThatReturnsValueBeforeLaterSources() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(Map.of("ratchet.poller.batch-size", "77"), Map.of()),
            new MapRatchetConfigSource(Map.of(), Map.of("RATCHET_POLLER_BATCH_SIZE", "123")));

    assertEquals(77, options.polling().batchSize());
  }

  @Test
  void preservesDefaultsWhenSourceChainHasNoValues() {
    RatchetOptions defaults = RatchetOptions.defaults();
    RatchetOptions options = optionsFrom((propertyName, environmentVariable) -> Optional.empty());

    assertMatchesDefaults(defaults, options);
  }

  @Test
  void fromEnvironmentTreatsNullAdditionalArrayAsNoOverlays() {
    RatchetOptions options = RatchetOptionsFactory.fromEnvironment((RatchetConfigSource[]) null);

    assertMatchesDefaults(RatchetOptions.defaults(), options);
  }

  @Test
  void mapsRepresentativeConfigurationKeysAcrossOptionGroups() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(
                Map.ofEntries(
                    Map.entry("ratchet.poller.burst-delay-ms", "11"),
                    Map.entry("ratchet.poller.min-delay-ms", "22"),
                    Map.entry("ratchet.poller.max-delay-ms", "33"),
                    Map.entry("ratchet.poller.deep-idle-delay-ms", "44"),
                    Map.entry("ratchet.poller.deep-idle-threshold-ms", "55"),
                    Map.entry("ratchet.poller.idle-threshold", "6"),
                    Map.entry("ratchet.poller.claim-headroom-factor", "7"),
                    Map.entry("ratchet.thread-pool.queue-size", "88"),
                    Map.entry("ratchet.thread-pool.size.workflow-join", "9"),
                    Map.entry("ratchet.virtual-thread-limit.workflow-join", "10"),
                    Map.entry("ratchet.rate-limit-per-minute.workflow-join", "11"),
                    Map.entry("ratchet.node.id", "node-a"),
                    Map.entry("ratchet.node.heartbeat-interval-seconds", "12"),
                    Map.entry("ratchet.node.orphan-grace-seconds", "13"),
                    Map.entry("ratchet.node.orphan-scan-interval-minutes", "14"),
                    Map.entry("ratchet.node.dynamic-heartbeat-enabled", "false"),
                    Map.entry("ratchet.recurring.batch-limit", "15"),
                    Map.entry("ratchet.recurring.poll-ms", "16"),
                    Map.entry("ratchet.recurring.max-poll-ms", "17"),
                    Map.entry("ratchet.recurring.startup-grace-seconds", "18"),
                    Map.entry("ratchet.recurring.convergence-window-seconds", "19"),
                    Map.entry("ratchet.retry-buffer.drain-interval-ms", "200"),
                    Map.entry("ratchet.timeout.soft-timeout-percent", "42"),
                    Map.entry("ratchet.timeout.default-sla-seconds", "21"),
                    Map.entry("ratchet.dlq.purge-enabled", "false"),
                    Map.entry("ratchet.dlq.purge-cron", "0 0 3 * * ?"),
                    Map.entry("ratchet.dlq.purge-days", "22"),
                    Map.entry("ratchet.jobs.archive-enabled", "false"),
                    Map.entry("ratchet.jobs.archive-cron", "0 0 4 * * ?"),
                    Map.entry("ratchet.jobs.retention-days", "23"),
                    Map.entry("ratchet.jobs.archive-batch-size", "24"),
                    Map.entry("ratchet.logs.purge-enabled", "false"),
                    Map.entry("ratchet.logs.purge-cron", "0 0 5 * * ?"),
                    Map.entry("ratchet.logs.retention-days", "25"),
                    Map.entry("ratchet.notifications.enabled", "false"),
                    Map.entry("ratchet.notifications.dlq-alert-channel", "#dlq"),
                    Map.entry("ratchet.notifications.timeout-alert-channel", "#timeout"),
                    Map.entry("ratchet.schema.auto-migrate", "true"),
                    Map.entry("ratchet.schema.migration-dialect", "postgresql"),
                    Map.entry("ratchet.schema.migration-prefix", "ddl/custom"),
                    Map.entry("ratchet.payload.max-payload-kb", "26"),
                    Map.entry("ratchet.jobs.max-result-bytes", "27"),
                    Map.entry("ratchet.metrics.clustering", "node"),
                    Map.entry("ratchet.security.redact-emails", "false"),
                    Map.entry("ratchet.priority-boost-interval-minutes", "28"),
                    Map.entry("ratchet.circuit-breaker.enabled", "false"),
                    Map.entry("ratchet.circuit-breaker.default.window-size", "29"),
                    Map.entry("ratchet.circuit-breaker.default.half-open-calls", "30"),
                    Map.entry("ratchet.circuit-breaker.default.minimum-calls", "31")),
                Map.of()));

    assertEquals(11L, options.polling().burstDelayMs());
    assertEquals(22L, options.polling().minDelayMs());
    assertEquals(33L, options.polling().maxDelayMs());
    assertEquals(44L, options.polling().deepIdleDelayMs());
    assertEquals(55L, options.polling().deepIdleThresholdMs());
    assertEquals(6, options.polling().idleThreshold());
    assertEquals(7, options.polling().claimHeadroomFactor());
    assertEquals(88, options.execution().queueSize());
    assertEquals(9, options.execution().maxConcurrency("WORKFLOW_JOIN", 0));
    assertEquals(10, options.execution().virtualThreadLimit("WORKFLOW_JOIN", 0));
    assertEquals(11, options.execution().rateLimitPerMinute("WORKFLOW_JOIN"));
    assertEquals(Optional.of("node-a"), options.node().explicitNodeId());
    assertEquals(12L, options.node().heartbeatIntervalSeconds());
    assertEquals(13L, options.node().orphanGraceSeconds());
    assertEquals(14L, options.node().orphanScanIntervalMinutes());
    assertFalse(options.node().dynamicHeartbeatEnabled());
    assertEquals(15, options.recurring().batchLimit());
    assertEquals(16L, options.recurring().pollMs());
    assertEquals(17L, options.recurring().maxPollMs());
    assertEquals(18L, options.recurring().startupGraceSeconds());
    assertEquals(19L, options.recurring().convergenceWindowSeconds());
    assertEquals(200L, options.retryBuffer().drainIntervalMs());
    assertEquals(42, options.timeout().softTimeoutPercent());
    assertEquals(21L, options.timeout().defaultSlaSeconds());
    assertFalse(options.maintenance().dlqPurgeEnabled());
    assertEquals("0 0 3 * * ?", options.maintenance().dlqPurgeCron());
    assertEquals(22L, options.maintenance().dlqPurgeDays());
    assertFalse(options.maintenance().jobArchiveEnabled());
    assertEquals("0 0 4 * * ?", options.maintenance().jobArchiveCron());
    assertEquals(23L, options.maintenance().jobRetentionDays());
    assertEquals(24, options.maintenance().jobArchiveBatchSize());
    assertFalse(options.maintenance().logPurgeEnabled());
    assertEquals("0 0 5 * * ?", options.maintenance().logPurgeCron());
    assertEquals(25L, options.maintenance().logRetentionDays());
    assertFalse(options.notifications().enabled());
    assertEquals("#dlq", options.notifications().dlqAlertChannel());
    assertEquals("#timeout", options.notifications().timeoutAlertChannel());
    assertTrue(options.schema().autoMigrate());
    assertEquals("postgresql", options.schema().migrationDialect());
    assertEquals("ddl/custom", options.schema().migrationPrefix());
    assertEquals(26, options.payload().maxPayloadKb());
    assertEquals(27L, options.payload().maxResultBytes());
    assertEquals("node", options.metrics().clustering());
    assertFalse(options.security().redactEmails());
    assertEquals(28, options.store().priorityBoostIntervalMinutes());
    assertFalse(options.circuitBreaker().enabled());
    assertEquals(
        29, options.circuitBreaker().profile(CircuitBreakerProfile.DEFAULT).slidingWindowSize());
    assertEquals(
        30,
        options.circuitBreaker().profile(CircuitBreakerProfile.DEFAULT).permittedCallsInHalfOpen());
    assertEquals(
        31, options.circuitBreaker().profile(CircuitBreakerProfile.DEFAULT).minimumCalls());
  }

  @Test
  void maintenanceCronEnvNamesUseActionNouns() {
    assertEquals(
        "RATCHET_JOB_ARCHIVE_CRON", RatchetConfigKeys.JOB_ARCHIVE_CRON.environmentVariable());
    assertEquals("RATCHET_LOG_PURGE_CRON", RatchetConfigKeys.LOG_PURGE_CRON.environmentVariable());
  }

  @Test
  void skipsSourceThatThrowsAndUsesNextSource() {
    RatchetOptions options =
        optionsFrom(
            (propertyName, environmentVariable) -> {
              if ("ratchet.poller.batch-size".equals(propertyName)) {
                throw new IllegalStateException("source down");
              }
              return Optional.empty();
            },
            new MapRatchetConfigSource(Map.of("ratchet.poller.batch-size", "88"), Map.of()));

    assertEquals(88, options.polling().batchSize());
  }

  @Test
  void treatsNullOptionalFromSourceAsAbsent() {
    RatchetOptions options =
        optionsFrom(
            (propertyName, environmentVariable) -> null,
            new MapRatchetConfigSource(Map.of("ratchet.poller.batch-size", "89"), Map.of()));

    assertEquals(89, options.polling().batchSize());
  }

  @Test
  void emptyStringFallsBackToTypedDefault() {
    RatchetOptions options =
        optionsFrom(new MapRatchetConfigSource(Map.of("ratchet.poller.batch-size", ""), Map.of()));

    assertEquals(RatchetOptions.defaults().polling().batchSize(), options.polling().batchSize());
  }

  @Test
  void unparseableNumericValueFallsBackToTypedDefault() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(Map.of("ratchet.poller.batch-size", "abc"), Map.of()));

    assertEquals(RatchetOptions.defaults().polling().batchSize(), options.polling().batchSize());
  }

  @Test
  void enumConfigValuesAreCaseInsensitive() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(Map.of("ratchet.isolation-check", "warn"), Map.of()));

    assertEquals(RatchetOptions.IsolationCheckMode.WARN, options.store().isolationCheckMode());
  }

  @Test
  void strictBooleanConfigRejectsNonBooleanAndFallsBackToDefault() {
    RatchetOptions options =
        optionsFrom(
            new MapRatchetConfigSource(
                Map.of("ratchet.worker.use-virtual-threads", "sometimes"), Map.of()));

    assertFalse(options.execution().useVirtualThreads());
  }

  @Test
  void fromEnvironmentReadsSystemPropertiesWithoutAdditionalSources() {
    String property = "ratchet.poller.batch-size";
    String previous = System.getProperty(property);
    try {
      System.setProperty(property, "321");

      RatchetOptions options = RatchetOptionsFactory.fromEnvironment();

      assertEquals(321, options.polling().batchSize());
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }

  private static RatchetOptions optionsFrom(RatchetConfigSource... sources) {
    return RatchetOptionsFactory.from(new DefaultRatchetConfig(List.of(sources)));
  }

  private static void assertMatchesDefaults(RatchetOptions defaults, RatchetOptions options) {
    assertEquals(defaults.polling(), options.polling());
    assertEquals(defaults.execution(), options.execution());
    assertEquals(defaults.node(), options.node());
    assertEquals(defaults.recurring(), options.recurring());
    assertEquals(defaults.retryBuffer(), options.retryBuffer());
    assertEquals(defaults.timeout(), options.timeout());
    assertEquals(defaults.maintenance(), options.maintenance());
    assertEquals(defaults.notifications(), options.notifications());
    assertEquals(defaults.schema(), options.schema());
    assertEquals(defaults.payload(), options.payload());
    assertEquals(defaults.metrics(), options.metrics());
    assertEquals(defaults.security(), options.security());
    assertEquals(defaults.store(), options.store());
    assertEquals(defaults.circuitBreaker(), options.circuitBreaker());
  }

  private record MapRatchetConfigSource(
      Map<String, String> properties, Map<String, String> environment)
      implements RatchetConfigSource {

    @Override
    public Optional<String> get(String propertyName, String environmentVariable) {
      return value(environment, environmentVariable).or(() -> value(properties, propertyName));
    }

    private static Optional<String> value(Map<String, String> values, String key) {
      if (key == null) {
        return Optional.empty();
      }
      return Optional.ofNullable(values.get(key));
    }
  }
}

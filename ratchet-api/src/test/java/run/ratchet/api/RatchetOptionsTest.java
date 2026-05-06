package run.ratchet.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RatchetOptionsTest {

  @Test
  void defaultsMatchRuntimeDefaults() {
    RatchetOptions options = RatchetOptions.defaults();

    assertEquals(50, options.polling().batchSize());
    assertEquals(20, options.execution().maxConcurrency("SINGLE", -1));
    assertEquals(5, options.execution().maxConcurrency("RECURRING", -1));
    assertFalse(options.execution().useVirtualThreads());
    assertEquals(60L, options.recurring().startupGraceSeconds());
    assertEquals(500, options.timeout().signalTimeoutBatchSize());
    assertEquals(RatchetOptions.IsolationCheckMode.FAIL, options.store().isolationCheckMode());
    assertTrue(options.security().redactEmails());
  }

  @Test
  void builderAppliesGroupedCustomizations() {
    RatchetOptions options =
        RatchetOptions.builder()
            .polling(polling -> polling.batchSize(100).claimHeadroomFactor(2))
            .execution(
                execution ->
                    execution
                        .useVirtualThreads(true)
                        .maxConcurrency("dlq-alert", 4)
                        .virtualThreadLimit("workflow-join", 19)
                        .rateLimitPerMinute("single", 50))
            .recurring(recurring -> recurring.batchLimit(40))
            .timeout(timeout -> timeout.signalTimeoutBatchSize(25))
            .security(security -> security.allowEmptyClassPolicy(true).redactEmails(false))
            .store(
                store ->
                    store
                        .isolationCheckMode(RatchetOptions.IsolationCheckMode.WARN)
                        .priorityBoostIntervalMinutes(0))
            .build();

    assertEquals(100, options.polling().batchSize());
    assertEquals(2, options.polling().claimHeadroomFactor());
    assertTrue(options.execution().useVirtualThreads());
    assertEquals(4, options.execution().maxConcurrency("DLQ_ALERT", -1));
    assertEquals(19, options.execution().virtualThreadLimit("WORKFLOW_JOIN", -1));
    assertEquals(50, options.execution().rateLimitPerMinute("SINGLE"));
    assertEquals(40, options.recurring().batchLimit());
    assertEquals(25, options.timeout().signalTimeoutBatchSize());
    assertTrue(options.security().allowEmptyClassPolicy());
    assertFalse(options.security().redactEmails());
    assertEquals(RatchetOptions.IsolationCheckMode.WARN, options.store().isolationCheckMode());
    assertEquals(0, options.store().priorityBoostIntervalMinutes());
  }

  @Test
  void builderRejectsInvalidValues() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().polling(polling -> polling.batchSize(0)));
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().timeout(timeout -> timeout.softTimeoutPercent(100)));
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().timeout(timeout -> timeout.signalTimeoutBatchSize(0)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RatchetOptions.builder()
                .circuitBreaker(
                    circuitBreaker ->
                        circuitBreaker.profile(
                            CircuitBreakerProfile.DEFAULT,
                            profile -> profile.failureRateThreshold(101.0f))));
  }
}

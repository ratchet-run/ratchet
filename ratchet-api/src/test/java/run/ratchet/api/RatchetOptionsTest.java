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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.CallerPrincipalResolver;

class RatchetOptionsTest {

  @Test
  void publicOptionGroupsMatchRuntimeBackedSurface() {
    Set<String> optionGroups =
        Arrays.stream(RatchetOptions.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getParameterCount() == 0)
            .filter(method -> method.getReturnType().getEnclosingClass() == RatchetOptions.class)
            .filter(method -> method.getReturnType().getSimpleName().endsWith("Options"))
            .map(method -> method.getName())
            .collect(Collectors.toUnmodifiableSet());

    assertEquals(
        Set.of(
            "circuitBreaker",
            "encryption",
            "execution",
            "maintenance",
            "node",
            "payload",
            "polling",
            "recurring",
            "retryBuffer",
            "schema",
            "security",
            "store",
            "timeout"),
        optionGroups);
  }

  @Test
  void defaultsMatchRuntimeDefaults() {
    RatchetOptions options = RatchetOptions.defaults();

    assertEquals(50, options.polling().batchSize());
    assertEquals(20, options.execution().maxConcurrency("SINGLE", -1));
    assertEquals(5, options.execution().maxConcurrency("RECURRING", -1));
    assertEquals(RatchetOptions.ThreadingMode.PLATFORM, options.execution().defaultThreadingMode());
    assertFalse(options.execution().hasVirtualExecutor());
    assertEquals("java:comp/DefaultManagedExecutorService", options.execution().jobExecutorJndi());
    assertEquals(
        "java:comp/DefaultManagedScheduledExecutorService",
        options.execution().scheduledExecutorJndi());
    assertEquals(
        "java:comp/DefaultManagedThreadFactory",
        options.execution().coordinatorThreadFactoryJndi());
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
                        .defaultThreadingMode(RatchetOptions.ThreadingMode.VIRTUAL)
                        .coordinatorThreadFactoryJndi(
                            "java:jboss/ee/concurrency/factory/RatchetCoordinator")
                        .maxConcurrency("workflow-branch", 4)
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
    assertEquals(RatchetOptions.ThreadingMode.VIRTUAL, options.execution().defaultThreadingMode());
    assertEquals(
        "java:jboss/ee/concurrency/factory/RatchetCoordinator",
        options.execution().coordinatorThreadFactoryJndi());
    assertEquals(4, options.execution().maxConcurrency("WORKFLOW_BRANCH", -1));
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
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RatchetOptions.builder()
                .polling(polling -> polling.minDelayMs(5000).maxDelayMs(1))
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RatchetOptions.builder()
                .recurring(recurring -> recurring.pollMs(5000).maxPollMs(1))
                .build());
  }

  @Test
  void circuitBreakerFailureRateAcceptsBoundaries() {
    RatchetOptions zero =
        RatchetOptions.builder()
            .circuitBreaker(
                circuitBreaker ->
                    circuitBreaker.profile(
                        CircuitBreakerProfile.DEFAULT,
                        profile -> profile.failureRateThreshold(0.0f)))
            .build();
    RatchetOptions hundred =
        RatchetOptions.builder()
            .circuitBreaker(
                circuitBreaker ->
                    circuitBreaker.profile(
                        CircuitBreakerProfile.DEFAULT,
                        profile -> profile.failureRateThreshold(100.0f)))
            .build();

    assertEquals(
        0.0f, zero.circuitBreaker().profile(CircuitBreakerProfile.DEFAULT).failureRateThreshold());
    assertEquals(
        100.0f,
        hundred.circuitBreaker().profile(CircuitBreakerProfile.DEFAULT).failureRateThreshold());
  }

  @Test
  void circuitBreakerFailureRateRejectsInvalidBoundaries() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RatchetOptions.builder()
                .circuitBreaker(
                    circuitBreaker ->
                        circuitBreaker.profile(
                            CircuitBreakerProfile.DEFAULT,
                            profile -> profile.failureRateThreshold(-1.0f))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RatchetOptions.builder()
                .circuitBreaker(
                    circuitBreaker ->
                        circuitBreaker.profile(
                            CircuitBreakerProfile.DEFAULT,
                            profile -> profile.failureRateThreshold(Float.NaN))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RatchetOptions.builder()
                .circuitBreaker(
                    circuitBreaker ->
                        circuitBreaker.profile(
                            CircuitBreakerProfile.DEFAULT,
                            profile -> profile.failureRateThreshold(Float.POSITIVE_INFINITY))));
  }

  @Test
  void toBuilderRoundTripsFullyPopulatedInstance() {
    CallerPrincipalResolver resolver = () -> Optional.of("round-trip-user");
    RatchetOptions original =
        RatchetOptions.builder()
            .polling(
                polling ->
                    polling
                        .batchSize(51)
                        .burstDelayMs(501L)
                        .minDelayMs(2001L)
                        .maxDelayMs(10001L)
                        .deepIdleDelayMs(30001L)
                        .deepIdleThresholdMs(60001L)
                        .idleThreshold(4)
                        .claimHeadroomFactor(1))
            .execution(
                execution ->
                    execution
                        .defaultThreadingMode(RatchetOptions.ThreadingMode.VIRTUAL)
                        .queueSize(101)
                        .maxConcurrency("SINGLE", 21)
                        .virtualThreadLimit("RECURRING", 22)
                        .rateLimitPerMinute("BATCH_CHILD", 23)
                        .jobExecutorJndi("java:app/concurrent/JobExecutor")
                        .scheduledExecutorJndi("java:app/concurrent/ScheduledExecutor")
                        .coordinatorThreadFactoryJndi("java:app/concurrent/CoordinatorFactory")
                        .virtualExecutorJndi("java:app/concurrent/VirtualExecutor")
                        .virtualCounterAccounting(true))
            .node(
                node ->
                    node.nodeId("node-a")
                        .heartbeatIntervalSeconds(11L)
                        .orphanGraceSeconds(61L)
                        .orphanScanIntervalMinutes(6L)
                        .dynamicHeartbeatEnabled(false)
                        .requireTags("blue", "primary")
                        .excludeTags("draining"))
            .recurring(
                recurring ->
                    recurring
                        .batchLimit(21)
                        .pollMs(1001L)
                        .maxPollMs(60001L)
                        .startupGraceSeconds(61L)
                        .convergenceWindowSeconds(1L))
            .retryBuffer(retryBuffer -> retryBuffer.drainIntervalMs(1001L))
            .timeout(
                timeout ->
                    timeout
                        .softTimeoutPercent(81)
                        .defaultSlaSeconds(1801L)
                        .signalTimeoutBatchSize(501))
            .maintenance(
                maintenance ->
                    maintenance
                        .dlqPurgeEnabled(false)
                        .dlqPurgeCron("0 1 2 * * ?")
                        .dlqPurgeDays(91L)
                        .jobArchiveEnabled(false)
                        .jobArchiveCron("0 1 1 * * ?")
                        .jobRetentionDays(91L)
                        .jobArchiveBatchSize(1001)
                        .logPurgeEnabled(false)
                        .logPurgeCron("0 31 2 * * ?")
                        .logRetentionDays(31L))
            .schema(
                schema ->
                    schema
                        .autoMigrate(true)
                        .migrationDialect("postgresql")
                        .migrationPrefix("ddl/custom"))
            .payload(payload -> payload.maxPayloadKb(101).maxResultBytes(65537L))
            .security(
                security ->
                    security.allowEmptyClassPolicy(true).redactEmails(false).maskPayloads(true))
            .store(
                store ->
                    store
                        .isolationCheckMode(RatchetOptions.IsolationCheckMode.WARN)
                        .priorityBoostIntervalMinutes(16))
            .circuitBreaker(
                circuitBreaker ->
                    circuitBreaker
                        .enabled(false)
                        .profile(
                            CircuitBreakerProfile.DEFAULT,
                            profile ->
                                profile
                                    .failureRateThreshold(51.0f)
                                    .slidingWindowSize(101)
                                    .waitDurationMs(30001L)
                                    .permittedCallsInHalfOpen(4)
                                    .minimumCalls(6)))
            .encryption(encryption -> encryption.enabled(true).writeAlgorithm("AES/GCM/NoPadding"))
            .callerPrincipalResolver(resolver)
            .build();

    RatchetOptions roundTripped = original.toBuilder().build();

    assertEquals(original.polling(), roundTripped.polling());
    assertEquals(original.execution(), roundTripped.execution());
    assertEquals(original.node(), roundTripped.node());
    assertEquals(original.recurring(), roundTripped.recurring());
    assertEquals(original.retryBuffer(), roundTripped.retryBuffer());
    assertEquals(original.timeout(), roundTripped.timeout());
    assertEquals(original.maintenance(), roundTripped.maintenance());
    assertEquals(original.schema(), roundTripped.schema());
    assertEquals(original.payload(), roundTripped.payload());
    assertEquals(original.security(), roundTripped.security());
    assertEquals(original.store(), roundTripped.store());
    assertEquals(original.circuitBreaker(), roundTripped.circuitBreaker());
    assertEquals(original.encryption(), roundTripped.encryption());
    assertSame(original.callerPrincipalResolver(), roundTripped.callerPrincipalResolver());
  }

  @Test
  void toBuilderPreservesSubsetMapsWithoutDefaultLeakBack() {
    Map<String, Integer> maxConcurrency = Map.of("SINGLE", 2, "RECURRING", 3);
    RatchetOptions.ExecutionOptions execution =
        new RatchetOptions.ExecutionOptions(
            RatchetOptions.ThreadingMode.PLATFORM,
            17,
            maxConcurrency,
            Map.of(),
            Map.of(),
            "java:app/concurrent/JobExecutor",
            "java:app/concurrent/ScheduledExecutor",
            "java:app/concurrent/CoordinatorFactory",
            null,
            false);
    RatchetOptions.CircuitBreakerProfileOptions profile =
        new RatchetOptions.CircuitBreakerProfileOptions(40.0f, 10, 5000L, 2, 3);
    RatchetOptions.CircuitBreakerOptions circuitBreaker =
        new RatchetOptions.CircuitBreakerOptions(true, Map.of(CircuitBreakerProfile.FAST, profile));
    RatchetOptions defaults = RatchetOptions.defaults();
    RatchetOptions original =
        new RatchetOptions(
            defaults.polling(),
            execution,
            defaults.node(),
            defaults.recurring(),
            defaults.retryBuffer(),
            defaults.timeout(),
            defaults.maintenance(),
            defaults.schema(),
            defaults.payload(),
            defaults.security(),
            defaults.store(),
            circuitBreaker,
            defaults.encryption());

    RatchetOptions roundTripped = original.toBuilder().build();

    assertEquals(maxConcurrency, roundTripped.execution().maxConcurrency());
    assertEquals(Map.of(), roundTripped.execution().virtualThreadLimits());
    assertEquals(Map.of(), roundTripped.execution().rateLimitsPerMinute());
    assertEquals(
        Map.of(CircuitBreakerProfile.FAST, profile), roundTripped.circuitBreaker().profiles());
  }

  @Test
  void toBuilderModificationsDoNotAffectOriginal() {
    RatchetOptions original =
        RatchetOptions.builder().polling(polling -> polling.batchSize(77)).build();
    RatchetOptions.PollingOptions originalPolling = original.polling();
    RatchetOptions.Builder builder = original.toBuilder();

    builder.polling(polling -> polling.batchSize(88));
    RatchetOptions modified = builder.build();

    assertEquals(originalPolling, original.polling());
    assertEquals(77, original.polling().batchSize());
    assertEquals(88, modified.polling().batchSize());
  }

  @Test
  void pollingOptionsRejectsMinDelayGreaterThanMaxDelay() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RatchetOptions.PollingOptions(1, 0L, 2L, 1L, 0L, 0L, 0, 0));
  }

  @Test
  void recurringOptionsRejectsPollGreaterThanMaxPoll() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RatchetOptions.RecurringOptions(1, 2L, 1L, 0L, 0L));
  }

  @Test
  void withCallerPrincipalResolverCopiesEveryOtherFieldAndLeavesOriginalUnchanged() {
    RatchetOptions original =
        RatchetOptions.builder()
            .node(node -> node.heartbeatIntervalSeconds(42L))
            .polling(polling -> polling.batchSize(77))
            .security(security -> security.maskPayloads(true))
            .execution(execution -> execution.jobExecutorJndi("java:app/concurrent/CustomExecutor"))
            .build();
    CallerPrincipalResolver resolver = () -> Optional.of("test-user");

    RatchetOptions withResolver = original.withCallerPrincipalResolver(resolver);

    assertSame(resolver, withResolver.callerPrincipalResolver());
    assertEquals(42L, withResolver.node().heartbeatIntervalSeconds());
    assertEquals(77, withResolver.polling().batchSize());
    assertTrue(withResolver.security().maskPayloads());
    assertEquals("java:app/concurrent/CustomExecutor", withResolver.execution().jobExecutorJndi());
    assertNull(original.callerPrincipalResolver());
  }

  @Test
  void nestedBuildersRejectBlankTextAndInvalidMinimums() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().node(node -> node.nodeId(" ")));
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().retryBuffer(retryBuffer -> retryBuffer.drainIntervalMs(49)));
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().maintenance(maintenance -> maintenance.dlqPurgeCron(" ")));
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().maintenance(maintenance -> maintenance.jobArchiveCron(" ")));
    assertThrows(
        IllegalArgumentException.class,
        () -> RatchetOptions.builder().maintenance(maintenance -> maintenance.logPurgeCron(" ")));
  }
}

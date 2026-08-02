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
package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.RecurringMisfirePolicy;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringMethodRegistrar;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.ri.runtime.RecurringMethodDiscovery;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;

// Verifies startup-lease-gated cleanup and convergence behavior in the portable registrar.
class RecurringJobProcessorLeaderGateTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);
  private static final RecurringMethodDiscovery LEADER_GATE_DISCOVERY =
      () -> Set.of(LeaderGateBean.class);
  private static final RecurringMethodDiscovery EMPTY_DISCOVERY = Set::of;

  @Test
  void cleanup_skippedWhenStartupLeaseNotAcquired() {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    var invocationSubmissionService = mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var coordinator = mock(StartupCoordinator.class);
    when(coordinator.tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5)))
        .thenReturn(false);
    var registrationState = new RecurringRegistrationState();
    var registrar =
        newRegistrar(
            invocationSubmissionService,
            maintenance,
            LEADER_GATE_DISCOVERY,
            coordinator,
            registrationState,
            RatchetOptions.defaults(),
            null,
            FIXED_CLOCK);

    registrar.register();

    ArgumentCaptor<JobInvocation> invocationCaptor = ArgumentCaptor.forClass(JobInvocation.class);
    verify(invocationSubmissionService)
        .scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), invocationCaptor.capture());
    assertEquals(
        new JobInvocation(
            RecurringMethodInvoker.class.getName(),
            "invoke",
            "(Ljava/lang/String;Ljava/lang/String;Z)V",
            false,
            List.of(LeaderGateBean.class.getName(), "run", false)),
        invocationCaptor.getValue());
    verify(recurringJobBuilder).withBusinessKey("leader-gate-job");
    verify(recurringJobBuilder).submit();
    assertTrue(registrationState.shouldFire("leader-gate-job"));
    assertFalse(registrationState.shouldFire("unknown-recurring-job"));
    verify(maintenance, never()).cancelOrphanedRecurringAnnotationJobs(anySet(), any());
    verify(coordinator, never()).release("recurring-annotation-orphan-cleanup");
  }

  @Test
  void registerRecurringJobs_usesPortableDiscovery() {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    var invocationSubmissionService = mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    var discovery = mock(RecurringMethodDiscovery.class);
    when(discovery.recurringBeanClasses()).thenReturn(Set.of(LeaderGateBean.class));
    when(discovery.isMethodInvocable(eq(LeaderGateBean.class), any())).thenReturn(true);
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var registrar =
        newRegistrar(
            invocationSubmissionService,
            maintenance,
            discovery,
            null,
            new RecurringRegistrationState(),
            RatchetOptions.defaults(),
            null,
            FIXED_CLOCK);

    registrar.register();

    verify(discovery).recurringBeanClasses();
    verify(invocationSubmissionService)
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
  }

  @Test
  void cleanup_runsWhenStartupLeaseAcquired_andAppliesConvergenceWindow() {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    when(maintenance.cancelOrphanedRecurringAnnotationJobs(anySet(), any())).thenReturn(0);
    var coordinator = mock(StartupCoordinator.class);
    when(coordinator.tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5)))
        .thenReturn(true);
    var registrar =
        newRegistrar(
            mock(InvocationSubmissionService.class),
            maintenance,
            EMPTY_DISCOVERY,
            coordinator,
            new RecurringRegistrationState(),
            RatchetOptions.builder()
                .recurring(recurring -> recurring.convergenceWindowSeconds(120))
                .build(),
            null,
            FIXED_CLOCK);

    registrar.register();

    ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(anySet(), cutoffCaptor.capture());
    verify(coordinator).release("recurring-annotation-orphan-cleanup");
    assertEquals(FIXED_NOW.minusSeconds(120), cutoffCaptor.getValue());
  }

  @Test
  void cleanup_runsWhenStartupCoordinatorMissing() {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    when(maintenance.cancelOrphanedRecurringAnnotationJobs(anySet(), any())).thenReturn(0);
    var registrar =
        newRegistrar(
            mock(InvocationSubmissionService.class),
            maintenance,
            EMPTY_DISCOVERY,
            null,
            new RecurringRegistrationState(),
            RatchetOptions.defaults(),
            null,
            FIXED_CLOCK);

    registrar.register();

    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(anySet(), any());
  }

  @Test
  void cleanup_releasesLease_evenIfMaintenanceThrows() {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    when(maintenance.cancelOrphanedRecurringAnnotationJobs(anySet(), any()))
        .thenThrow(new IllegalStateException("store unavailable"));
    var coordinator = mock(StartupCoordinator.class);
    when(coordinator.tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5)))
        .thenReturn(true);
    var registrar =
        newRegistrar(
            mock(InvocationSubmissionService.class),
            maintenance,
            EMPTY_DISCOVERY,
            coordinator,
            new RecurringRegistrationState(),
            RatchetOptions.defaults(),
            null,
            FIXED_CLOCK);

    assertDoesNotThrow(registrar::register);

    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(anySet(), any());
    verify(coordinator).tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5));
    verify(coordinator).release("recurring-annotation-orphan-cleanup");
    verifyNoMoreInteractions(coordinator);
  }

  @Test
  void registerRecurringJobs_doesNotCancelExistingJobWhenSubmitFails() {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    var invocationSubmissionService = mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    when(recurringJobBuilder.submit()).thenThrow(new IllegalStateException("store unavailable"));
    var registrar =
        newRegistrar(
            invocationSubmissionService,
            maintenance,
            LEADER_GATE_DISCOVERY,
            null,
            new RecurringRegistrationState(),
            RatchetOptions.defaults(),
            null,
            FIXED_CLOCK);

    assertDoesNotThrow(registrar::register);

    verify(recurringJobBuilder).submit();
    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(eq(Set.of("leader-gate-job")), any());
  }

  @Test
  void registerRecurringJobs_submitsExistingBusinessKeySoDefinitionCanBeReconciled()
      throws Exception {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    var invocationSubmissionService = mock(InvocationSubmissionService.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    UUID existingId = UUID.randomUUID();
    when(recurringJobBuilder.submit()).thenReturn((JobHandle) () -> existingId);
    when(invocationSubmissionService.scheduleRecurringInvocation(
            eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var registrationState = new RecurringRegistrationState();
    var recurringJobStore = mock(RecurringJobStore.class);
    when(recurringJobStore.findRecurringByBusinessKey("leader-gate-job"))
        .thenReturn(Optional.of(recurringDefinition(existingId, "leader-gate-job")));
    var jobStore = mock(JobStore.class);
    when(jobStore.capability(RecurringJobStore.class)).thenReturn(Optional.of(recurringJobStore));
    var registrar =
        newRegistrar(
            invocationSubmissionService,
            maintenance,
            LEADER_GATE_DISCOVERY,
            null,
            registrationState,
            RatchetOptions.defaults(),
            jobStore,
            FIXED_CLOCK);

    registrar.register();

    verify(invocationSubmissionService)
        .scheduleRecurringInvocation(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).withBusinessKey("leader-gate-job");
    verify(recurringJobBuilder).submit();
    assertRegisteredJobId(registrar, "leader-gate-job", existingId);
    assertTrue(registrationState.shouldFire("leader-gate-job"));
    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(eq(Set.of("leader-gate-job")), any());
  }

  private static RecurringMethodRegistrar newRegistrar(
      InvocationSubmissionService invocationSubmissionService,
      RecurringAnnotationMaintenanceService maintenance,
      RecurringMethodDiscovery discovery,
      StartupCoordinator coordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options,
      JobStore jobStore,
      Clock clock) {
    return new RecurringMethodRegistrar(
        invocationSubmissionService,
        maintenance,
        discovery,
        mock(RecurringMethodInvoker.class),
        coordinator,
        registrationState,
        options,
        null,
        jobStore,
        clock);
  }

  static RecurringJobBuilder mockRecurringJobBuilder() {
    var builder = mock(RecurringJobBuilder.class);
    when(builder.withOptions(any())).thenReturn(builder);
    when(builder.withBusinessKey(any())).thenReturn(builder);
    when(builder.withTags(any())).thenReturn(builder);
    when(builder.submit()).thenReturn((JobHandle) () -> UUID.randomUUID());
    return builder;
  }

  static RecurringJobDefinition recurringDefinition(UUID id, String businessKey) {
    JobOptions options = JobOptions.defaults();
    return new RecurringJobDefinition(
        id,
        "0 0/5 * * * ?",
        "UTC",
        FIXED_NOW.plusSeconds(60),
        false,
        null,
        options.priority().persistedCode(),
        options.maxRetries(),
        options.backoffPolicy(),
        (int) options.backoffParam().toMillis(),
        options.timeoutSec(),
        JobPayloadFactory.noop(),
        null,
        null,
        businessKey,
        null,
        null,
        FIXED_NOW,
        null,
        false,
        RecurringMisfirePolicy.defaults());
  }

  @SuppressWarnings("unchecked")
  private static void assertRegisteredJobId(
      RecurringMethodRegistrar registrar, String businessKey, UUID jobId) throws Exception {
    Field field = RecurringMethodRegistrar.class.getDeclaredField("registeredJobIds");
    field.setAccessible(true);
    Map<String, String> registeredJobIds = (Map<String, String>) field.get(registrar);
    assertEquals(jobId.toString(), registeredJobIds.get(businessKey));
  }

  static class LeaderGateBean {
    @Recurring(id = "leader-gate-job", cron = "0 0/5 * * * ?")
    public void run() {}
  }
}

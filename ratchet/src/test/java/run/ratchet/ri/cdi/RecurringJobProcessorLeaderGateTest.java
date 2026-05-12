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

import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.ri.core.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.RecurringRegistrationState;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.JobBatchStatusStore;

// Verifies startup-lease-gated cleanup and convergence window in RecurringJobProcessor.
class RecurringJobProcessorLeaderGateTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Test
  void cleanup_skippedWhenStartupLeaseNotAcquired() throws Exception {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    var schedulerService = mock(JobSchedulerService.class);
    var jobBatchStatusStore = mock(JobBatchStatusStore.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    var beanManager = mock(BeanManager.class);
    Set<Bean<?>> beans = Set.of(beanFor(LeaderGateBean.class));
    when(beanManager.getBeans(any(), any())).thenReturn(beans);
    when(schedulerService.scheduleRecurring(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);
    var coordinator = mock(StartupCoordinator.class);
    when(coordinator.tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5)))
        .thenReturn(false);
    var registrationState = new RecurringRegistrationState();

    var processor =
        new RecurringJobProcessor(
            schedulerService,
            jobBatchStatusStore,
            maintenance,
            beanManager,
            mock(RecurringMethodInvoker.class),
            coordinator,
            registrationState);

    processor.registerRecurringJobs();

    verify(schedulerService).scheduleRecurring(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(jobBatchStatusStore).cancelRecurringJobsByBusinessKeys(Set.of("leader-gate-job"));
    verify(recurringJobBuilder).withBusinessKey("leader-gate-job");
    verify(recurringJobBuilder).submit();
    assertTrue(registrationState.shouldFire("leader-gate-job"));
    assertFalse(registrationState.shouldFire("unknown-recurring-job"));
    verify(maintenance, never()).cancelOrphanedRecurringAnnotationJobs(anySet(), any());
    verify(coordinator, never()).release("recurring-annotation-orphan-cleanup");
  }

  @Test
  void registerRecurringJobs_usesDiscoveredRecurringBeanClassesWhenAvailable() throws Exception {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    var schedulerService = mock(JobSchedulerService.class);
    var jobBatchStatusStore = mock(JobBatchStatusStore.class);
    var recurringJobBuilder = mockRecurringJobBuilder();
    var beanManager = mock(BeanManager.class);
    Set<Bean<?>> beans = Set.of(beanFor(LeaderGateBean.class));
    when(beanManager.getBeans(eq(LeaderGateBean.class), any())).thenReturn(beans);
    when(schedulerService.scheduleRecurring(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any()))
        .thenReturn(recurringJobBuilder);

    var processor =
        new RecurringJobProcessor(
            schedulerService,
            jobBatchStatusStore,
            maintenance,
            beanManager,
            mock(RecurringMethodInvoker.class),
            null,
            new RecurringRegistrationState(),
            RatchetOptions.defaults(),
            Set.of(LeaderGateBean.class));

    processor.registerRecurringJobs();

    verify(beanManager).getBeans(eq(LeaderGateBean.class), any());
    verify(beanManager, never()).getBeans(eq(Object.class), any());
    verify(jobBatchStatusStore).cancelRecurringJobsByBusinessKeys(Set.of("leader-gate-job"));
    verify(schedulerService).scheduleRecurring(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
  }

  @Test
  void cleanup_runsWhenStartupLeaseAcquired_andAppliesConvergenceWindow() throws Exception {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    when(maintenance.cancelOrphanedRecurringAnnotationJobs(anySet(), any())).thenReturn(0);
    var beanManager = mock(BeanManager.class);
    when(beanManager.getBeans(any(), any())).thenReturn(Collections.emptySet());
    var coordinator = mock(StartupCoordinator.class);
    when(coordinator.tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5)))
        .thenReturn(true);

    var processor =
        new RecurringJobProcessor(
            mock(JobSchedulerService.class),
            mock(JobBatchStatusStore.class),
            maintenance,
            beanManager,
            mock(RecurringMethodInvoker.class),
            coordinator,
            new RecurringRegistrationState(),
            RatchetOptions.builder()
                .recurring(recurring -> recurring.convergenceWindowSeconds(120))
                .build(),
            Set.of(),
            FIXED_CLOCK);

    processor.registerRecurringJobs();

    ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(anySet(), cutoffCaptor.capture());
    verify(coordinator).release("recurring-annotation-orphan-cleanup");

    Instant cutoff = cutoffCaptor.getValue();
    assertEquals(FIXED_NOW.minusSeconds(120), cutoff);
  }

  @Test
  void cleanup_runsWhenStartupCoordinatorMissing() throws Exception {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    when(maintenance.cancelOrphanedRecurringAnnotationJobs(anySet(), any())).thenReturn(0);
    var beanManager = mock(BeanManager.class);
    when(beanManager.getBeans(any(), any())).thenReturn(Collections.emptySet());

    var processor =
        new RecurringJobProcessor(
            mock(JobSchedulerService.class),
            mock(JobBatchStatusStore.class),
            maintenance,
            beanManager,
            mock(RecurringMethodInvoker.class),
            null,
            new RecurringRegistrationState());

    processor.registerRecurringJobs();

    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(anySet(), any());
  }

  @Test
  void cleanup_releasesLease_evenIfMaintenanceThrows() {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    when(maintenance.cancelOrphanedRecurringAnnotationJobs(anySet(), any()))
        .thenThrow(new IllegalStateException("store unavailable"));
    var beanManager = mock(BeanManager.class);
    when(beanManager.getBeans(any(), any())).thenReturn(Collections.emptySet());
    var coordinator = mock(StartupCoordinator.class);
    when(coordinator.tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5)))
        .thenReturn(true);

    var processor =
        new RecurringJobProcessor(
            mock(JobSchedulerService.class),
            mock(JobBatchStatusStore.class),
            maintenance,
            beanManager,
            mock(RecurringMethodInvoker.class),
            coordinator,
            new RecurringRegistrationState());

    assertDoesNotThrow(processor::registerRecurringJobs);

    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(anySet(), any());
    verify(coordinator).tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5));
    verify(coordinator).release("recurring-annotation-orphan-cleanup");
    verifyNoMoreInteractions(coordinator);
  }

  private static Bean<?> beanFor(Class<?> beanClass) {
    var bean = mock(Bean.class);
    when(bean.getBeanClass()).thenReturn(beanClass);
    return bean;
  }

  private static RecurringJobBuilder mockRecurringJobBuilder() {
    var builder = mock(RecurringJobBuilder.class);
    when(builder.withOptions(any())).thenReturn(builder);
    when(builder.withBusinessKey(any())).thenReturn(builder);
    when(builder.withTags(any())).thenReturn(builder);
    when(builder.submit()).thenReturn((JobHandle) () -> UUID.randomUUID());
    return builder;
  }

  static class LeaderGateBean {
    @Recurring(id = "leader-gate-job", cron = "0 0/5 * * * ?")
    public void run() {}
  }
}

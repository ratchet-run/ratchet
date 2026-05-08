package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
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

  @Test
  void cleanup_skippedWhenStartupLeaseNotAcquired() throws Exception {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    var schedulerService = mock(JobSchedulerService.class);
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
            mock(JobBatchStatusStore.class),
            maintenance,
            beanManager,
            mock(RecurringMethodInvoker.class),
            coordinator,
            registrationState);

    processor.registerRecurringJobs();

    verify(schedulerService)
        .scheduleRecurring(eq("0 0/5 * * * ?"), eq(ZoneId.of("UTC")), any());
    verify(recurringJobBuilder).withBusinessKey("leader-gate-job");
    verify(recurringJobBuilder).submit();
    assertTrue(registrationState.shouldFire("leader-gate-job"));
    assertFalse(registrationState.shouldFire("unknown-recurring-job"));
    verify(maintenance, never()).cancelOrphanedRecurringAnnotationJobs(anySet(), any());
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
                .build());

    Instant beforeRun = Instant.now();
    processor.registerRecurringJobs();
    Instant afterRun = Instant.now();

    ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(anySet(), cutoffCaptor.capture());
    verify(coordinator).release("recurring-annotation-orphan-cleanup");

    Instant cutoff = cutoffCaptor.getValue();
    Instant lowerBound = beforeRun.minusSeconds(120);
    Instant upperBound = afterRun.minusSeconds(120);
    assertFalse(cutoff.isBefore(lowerBound), "Cutoff must not be before lowerBound; got " + cutoff);
    assertFalse(cutoff.isAfter(upperBound), "Cutoff must not be after upperBound; got " + cutoff);
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

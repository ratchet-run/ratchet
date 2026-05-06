package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.spi.BeanManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.RecurringRegistrationState;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.JobBatchStatusStore;

// Verifies startup-lease-gated cleanup and convergence window in RecurringJobProcessor.
class RecurringJobProcessorLeaderGateTest {

  @Test
  void cleanup_skippedWhenStartupLeaseNotAcquired() throws Exception {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    var beanManager = mock(BeanManager.class);
    when(beanManager.getBeans(any(), any())).thenReturn(Collections.emptySet());
    var coordinator = mock(StartupCoordinator.class);
    when(coordinator.tryAcquire("recurring-annotation-orphan-cleanup", Duration.ofMinutes(5)))
        .thenReturn(false);

    var processor =
        new RecurringJobProcessor(
            mock(JobSchedulerService.class),
            mock(JobBatchStatusStore.class),
            maintenance,
            beanManager,
            mock(RecurringMethodInvoker.class),
            coordinator,
            new RecurringRegistrationState());

    processor.registerRecurringJobs();

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
}

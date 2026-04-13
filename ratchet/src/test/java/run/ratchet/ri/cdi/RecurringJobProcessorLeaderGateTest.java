package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.api.JobSchedulerService;
import run.ratchet.ri.core.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.RecurringRegistrationState;
import run.ratchet.spi.ClusterCoordinator;
import jakarta.enterprise.inject.spi.BeanManager;
import java.time.Instant;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

// Verifies leader-gated cleanup and convergence window in RecurringJobProcessor.
class RecurringJobProcessorLeaderGateTest {

  @AfterEach
  void clearSystemProperty() {
    System.clearProperty(RecurringJobProcessor.CONVERGENCE_WINDOW_PROPERTY);
  }

  @Test
  void cleanup_skippedWhenNotLeader() throws Exception {
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    var beanManager = mock(BeanManager.class);
    when(beanManager.getBeans(any(), any())).thenReturn(Collections.emptySet());
    var coordinator = mock(ClusterCoordinator.class);
    when(coordinator.isLeader()).thenReturn(false);

    var processor =
        new RecurringJobProcessor(
            mock(JobSchedulerService.class),
            maintenance,
            beanManager,
            mock(RecurringMethodInvoker.class),
            coordinator,
            new RecurringRegistrationState());

    processor.registerRecurringJobs();

    verify(maintenance, never()).cancelOrphanedRecurringAnnotationJobs(anySet(), any());
  }

  @Test
  void cleanup_runsWhenLeader_andAppliesConvergenceWindow() throws Exception {
    System.setProperty(RecurringJobProcessor.CONVERGENCE_WINDOW_PROPERTY, "120");
    var maintenance = mock(RecurringAnnotationMaintenanceService.class);
    when(maintenance.cancelOrphanedRecurringAnnotationJobs(anySet(), any())).thenReturn(0);
    var beanManager = mock(BeanManager.class);
    when(beanManager.getBeans(any(), any())).thenReturn(Collections.emptySet());
    var coordinator = mock(ClusterCoordinator.class);
    when(coordinator.isLeader()).thenReturn(true);

    var processor =
        new RecurringJobProcessor(
            mock(JobSchedulerService.class),
            maintenance,
            beanManager,
            mock(RecurringMethodInvoker.class),
            coordinator,
            new RecurringRegistrationState());

    Instant beforeRun = Instant.now();
    processor.registerRecurringJobs();
    Instant afterRun = Instant.now();

    ArgumentCaptor<Instant> cutoffCaptor = ArgumentCaptor.forClass(Instant.class);
    verify(maintenance).cancelOrphanedRecurringAnnotationJobs(anySet(), cutoffCaptor.capture());

    Instant cutoff = cutoffCaptor.getValue();
    Instant lowerBound = beforeRun.minusSeconds(120);
    Instant upperBound = afterRun.minusSeconds(120);
    assertFalse(cutoff.isBefore(lowerBound), "Cutoff must not be before lowerBound; got " + cutoff);
    assertFalse(cutoff.isAfter(upperBound), "Cutoff must not be after upperBound; got " + cutoff);
  }

  @Test
  void convergenceWindowSeconds_defaultsToZero_whenPropertyUnset() {
    System.clearProperty(RecurringJobProcessor.CONVERGENCE_WINDOW_PROPERTY);
    // 0.2.0: default is 0 (deprecated). The role is now covered by
    // RecurringRegistrationState.shouldFire().
    assertEquals(0L, RecurringJobProcessor.convergenceWindowSeconds());
  }

  @Test
  void convergenceWindowSeconds_honorsSystemProperty() {
    System.setProperty(RecurringJobProcessor.CONVERGENCE_WINDOW_PROPERTY, "45");
    assertEquals(45L, RecurringJobProcessor.convergenceWindowSeconds());
  }

  @Test
  void convergenceWindowSeconds_clampsNegativeToZero() {
    System.setProperty(RecurringJobProcessor.CONVERGENCE_WINDOW_PROPERTY, "-10");
    assertEquals(0L, RecurringJobProcessor.convergenceWindowSeconds());
  }

  @Test
  void convergenceWindowSeconds_fallsBackToDefaultZeroOnInvalidProperty() {
    System.setProperty(RecurringJobProcessor.CONVERGENCE_WINDOW_PROPERTY, "not-a-number");
    assertEquals(0L, RecurringJobProcessor.convergenceWindowSeconds());
  }
}

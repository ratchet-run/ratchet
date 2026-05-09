package run.ratchet.ri.cdi;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.BatchRecoveryTimer;
import run.ratchet.ri.core.DeadLetterService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.JobExecutionCoordinator;
import run.ratchet.ri.core.LogPurgeTimer;
import run.ratchet.ri.core.OrphanRecoveryTimer;
import run.ratchet.ri.core.Poller;
import run.ratchet.ri.core.PollerWakeupListener;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;

// Verifies drain is engaged before poller.stop() during shutdown.
class RatchetLifecycleShutdownTest {

  @Test
  void onStartupStartsRetryBufferDrainer() {
    LifecycleFixture fixture = new LifecycleFixture(quietOptions());

    fixture.lifecycle.onStartup(new Object());

    verify(fixture.jobExecutionCoordinator).initRetryBufferDrainer();
    verifyNoInteractions(
        fixture.deadLetterService, fixture.jobArchivingService, fixture.logPurgeTimer);
  }

  @Test
  void onShutdownEngagesDrainBeforeStoppingPoller() {
    LifecycleFixture fixture = new LifecycleFixture(RatchetOptions.defaults());

    fixture.lifecycle.onShutdown();

    InOrder inOrder = inOrder(fixture.drainController, fixture.poller);
    inOrder.verify(fixture.drainController).setDraining(true);
    inOrder.verify(fixture.poller).stop();

    verify(fixture.recurringScheduler).stop();
    verify(fixture.orphanRecoveryTimer).stop();
    verify(fixture.batchRecoveryTimer).stop();
    verify(fixture.deadLetterService).stop();
    verify(fixture.jobArchivingService).stop();
    verify(fixture.logPurgeTimer).stop();
    verify(fixture.jobExecutionCoordinator).shutdown();
  }

  private static RatchetOptions quietOptions() {
    return RatchetOptions.builder()
        .node(node -> node.orphanScanIntervalMinutes(1L))
        .maintenance(
            maintenance ->
                maintenance.dlqPurgeEnabled(false).jobArchiveEnabled(false).logPurgeEnabled(false))
        .build();
  }

  private static ExecutorProvider executorProviderWithScheduler() {
    ExecutorProvider executorProvider = mock(ExecutorProvider.class);
    when(executorProvider.getScheduledExecutor()).thenReturn(mock(ScheduledExecutorService.class));
    return executorProvider;
  }

  private static final class LifecycleFixture {
    final Poller poller = mock(Poller.class);
    final RecurringScheduler recurringScheduler = mock(RecurringScheduler.class);
    final OrphanRecoveryTimer orphanRecoveryTimer = mock(OrphanRecoveryTimer.class);
    final BatchRecoveryTimer batchRecoveryTimer = mock(BatchRecoveryTimer.class);
    final DeadLetterService deadLetterService = mock(DeadLetterService.class);
    final JobArchivingService jobArchivingService = mock(JobArchivingService.class);
    final LogPurgeTimer logPurgeTimer = mock(LogPurgeTimer.class);
    final DrainController drainController = mock(DrainController.class);
    final JobExecutionCoordinator jobExecutionCoordinator = mock(JobExecutionCoordinator.class);
    final RatchetLifecycle lifecycle;

    LifecycleFixture(RatchetOptions options) {
      lifecycle =
          new RatchetLifecycle(
              poller,
              recurringScheduler,
              orphanRecoveryTimer,
              batchRecoveryTimer,
              deadLetterService,
              jobArchivingService,
              logPurgeTimer,
              mock(PollerWakeupListener.class),
              executorProviderWithScheduler(),
              mock(NodeIdentityProvider.class),
              drainController,
              options,
              jobExecutionCoordinator);
    }
  }
}

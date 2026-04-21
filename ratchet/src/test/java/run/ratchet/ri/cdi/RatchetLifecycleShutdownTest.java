package run.ratchet.ri.cdi;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

// Verifies drain is engaged before poller.stop() during shutdown.
class RatchetLifecycleShutdownTest {

  @Test
  void onStartupStartsRetryBufferDrainer() {
    Poller poller = mock(Poller.class);
    RecurringScheduler recurringScheduler = mock(RecurringScheduler.class);
    OrphanRecoveryTimer orphanRecoveryTimer = mock(OrphanRecoveryTimer.class);
    BatchRecoveryTimer batchRecoveryTimer = mock(BatchRecoveryTimer.class);
    DeadLetterService deadLetterService = mock(DeadLetterService.class);
    JobArchivingService jobArchivingService = mock(JobArchivingService.class);
    LogPurgeTimer logPurgeTimer = mock(LogPurgeTimer.class);
    PollerWakeupListener pollerWakeupListener = mock(PollerWakeupListener.class);
    ExecutorProvider executorProvider = mock(ExecutorProvider.class);
    ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
    NodeIdentityProvider nodeIdentityProvider = mock(NodeIdentityProvider.class);
    DrainController drainController = mock(DrainController.class);
    RatchetOptions options =
        RatchetOptions.builder()
            .node(node -> node.orphanScanIntervalMinutes(1L))
            .maintenance(
                maintenance ->
                    maintenance
                        .dlqPurgeEnabled(false)
                        .jobArchiveEnabled(false)
                        .logPurgeEnabled(false))
            .build();
    JobExecutionCoordinator jobExecutionCoordinator = mock(JobExecutionCoordinator.class);

    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);

    RatchetLifecycle lifecycle =
        new RatchetLifecycle(
            poller,
            recurringScheduler,
            orphanRecoveryTimer,
            batchRecoveryTimer,
            deadLetterService,
            jobArchivingService,
            logPurgeTimer,
            pollerWakeupListener,
            executorProvider,
            nodeIdentityProvider,
            drainController,
            options,
            jobExecutionCoordinator);

    lifecycle.onStartup(new Object());

    verify(jobExecutionCoordinator).initRetryBufferDrainer();
  }

  @Test
  void onShutdownEngagesDrainBeforeStoppingPoller() {
    Poller poller = mock(Poller.class);
    DrainController drainController = mock(DrainController.class);
    RecurringScheduler recurringScheduler = mock(RecurringScheduler.class);
    OrphanRecoveryTimer orphanRecoveryTimer = mock(OrphanRecoveryTimer.class);
    BatchRecoveryTimer batchRecoveryTimer = mock(BatchRecoveryTimer.class);
    DeadLetterService deadLetterService = mock(DeadLetterService.class);
    JobArchivingService jobArchivingService = mock(JobArchivingService.class);
    LogPurgeTimer logPurgeTimer = mock(LogPurgeTimer.class);
    PollerWakeupListener pollerWakeupListener = mock(PollerWakeupListener.class);
    ExecutorProvider executorProvider = mock(ExecutorProvider.class);
    NodeIdentityProvider nodeIdentityProvider = mock(NodeIdentityProvider.class);
    RatchetOptions options = RatchetOptions.defaults();
    JobExecutionCoordinator jobExecutionCoordinator = mock(JobExecutionCoordinator.class);

    RatchetLifecycle lifecycle =
        new RatchetLifecycle(
            poller,
            recurringScheduler,
            orphanRecoveryTimer,
            batchRecoveryTimer,
            deadLetterService,
            jobArchivingService,
            logPurgeTimer,
            pollerWakeupListener,
            executorProvider,
            nodeIdentityProvider,
            drainController,
            options,
            jobExecutionCoordinator);

    lifecycle.onShutdown();

    InOrder inOrder = inOrder(drainController, poller);
    inOrder.verify(drainController).setDraining(true);
    inOrder.verify(poller).stop();

    verify(recurringScheduler).stop();
    verify(orphanRecoveryTimer).stop();
    verify(batchRecoveryTimer).stop();
    verify(deadLetterService).stop();
    verify(jobArchivingService).stop();
    verify(logPurgeTimer).stop();
    verify(jobExecutionCoordinator).shutdown();
  }
}

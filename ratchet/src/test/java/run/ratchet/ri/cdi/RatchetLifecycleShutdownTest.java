package run.ratchet.ri.cdi;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import run.ratchet.ri.core.BatchRecoveryTimer;
import run.ratchet.ri.core.DeadLetterService;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.LogPurgeTimer;
import run.ratchet.ri.core.OrphanRecoveryTimer;
import run.ratchet.ri.core.Poller;
import run.ratchet.ri.core.PollerWakeupListener;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.util.RatchetConfiguration;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Verifies that {@link RatchetLifecycle#onShutdown()} engages drain mode BEFORE stopping the
 * poller. This ordering is critical: a Poller.tick() already past its {@code started.get()} check
 * must see {@code drainController.isDraining() == true} and short-circuit instead of claiming jobs
 * that would be orphaned when the executor tears down.
 *
 * <p>This is a unit test (not an Arquillian IT) because verifying method call ordering requires
 * Mockito InOrder, and reflective invocation of package-private {@code @PreDestroy} methods on CDI
 * proxies is unreliable (the proxy's superclass fields are null, bypassing the contextual
 * instance).
 */
class RatchetLifecycleShutdownTest {

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
    RatchetConfiguration config = mock(RatchetConfiguration.class);

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
            config);

    lifecycle.onShutdown();

    // The critical ordering: drain MUST be engaged before the poller is stopped.
    InOrder inOrder = inOrder(drainController, poller);
    inOrder.verify(drainController).setDraining(true);
    inOrder.verify(poller).stop();

    // All other components must also be stopped.
    verify(recurringScheduler).stop();
    verify(orphanRecoveryTimer).stop();
    verify(batchRecoveryTimer).stop();
    verify(deadLetterService).stop();
    verify(jobArchivingService).stop();
    verify(logPurgeTimer).stop();
  }
}

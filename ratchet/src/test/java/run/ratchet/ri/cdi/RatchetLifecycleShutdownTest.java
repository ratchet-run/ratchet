package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;
import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
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
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;

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

  @Test
  void onShutdown_stopFailure_continuesStoppingRemainingServices() {
    LifecycleFixture fixture = new LifecycleFixture(RatchetOptions.defaults());
    doThrow(new IllegalStateException("poller failed")).when(fixture.poller).stop();

    fixture.lifecycle.onShutdown();

    verify(fixture.recurringScheduler).stop();
    verify(fixture.orphanRecoveryTimer).stop();
    verify(fixture.batchRecoveryTimer).stop();
    verify(fixture.deadLetterService).stop();
    verify(fixture.jobArchivingService).stop();
    verify(fixture.logPurgeTimer).stop();
    verify(fixture.jobExecutionCoordinator).shutdown();
    verify(fixture.clusterCoordinator).close();
  }

  @Test
  void onShutdownClosesClusterCoordinatorAfterJobExecutionCoordinator() {
    LifecycleFixture fixture = new LifecycleFixture(RatchetOptions.defaults());

    fixture.lifecycle.onShutdown();

    InOrder inOrder = inOrder(fixture.jobExecutionCoordinator, fixture.clusterCoordinator);
    inOrder.verify(fixture.jobExecutionCoordinator).shutdown();
    inOrder.verify(fixture.clusterCoordinator).close();
  }

  @Test
  void onShutdown_hookImplementingCoordinator_closesExactlyOnceViaAfterStop() {
    HookCoordinator coordinator = new HookCoordinator();
    Instance<SchedulerLifecycleHook> hookInstance = new StubHookInstance(List.of(coordinator));
    RatchetLifecycle lifecycle =
        new RatchetLifecycle(
            mock(Poller.class),
            mock(RecurringScheduler.class),
            mock(OrphanRecoveryTimer.class),
            mock(BatchRecoveryTimer.class),
            mock(DeadLetterService.class),
            mock(JobArchivingService.class),
            mock(LogPurgeTimer.class),
            mock(PollerWakeupListener.class),
            executorProviderWithScheduler(),
            mock(NodeIdentityProvider.class),
            mock(DrainController.class),
            quietOptions(),
            mock(JobExecutionCoordinator.class),
            coordinator,
            hookInstance);

    lifecycle.onStartup(new Object());
    lifecycle.onShutdown();

    assertEquals(
        1,
        coordinator.closes.get(),
        "coordinator implementing SchedulerLifecycleHook must be closed exactly once via afterStop"
            + " — the direct fallback in RatchetLifecycle must skip it");
  }

  @Test
  void onShutdown_nonHookCoordinator_stillClosedViaDirectFallback() {
    // Backwards-compat: a coordinator that does NOT implement SchedulerLifecycleHook is closed
    // via the direct stopService call.
    LifecycleFixture fixture = new LifecycleFixture(RatchetOptions.defaults());

    fixture.lifecycle.onShutdown();

    verify(fixture.clusterCoordinator, org.mockito.Mockito.times(1)).close();
  }

  /** Minimal {@link Instance} stub that just iterates the supplied list. */
  private static final class StubHookInstance implements Instance<SchedulerLifecycleHook> {
    private final List<SchedulerLifecycleHook> hooks;

    StubHookInstance(List<SchedulerLifecycleHook> hooks) {
      this.hooks = hooks;
    }

    @Override
    public Iterator<SchedulerLifecycleHook> iterator() {
      return hooks.iterator();
    }

    @Override
    public Stream<SchedulerLifecycleHook> stream() {
      return hooks.stream();
    }

    @Override
    public SchedulerLifecycleHook get() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instance<SchedulerLifecycleHook> select(Annotation... qualifiers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <U extends SchedulerLifecycleHook> Instance<U> select(
        Class<U> subtype, Annotation... qualifiers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <U extends SchedulerLifecycleHook> Instance<U> select(
        TypeLiteral<U> subtype, Annotation... qualifiers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUnsatisfied() {
      return hooks.isEmpty();
    }

    @Override
    public boolean isAmbiguous() {
      return hooks.size() > 1;
    }

    @Override
    public void destroy(SchedulerLifecycleHook instance) {}

    @Override
    public Handle<SchedulerLifecycleHook> getHandle() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<? extends Handle<SchedulerLifecycleHook>> handles() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stream<? extends Handle<SchedulerLifecycleHook>> handlesStream() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Concrete coordinator that also implements SchedulerLifecycleHook so the test can assert close()
   * is invoked through afterStop, not directly.
   */
  private static final class HookCoordinator implements ClusterCoordinator, SchedulerLifecycleHook {
    final AtomicInteger closes = new AtomicInteger();

    @Override
    public void notifyNewWork(JobPriority priority, NodeIdentity source) {}

    @Override
    public void registerWakeupListener(BiConsumer<JobPriority, NodeIdentity> listener) {}

    @Override
    public void close() {
      closes.incrementAndGet();
    }

    @Override
    public void afterStop() {
      close();
    }
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
    final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
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
              jobExecutionCoordinator,
              clusterCoordinator);
    }
  }
}

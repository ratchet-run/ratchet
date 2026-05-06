package run.ratchet.ri.cdi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Instance.Handle;
import jakarta.enterprise.util.TypeLiteral;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
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
import run.ratchet.spi.SchedulerLifecycleHook;

/**
 * Verifies that {@link RatchetLifecycle} releases each resolved {@link SchedulerLifecycleHook} via
 * {@link Instance#destroy(Object)} during shutdown — required for {@code @Dependent}-scoped hooks
 * to have their {@code @PreDestroy} methods invoked. Pre-fix, the field was kept as {@code
 * Iterable<SchedulerLifecycleHook>}, losing the {@code destroy()} capability.
 */
class RatchetLifecycleHookTest {

  @Test
  void onShutdown_destroysEveryResolvedHookExactlyOnce() {
    SchedulerLifecycleHook hookA = mock(SchedulerLifecycleHook.class);
    SchedulerLifecycleHook hookB = mock(SchedulerLifecycleHook.class);
    RecordingInstance<SchedulerLifecycleHook> hookInstance =
        new RecordingInstance<>(List.of(hookA, hookB));

    RatchetLifecycle lifecycle = newLifecycle(hookInstance);
    lifecycle.onStartup(new Object());
    lifecycle.onShutdown();

    verify(hookA).beforeStart();
    verify(hookA).afterStart();
    verify(hookA).beforeStop();
    verify(hookA).afterStop();
    verify(hookB).beforeStart();
    verify(hookB).afterStart();
    verify(hookB).beforeStop();
    verify(hookB).afterStop();

    assertEquals(
        List.of(hookA, hookB),
        hookInstance.destroyed,
        "Every resolved hook must be released via Instance.destroy() during @PreDestroy");
    assertEquals(
        1,
        hookInstance.iteratorCalls,
        "Hooks must be resolved once and cached — multiple iterations leak @Dependent instances");
  }

  @Test
  void onShutdown_withNullInstance_doesNotThrow() {
    // Non-CDI construction path — no Instance available; destroyHooks must be a safe no-op.
    RatchetLifecycle lifecycle = newLifecycleNoCdi();
    lifecycle.onShutdown();
    // No assertion needed: just verifying no NPE on null lifecycleHooks field.
  }

  private RatchetLifecycle newLifecycle(Instance<SchedulerLifecycleHook> hooks) {
    return new RatchetLifecycle(
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
        hooks);
  }

  private RatchetLifecycle newLifecycleNoCdi() {
    return new RatchetLifecycle(
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
        mock(JobExecutionCoordinator.class));
  }

  private ExecutorProvider executorProviderWithScheduler() {
    ExecutorProvider provider = mock(ExecutorProvider.class);
    when(provider.getScheduledExecutor())
        .thenReturn(mock(java.util.concurrent.ScheduledExecutorService.class));
    return provider;
  }

  private RatchetOptions quietOptions() {
    return RatchetOptions.builder()
        .node(node -> node.orphanScanIntervalMinutes(1L))
        .maintenance(
            maintenance ->
                maintenance.dlqPurgeEnabled(false).jobArchiveEnabled(false).logPurgeEnabled(false))
        .build();
  }

  /**
   * Minimal {@link Instance} fake that records destroy() calls and counts iterator() calls. Mirrors
   * the destroy contract well enough to verify the lifecycle wiring without spinning up a full CDI
   * container.
   */
  private static final class RecordingInstance<T> implements Instance<T> {
    private final List<T> elements;
    final List<T> destroyed = new ArrayList<>();
    int iteratorCalls = 0;

    RecordingInstance(List<T> elements) {
      this.elements = elements;
    }

    @Override
    public Iterator<T> iterator() {
      iteratorCalls++;
      return elements.iterator();
    }

    @Override
    public void destroy(T instance) {
      destroyed.add(instance);
    }

    @Override
    public T get() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instance<T> select(Annotation... qualifiers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <U extends T> Instance<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUnsatisfied() {
      return elements.isEmpty();
    }

    @Override
    public boolean isAmbiguous() {
      return elements.size() > 1;
    }

    @Override
    public Handle<T> getHandle() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<? extends Handle<T>> handles() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stream<Handle<T>> handlesStream() {
      throw new UnsupportedOperationException();
    }
  }
}

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
package run.ratchet.ri.cdi.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Instance.Handle;
import jakarta.enterprise.util.TypeLiteral;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.core.internal.BatchRecoveryTimer;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.internal.JobExecutionCoordinator;
import run.ratchet.ri.core.internal.LogPurgeTimer;
import run.ratchet.ri.core.internal.OrphanRecoveryTimer;
import run.ratchet.ri.core.internal.Poller;
import run.ratchet.ri.core.internal.PollerWakeupListener;
import run.ratchet.ri.runtime.RatchetRuntime;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.migration.SchemaInitializationException;

/**
 * Verifies that {@link DefaultRatchetLifecycle} releases each resolved {@link
 * SchedulerLifecycleHook} via {@link Instance#destroy(Object)} during shutdown — required for
 * {@code @Dependent}-scoped hooks to have their {@code @PreDestroy} methods invoked. Pre-fix, the
 * field was kept as {@code Iterable<SchedulerLifecycleHook>}, losing the {@code destroy()}
 * capability.
 */
class DefaultRatchetLifecycleHookTest {

  @Test
  void onShutdown_destroysEveryResolvedHookExactlyOnce() {
    SchedulerLifecycleHook hookA = mock(SchedulerLifecycleHook.class);
    SchedulerLifecycleHook hookB = mock(SchedulerLifecycleHook.class);
    RecordingInstance<SchedulerLifecycleHook> hookInstance =
        new RecordingInstance<>(List.of(hookA, hookB));

    DefaultRatchetLifecycle lifecycle = newLifecycle(hookInstance);
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
  void hooksRunInPriorityThenClassNameOrder() {
    List<String> events = new ArrayList<>();
    SchedulerLifecycleHook zulu = new ZuluFallbackHook(events);
    SchedulerLifecycleHook lowPriority = new LowPriorityHook(events);
    SchedulerLifecycleHook alpha = new AlphaFallbackHook(events);
    SchedulerLifecycleHook highPriority = new HighPriorityHook(events);

    DefaultRatchetLifecycle lifecycle =
        newLifecycle(new RecordingInstance<>(List.of(zulu, lowPriority, alpha, highPriority)));

    lifecycle.onStartup(new Object());

    assertEquals(
        List.of(
            "high.beforeStart",
            "low.beforeStart",
            "alpha.beforeStart",
            "zulu.beforeStart",
            "high.afterStart",
            "low.afterStart",
            "alpha.afterStart",
            "zulu.afterStart"),
        events);
  }

  @Test
  void beforeStartHooksRunBeforeDefaultNodeRegistration() {
    List<String> events = new ArrayList<>();
    DefaultNodeIdentityProvider nodeIdentityProvider = mock(DefaultNodeIdentityProvider.class);
    doAnswer(
            invocation -> {
              events.add("node.init");
              return null;
            })
        .when(nodeIdentityProvider)
        .init();

    DefaultRatchetLifecycle lifecycle =
        new DefaultRatchetLifecycle(
            mock(Poller.class),
            mock(RecurringScheduler.class),
            mock(OrphanRecoveryTimer.class),
            mock(BatchRecoveryTimer.class),
            mock(DeadLetterService.class),
            mock(JobArchivingService.class),
            mock(LogPurgeTimer.class),
            mock(PollerWakeupListener.class),
            executorProviderWithScheduler(),
            nodeIdentityProvider,
            mock(DrainController.class),
            quietOptions(),
            mock(JobExecutionCoordinator.class),
            mock(ClusterCoordinator.class),
            new RecordingInstance<>(List.of(new SuccessfulHook(events))));

    lifecycle.onStartup(new Object());

    assertEquals(List.of("successful.beforeStart", "node.init", "successful.afterStart"), events);
  }

  @Test
  void failedBeforeStartHookDoesNotReceivePairedLifecyclePhases() {
    List<String> events = new ArrayList<>();
    SchedulerLifecycleHook failing = new FailingBeforeStartHook(events);
    SchedulerLifecycleHook successful = new SuccessfulHook(events);

    DefaultRatchetLifecycle lifecycle =
        newLifecycle(new RecordingInstance<>(List.of(successful, failing)));

    assertDoesNotThrow(() -> lifecycle.onStartup(new Object()));
    lifecycle.onShutdown();

    assertEquals(
        List.of(
            "failing.beforeStart",
            "successful.beforeStart",
            "successful.afterStart",
            "successful.beforeStop",
            "successful.afterStop"),
        events);
  }

  @Test
  void failedBeforeStopHookDoesNotReceiveAfterStop() {
    List<String> events = new ArrayList<>();
    SchedulerLifecycleHook hook = new FailingBeforeStopHook(events);

    DefaultRatchetLifecycle lifecycle = newLifecycle(new RecordingInstance<>(List.of(hook)));

    lifecycle.onStartup(new Object());
    assertDoesNotThrow(lifecycle::onShutdown);

    assertEquals(
        List.of("failingStop.beforeStart", "failingStop.afterStart", "failingStop.beforeStop"),
        events);
  }

  @Test
  void onShutdown_isIdempotent() {
    SchedulerLifecycleHook hook = mock(SchedulerLifecycleHook.class);
    RecordingInstance<SchedulerLifecycleHook> hookInstance = new RecordingInstance<>(List.of(hook));

    DefaultRatchetLifecycle lifecycle = newLifecycle(hookInstance);
    lifecycle.onStartup(new Object());
    lifecycle.onShutdown();
    lifecycle.onShutdown();

    verify(hook, times(1)).beforeStop();
    verify(hook, times(1)).afterStop();
    assertEquals(List.of(hook), hookInstance.destroyed);
  }

  @Test
  void onShutdown_destroysResolvedHooksWhenRuntimeStopThrows() throws Exception {
    SchedulerLifecycleHook hook = mock(SchedulerLifecycleHook.class);
    RecordingInstance<SchedulerLifecycleHook> hookInstance = new RecordingInstance<>(List.of(hook));
    DefaultRatchetLifecycle lifecycle = newLifecycle(hookInstance);
    lifecycle.onStartup(new Object());

    RatchetRuntime failingRuntime = mock(RatchetRuntime.class);
    doThrow(new IllegalStateException("stop failed")).when(failingRuntime).stop();
    Field runtimeField = DefaultRatchetLifecycle.class.getDeclaredField("runtime");
    runtimeField.setAccessible(true);
    runtimeField.set(lifecycle, failingRuntime);

    assertThrows(IllegalStateException.class, lifecycle::onShutdown);

    assertEquals(
        List.of(hook),
        hookInstance.destroyed,
        "Hook destruction must run from the CDI delegate's finally block");
  }

  @Test
  void onShutdown_withNullInstance_doesNotThrow() {
    // Non-CDI construction path — no Instance available; destroyHooks must be a safe no-op.
    DefaultRatchetLifecycle lifecycle = newLifecycleNoCdi();
    lifecycle.onShutdown();
    // No assertion needed: just verifying no NPE on null lifecycleHooks field.
  }

  @Test
  void beforeStart_schemaInitializationExceptionAbortsStartup() {
    SchedulerLifecycleHook hook =
        new SchedulerLifecycleHook() {
          @Override
          public void beforeStart() {
            throw new SchemaInitializationException("schema not ready");
          }
        };

    DefaultRatchetLifecycle lifecycle = newLifecycle(new RecordingInstance<>(List.of(hook)));

    assertThrows(SchemaInitializationException.class, () -> lifecycle.onStartup(new Object()));
  }

  @Test
  void nonStartupPhases_swallowSchemaInitializationExceptionLikeOtherHookFailures() {
    SchedulerLifecycleHook afterStartFailure =
        new SchedulerLifecycleHook() {
          @Override
          public void afterStart() {
            throw new SchemaInitializationException("late hook failure");
          }
        };

    SchedulerLifecycleHook beforeStopFailure =
        new SchedulerLifecycleHook() {
          @Override
          public void beforeStop() {
            throw new SchemaInitializationException("shutdown hook failure");
          }
        };

    SchedulerLifecycleHook afterStopFailure =
        new SchedulerLifecycleHook() {
          @Override
          public void afterStop() {
            throw new SchemaInitializationException("post-shutdown hook failure");
          }
        };

    DefaultRatchetLifecycle lifecycle =
        newLifecycle(
            new RecordingInstance<>(
                List.of(afterStartFailure, beforeStopFailure, afterStopFailure)));

    assertDoesNotThrow(() -> lifecycle.onStartup(new Object()));
    assertDoesNotThrow(lifecycle::onShutdown);
  }

  @Test
  void onStartup_whenScheduledExecutorUnavailable_degradesScheduledServicesAndContinues() {
    Poller poller = mock(Poller.class);
    RecurringScheduler recurringScheduler = mock(RecurringScheduler.class);
    OrphanRecoveryTimer orphanRecoveryTimer = mock(OrphanRecoveryTimer.class);
    BatchRecoveryTimer batchRecoveryTimer = mock(BatchRecoveryTimer.class);
    DeadLetterService deadLetterService = mock(DeadLetterService.class);
    JobArchivingService jobArchivingService = mock(JobArchivingService.class);
    LogPurgeTimer logPurgeTimer = mock(LogPurgeTimer.class);
    PollerWakeupListener pollerWakeupListener = mock(PollerWakeupListener.class);
    JobExecutionCoordinator jobExecutionCoordinator = mock(JobExecutionCoordinator.class);
    ExecutorProvider provider = mock(ExecutorProvider.class);
    when(provider.getScheduledExecutor()).thenThrow(new IllegalStateException("java:comp unbound"));

    List<String> events = new ArrayList<>();
    DefaultRatchetLifecycle lifecycle =
        new DefaultRatchetLifecycle(
            poller,
            recurringScheduler,
            orphanRecoveryTimer,
            batchRecoveryTimer,
            deadLetterService,
            jobArchivingService,
            logPurgeTimer,
            pollerWakeupListener,
            provider,
            mock(NodeIdentityProvider.class),
            mock(DrainController.class),
            RatchetOptions.defaults(),
            jobExecutionCoordinator,
            mock(ClusterCoordinator.class),
            new RecordingInstance<>(List.of(new SuccessfulHook(events))));

    assertDoesNotThrow(() -> lifecycle.onStartup(new Object()));

    assertEquals(List.of("successful.beforeStart", "successful.afterStart"), events);
    verify(provider).getScheduledExecutor();
    verify(pollerWakeupListener).init();
    verifyNoInteractions(
        poller,
        recurringScheduler,
        orphanRecoveryTimer,
        batchRecoveryTimer,
        deadLetterService,
        jobArchivingService,
        logPurgeTimer,
        jobExecutionCoordinator);
  }

  private DefaultRatchetLifecycle newLifecycle(Instance<SchedulerLifecycleHook> hooks) {
    return new DefaultRatchetLifecycle(
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
        mock(ClusterCoordinator.class),
        hooks);
  }

  private DefaultRatchetLifecycle newLifecycleNoCdi() {
    return new DefaultRatchetLifecycle(
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
        mock(ClusterCoordinator.class));
  }

  private ExecutorProvider executorProviderWithScheduler() {
    ExecutorProvider provider = mock(ExecutorProvider.class);
    when(provider.getScheduledExecutor()).thenReturn(mock(ScheduledExecutorService.class));
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
    final List<T> destroyed = new ArrayList<>();
    private final List<T> elements;
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

  private static final class AlphaFallbackHook implements SchedulerLifecycleHook {
    private final List<String> events;

    AlphaFallbackHook(List<String> events) {
      this.events = events;
    }

    @Override
    public void beforeStart() {
      events.add("alpha.beforeStart");
    }

    @Override
    public void afterStart() {
      events.add("alpha.afterStart");
    }
  }

  private static final class ZuluFallbackHook implements SchedulerLifecycleHook {
    private final List<String> events;

    ZuluFallbackHook(List<String> events) {
      this.events = events;
    }

    @Override
    public void beforeStart() {
      events.add("zulu.beforeStart");
    }

    @Override
    public void afterStart() {
      events.add("zulu.afterStart");
    }
  }

  @Priority(10)
  private static final class HighPriorityHook implements SchedulerLifecycleHook {
    private final List<String> events;

    HighPriorityHook(List<String> events) {
      this.events = events;
    }

    @Override
    public void beforeStart() {
      events.add("high.beforeStart");
    }

    @Override
    public void afterStart() {
      events.add("high.afterStart");
    }
  }

  @Priority(20)
  private static final class LowPriorityHook implements SchedulerLifecycleHook {
    private final List<String> events;

    LowPriorityHook(List<String> events) {
      this.events = events;
    }

    @Override
    public void beforeStart() {
      events.add("low.beforeStart");
    }

    @Override
    public void afterStart() {
      events.add("low.afterStart");
    }
  }

  @Priority(10)
  private static final class FailingBeforeStartHook implements SchedulerLifecycleHook {
    private final List<String> events;

    FailingBeforeStartHook(List<String> events) {
      this.events = events;
    }

    @Override
    public void beforeStart() {
      events.add("failing.beforeStart");
      throw new IllegalStateException("hook failed");
    }

    @Override
    public void afterStart() {
      events.add("failing.afterStart");
    }

    @Override
    public void beforeStop() {
      events.add("failing.beforeStop");
    }

    @Override
    public void afterStop() {
      events.add("failing.afterStop");
    }
  }

  @Priority(20)
  private static final class SuccessfulHook implements SchedulerLifecycleHook {
    private final List<String> events;

    SuccessfulHook(List<String> events) {
      this.events = events;
    }

    @Override
    public void beforeStart() {
      events.add("successful.beforeStart");
    }

    @Override
    public void afterStart() {
      events.add("successful.afterStart");
    }

    @Override
    public void beforeStop() {
      events.add("successful.beforeStop");
    }

    @Override
    public void afterStop() {
      events.add("successful.afterStop");
    }
  }

  private static final class FailingBeforeStopHook implements SchedulerLifecycleHook {
    private final List<String> events;

    FailingBeforeStopHook(List<String> events) {
      this.events = events;
    }

    @Override
    public void beforeStart() {
      events.add("failingStop.beforeStart");
    }

    @Override
    public void afterStart() {
      events.add("failingStop.afterStart");
    }

    @Override
    public void beforeStop() {
      events.add("failingStop.beforeStop");
      throw new IllegalStateException("stop hook failed");
    }

    @Override
    public void afterStop() {
      events.add("failingStop.afterStop");
    }
  }
}

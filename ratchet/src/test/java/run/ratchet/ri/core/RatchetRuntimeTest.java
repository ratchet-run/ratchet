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
package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.ri.core.internal.*;
import run.ratchet.spi.*;

class RatchetRuntimeTest {
  @Test
  void partialStartupFailureDrainsAndReleasesHooksExactlyOnce() {
    Poller poller = mock(Poller.class);
    RecurringScheduler recurring = mock(RecurringScheduler.class);
    DrainController drain = mock(DrainController.class);
    SchedulerLifecycleHook hook = mock(SchedulerLifecycleHook.class);
    ExecutorProvider executors = mock(ExecutorProvider.class);
    when(executors.getScheduledExecutor()).thenReturn(mock(ScheduledExecutorService.class));
    doThrow(new IllegalStateException("start failed")).when(recurring).init();
    AtomicInteger release = new AtomicInteger();
    RatchetRuntime runtime =
        new RatchetRuntime(
            poller,
            recurring,
            mock(OrphanRecoveryTimer.class),
            mock(BatchRecoveryTimer.class),
            mock(DeadLetterService.class),
            mock(JobArchivingService.class),
            mock(LogPurgeTimer.class),
            mock(PollerWakeupListener.class),
            executors,
            mock(NodeIdentityProvider.class),
            drain,
            RatchetOptions.defaults(),
            mock(JobExecutionCoordinator.class),
            mock(ClusterCoordinator.class),
            () -> List.of(hook),
            h -> release.incrementAndGet(),
            () -> {});
    assertThrows(IllegalStateException.class, runtime::start);
    runtime.onShutdown();
    verify(poller).init();
    verify(poller).stop();
    verify(drain).setDraining(true);
    verify(hook).beforeStart();
    verify(hook, never()).afterStart();
    verify(hook, never()).beforeStop();
    verify(hook, never()).afterStop();
    assertEquals(1, release.get());
    assertThrows(IllegalStateException.class, runtime::start);
  }

  @Test
  void afterStartErrorStillStopsHooksThatAlreadyCompletedStartup() {
    SchedulerLifecycleHook first = mock(SchedulerLifecycleHook.class);
    SchedulerLifecycleHook failing = mock(SchedulerLifecycleHook.class);
    SchedulerLifecycleHook last = mock(SchedulerLifecycleHook.class);
    AssertionError failure = new AssertionError("afterStart failed");
    doThrow(failure).when(failing).afterStart();
    RatchetRuntime runtime = runtimeWithHooks(List.of(first, failing, last));

    assertSame(failure, assertThrows(AssertionError.class, runtime::start));
    runtime.onShutdown();

    var order = inOrder(first, failing, last);
    order.verify(first).beforeStart();
    order.verify(failing).beforeStart();
    order.verify(last).beforeStart();
    order.verify(first).afterStart();
    order.verify(failing).afterStart();
    order.verify(first).beforeStop();
    order.verify(first).afterStop();
    verify(first, times(1)).beforeStop();
    verify(first, times(1)).afterStop();
    verify(failing, never()).beforeStop();
    verify(failing, never()).afterStop();
    verify(last, never()).afterStart();
    verify(last, never()).beforeStop();
    verify(last, never()).afterStop();
  }

  @Test
  void successfulStartupPairsEveryHookWithShutdownExactlyOnce() {
    SchedulerLifecycleHook first = mock(SchedulerLifecycleHook.class);
    SchedulerLifecycleHook second = mock(SchedulerLifecycleHook.class);
    RatchetRuntime runtime = runtimeWithHooks(List.of(first, second));

    runtime.start();
    verify(first, never()).beforeStop();
    verify(second, never()).beforeStop();
    runtime.onShutdown();
    runtime.onShutdown();

    for (SchedulerLifecycleHook hook : List.of(first, second)) {
      var order = inOrder(hook);
      order.verify(hook).beforeStart();
      order.verify(hook).afterStart();
      order.verify(hook).beforeStop();
      order.verify(hook).afterStop();
      verifyNoMoreInteractions(hook);
    }
  }

  private static RatchetRuntime runtimeWithHooks(List<SchedulerLifecycleHook> hooks) {
    ExecutorProvider executors = mock(ExecutorProvider.class);
    when(executors.getScheduledExecutor()).thenReturn(mock(ScheduledExecutorService.class));
    return new RatchetRuntime(
        mock(Poller.class),
        mock(RecurringScheduler.class),
        mock(OrphanRecoveryTimer.class),
        mock(BatchRecoveryTimer.class),
        mock(DeadLetterService.class),
        mock(JobArchivingService.class),
        mock(LogPurgeTimer.class),
        mock(PollerWakeupListener.class),
        executors,
        mock(NodeIdentityProvider.class),
        mock(DrainController.class),
        RatchetOptions.defaults(),
        mock(JobExecutionCoordinator.class),
        mock(ClusterCoordinator.class),
        () -> hooks,
        hook -> {},
        () -> {});
  }

  @Test
  void shutdownDrainsAcceptedWorkBeforeCancellationAndReset() throws Exception {
    JobExecutorService executor = mock(JobExecutorService.class);
    RetryBufferDrainer retryBuffer = mock(RetryBufferDrainer.class);
    JobStateManager state = mock(JobStateManager.class);
    JobExecutionCoordinator coordinator =
        new JobExecutionCoordinator(mock(JobSubmissionService.class), state, retryBuffer, executor);
    Duration timeout = Duration.ofSeconds(12);
    when(executor.awaitIdle(timeout)).thenReturn(true);
    coordinator.shutdown(timeout);
    var order = inOrder(retryBuffer, executor, state);
    order.verify(retryBuffer).shutdown();
    order.verify(executor).awaitIdle(timeout);
    order.verify(executor).shutdownActiveExecutions();
    order.verify(state).resetRunningJobsForNode();
  }
}

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

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.core.internal.*;
import run.ratchet.ri.core.internal.JobStateFlusher;
import run.ratchet.spi.*;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.OrphanRecovery;

class RatchetRuntimeTest {
  @Test
  void rejectsReleaseExecutorSharedWithScheduler() {
    ExecutorProvider executors = mock(ExecutorProvider.class);
    ScheduledExecutorService shared = mock(ScheduledExecutorService.class);
    when(executors.getScheduledExecutor()).thenReturn(shared);
    when(executors.getReleaseExecutor()).thenReturn(shared);
    RatchetRuntime runtime =
        runtimeWithNode(
            () -> "node",
            mock(NodeLiveness.class),
            mock(OrphanRecoveryTimer.class),
            mock(Poller.class),
            mock(JobExecutionCoordinator.class),
            executors);
    assertTrue(
        assertThrows(IllegalStateException.class, runtime::start)
            .getMessage()
            .contains("getReleaseExecutor()"));
  }

  @Test
  void unavailableReleaseExecutorStillStartsHeartbeatAndLogsOnce() {
    ExecutorProvider executors = mock(ExecutorProvider.class);
    ScheduledExecutorService scheduled = mock(ScheduledExecutorService.class);
    RuntimeException failure = new IllegalStateException("managed thread factory unavailable");
    when(executors.getScheduledExecutor()).thenReturn(scheduled);
    when(executors.getReleaseExecutor()).thenThrow(failure);
    NodeLiveness liveness = mock(NodeLiveness.class);
    Poller poller = mock(Poller.class);
    RatchetRuntime runtime =
        runtimeWithNode(
            () -> "node",
            liveness,
            mock(OrphanRecoveryTimer.class),
            poller,
            mock(JobExecutionCoordinator.class),
            executors);
    Logger logger = Logger.getLogger(RatchetRuntime.class.getName());
    List<LogRecord> records = new ArrayList<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            if (record.getMessage().contains("Release retries are unavailable")) {
              records.add(record);
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.ALL);
    logger.addHandler(handler);
    try {
      assertDoesNotThrow(runtime::start);
      assertDoesNotThrow(runtime::start);
      verify(liveness).startHeartbeat(scheduled);
      verify(poller).init();
      verify(executors).getReleaseExecutor();
      assertEquals(1, records.size());
      assertEquals(Level.SEVERE, records.get(0).getLevel());
      assertSame(failure, records.get(0).getThrown());
    } finally {
      runtime.onShutdown();
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
    }
  }

  @Test
  void nullReleaseExecutorStillStarts() {
    ExecutorProvider executors = mock(ExecutorProvider.class);
    when(executors.getScheduledExecutor()).thenReturn(mock(ScheduledExecutorService.class));
    RatchetRuntime runtime =
        runtimeWithNode(
            () -> "node",
            mock(NodeLiveness.class),
            mock(OrphanRecoveryTimer.class),
            mock(Poller.class),
            mock(JobExecutionCoordinator.class),
            executors);
    try {
      assertDoesNotThrow(runtime::start);
    } finally {
      runtime.onShutdown();
    }
  }

  @Test
  void defaultNodeCleansStartupPermitsAfterInitBeforePolling() {
    DefaultNodeIdentityProvider node = mock(DefaultNodeIdentityProvider.class);
    when(node.getNodeId()).thenReturn("node-1");
    OrphanRecoveryTimer recovery = mock(OrphanRecoveryTimer.class);
    Poller poller = mock(Poller.class);
    NodeLiveness liveness = mock(NodeLiveness.class);
    RatchetRuntime runtime = runtimeWithNode(node, liveness, recovery, poller);

    try {
      runtime.start();
      var order = inOrder(liveness, recovery, poller);
      order.verify(liveness).init();
      order.verify(recovery).cleanupOwnPermitsAtStartup("node-1");
      order.verify(poller).init();
    } finally {
      runtime.onShutdown();
    }
  }

  @Test
  void customNodeLivenessFollowsStartupAndShutdownOrdering() {
    NodeIdentityProvider node = () -> "custom-1";
    NodeStore nodes = mock(NodeStore.class);
    JobBulkStore jobs = mock(JobBulkStore.class);
    when(jobs.resetOrphanJobsForNode(eq("custom-1"), eq(3), anyInt()))
        .thenReturn(new OrphanRecovery(0, 0, 0, List.of()));
    when(jobs.resetOrphanJobsBefore(any(), eq(3), anyInt()))
        .thenReturn(new OrphanRecovery(0, 0, 0, List.of()));
    NodeLiveness liveness =
        spy(
            new NodeLiveness(
                node,
                nodes,
                jobs,
                mock(DynamicHeartbeatCalculator.class),
                10,
                60,
                3,
                false,
                Clock.systemUTC()));
    OrphanRecoveryTimer recovery = mock(OrphanRecoveryTimer.class);
    Poller poller = mock(Poller.class);
    JobExecutionCoordinator claims = mock(JobExecutionCoordinator.class);
    RatchetRuntime runtime = runtimeWithNode(node, liveness, recovery, poller, claims);
    try {
      runtime.start();
      var order = inOrder(nodes, jobs, recovery, liveness, poller);
      order.verify(nodes).upsertHeartbeat(eq("custom-1"), any());
      order.verify(jobs).resetOrphanJobsForNode(eq("custom-1"), eq(3), anyInt());
      order.verify(recovery).cleanupOwnPermitsAtStartup("custom-1");
      order.verify(liveness).startHeartbeat(any(ScheduledExecutorService.class));
      order.verify(poller).init();
    } finally {
      runtime.onShutdown();
    }
    var stopOrder = inOrder(claims, liveness);
    stopOrder.verify(claims).shutdown(Duration.ZERO);
    stopOrder.verify(liveness).shutdown();
  }

  @Test
  void unavailableScheduledExecutorKeepsStartupRunningWithRealLiveness() {
    NodeIdentityProvider node = () -> "custom-1";
    NodeStore nodes = mock(NodeStore.class);
    JobBulkStore jobs = mock(JobBulkStore.class);
    ExecutorProvider executors = mock(ExecutorProvider.class);
    when(executors.getScheduledExecutor())
        .thenThrow(new IllegalStateException("java:comp unbound"));
    when(jobs.resetOrphanJobsForNode(eq("custom-1"), eq(3), anyInt()))
        .thenReturn(new OrphanRecovery(0, 0, 0, List.of()));
    when(jobs.resetOrphanJobsBefore(any(), eq(3), anyInt()))
        .thenReturn(new OrphanRecovery(0, 0, 0, List.of()));
    NodeLiveness liveness =
        new NodeLiveness(
            node,
            nodes,
            jobs,
            mock(DynamicHeartbeatCalculator.class),
            10,
            60,
            3,
            false,
            Clock.systemUTC());
    OrphanRecoveryTimer recovery = mock(OrphanRecoveryTimer.class);
    Poller poller = mock(Poller.class);
    RatchetRuntime runtime =
        runtimeWithNode(
            node, liveness, recovery, poller, mock(JobExecutionCoordinator.class), executors);
    try {
      assertDoesNotThrow(runtime::start);
      verify(nodes).upsertHeartbeat(eq("custom-1"), any());
      verify(jobs).resetOrphanJobsForNode(eq("custom-1"), eq(3), anyInt());
      verify(recovery).cleanupOwnPermitsAtStartup("custom-1");
      verify(poller, never()).init();
    } finally {
      runtime.onShutdown();
    }
    verify(executors, times(1)).getScheduledExecutor();
  }

  @Test
  void startupPermitCleanupFailureStopsStartupBeforePolling() {
    OrphanRecoveryTimer recovery = mock(OrphanRecoveryTimer.class);
    Poller poller = mock(Poller.class);
    var failure = new RatchetTransientStoreException("cleanup failed");
    doThrow(failure).when(recovery).cleanupOwnPermitsAtStartup("custom-1");
    RatchetRuntime runtime =
        runtimeWithNode(() -> "custom-1", mock(NodeLiveness.class), recovery, poller);

    assertSame(failure, assertThrows(RatchetTransientStoreException.class, runtime::start));
    verify(poller, never()).init();
    verify(poller).stop();
  }

  private static RatchetRuntime runtimeWithNode(
      NodeIdentityProvider node,
      NodeLiveness liveness,
      OrphanRecoveryTimer recovery,
      Poller poller) {
    return runtimeWithNode(node, liveness, recovery, poller, mock(JobExecutionCoordinator.class));
  }

  private static RatchetRuntime runtimeWithNode(
      NodeIdentityProvider node,
      NodeLiveness liveness,
      OrphanRecoveryTimer recovery,
      Poller poller,
      JobExecutionCoordinator claims) {
    ExecutorProvider executors = mock(ExecutorProvider.class);
    when(executors.getScheduledExecutor()).thenReturn(mock(ScheduledExecutorService.class));
    return runtimeWithNode(node, liveness, recovery, poller, claims, executors);
  }

  private static RatchetRuntime runtimeWithNode(
      NodeIdentityProvider node,
      NodeLiveness liveness,
      OrphanRecoveryTimer recovery,
      Poller poller,
      JobExecutionCoordinator claims,
      ExecutorProvider executors) {
    return new RatchetRuntime(
        poller,
        mock(RecurringScheduler.class),
        recovery,
        mock(BatchRecoveryTimer.class),
        mock(CancelRequestRelay.class),
        mock(JobStateFlusher.class),
        mock(DeadLetterService.class),
        null,
        mock(JobArchivingService.class),
        mock(LogPurgeTimer.class),
        mock(PollerWakeupListener.class),
        executors,
        node,
        liveness,
        mock(DrainController.class),
        RatchetOptions.defaults(),
        claims,
        mock(ClusterCoordinator.class),
        List::of,
        hook -> {},
        () -> {});
  }

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
            mock(CancelRequestRelay.class),
            mock(JobStateFlusher.class),
            mock(DeadLetterService.class),
            null,
            mock(JobArchivingService.class),
            mock(LogPurgeTimer.class),
            mock(PollerWakeupListener.class),
            executors,
            mock(NodeIdentityProvider.class),
            mock(NodeLiveness.class),
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
        mock(CancelRequestRelay.class),
        mock(JobStateFlusher.class),
        mock(DeadLetterService.class),
        null,
        mock(JobArchivingService.class),
        mock(LogPurgeTimer.class),
        mock(PollerWakeupListener.class),
        executors,
        mock(NodeIdentityProvider.class),
        mock(NodeLiveness.class),
        mock(DrainController.class),
        RatchetOptions.defaults(),
        mock(JobExecutionCoordinator.class),
        mock(ClusterCoordinator.class),
        () -> hooks,
        hook -> {},
        () -> {});
  }

  @Test
  void shutdownDrainsAcceptedWorkBeforeResetAndCancellation() throws Exception {
    JobExecutorService executor = mock(JobExecutorService.class);
    RetryBufferDrainer retryBuffer = mock(RetryBufferDrainer.class);
    JobStateManager state = mock(JobStateManager.class);
    JobExecutionCoordinator coordinator =
        new JobExecutionCoordinator(
            mock(JobSubmissionService.class),
            state,
            retryBuffer,
            executor,
            mock(JobStateFlusher.class));
    Duration timeout = Duration.ofSeconds(12);
    when(executor.awaitIdle(timeout)).thenReturn(true);
    coordinator.shutdown(timeout);
    var order = inOrder(retryBuffer, executor, state);
    order.verify(retryBuffer).shutdown();
    order.verify(executor).awaitIdle(timeout);
    order.verify(state).resetRunningJobsForNode();
    order.verify(executor).shutdownActiveExecutions();
  }

  @Test
  void start_startsCancelRelayOnTheScheduledExecutor_andShutdownStopsIt() {
    // The runtime starts the cancel relay on its scheduled executor and stops it at shutdown.
    ExecutorProvider executors = mock(ExecutorProvider.class);
    ScheduledExecutorService scheduled = mock(ScheduledExecutorService.class);
    when(executors.getScheduledExecutor()).thenReturn(scheduled);
    CancelRequestRelay relay = mock(CancelRequestRelay.class);
    RatchetRuntime runtime =
        new RatchetRuntime(
            mock(Poller.class),
            mock(RecurringScheduler.class),
            mock(OrphanRecoveryTimer.class),
            mock(BatchRecoveryTimer.class),
            relay,
            mock(JobStateFlusher.class),
            mock(DeadLetterService.class),
            null,
            mock(JobArchivingService.class),
            mock(LogPurgeTimer.class),
            mock(PollerWakeupListener.class),
            executors,
            () -> "node-1",
            mock(NodeLiveness.class),
            mock(DrainController.class),
            RatchetOptions.defaults(),
            mock(JobExecutionCoordinator.class),
            mock(ClusterCoordinator.class),
            List::of,
            hook -> {},
            () -> {});
    try {
      runtime.start();
      verify(relay).start(scheduled);
    } finally {
      runtime.onShutdown();
    }
    verify(relay).stop();
  }

  @Test
  void shutdown_keepsTheCancelRelayRunningWhileTheCoordinatorDrains() {
    // Cancels from other nodes still reach running bodies during the shutdown drain.
    ExecutorProvider executors = mock(ExecutorProvider.class);
    when(executors.getScheduledExecutor()).thenReturn(mock(ScheduledExecutorService.class));
    CancelRequestRelay relay = mock(CancelRequestRelay.class);
    JobExecutionCoordinator coordinator = mock(JobExecutionCoordinator.class);
    RatchetRuntime runtime =
        new RatchetRuntime(
            mock(Poller.class),
            mock(RecurringScheduler.class),
            mock(OrphanRecoveryTimer.class),
            mock(BatchRecoveryTimer.class),
            relay,
            mock(JobStateFlusher.class),
            mock(DeadLetterService.class),
            null,
            mock(JobArchivingService.class),
            mock(LogPurgeTimer.class),
            mock(PollerWakeupListener.class),
            executors,
            () -> "node-1",
            mock(NodeLiveness.class),
            mock(DrainController.class),
            RatchetOptions.defaults(),
            coordinator,
            mock(ClusterCoordinator.class),
            List::of,
            hook -> {},
            () -> {});
    runtime.start();
    runtime.onShutdown();
    var order = inOrder(coordinator, relay);
    order.verify(coordinator).shutdown(any());
    order.verify(relay).stop();
  }

  @Test
  void start_releasesOwnHoldsRightAfterOwnPermits() {
    // Startup releases this node's held keys immediately after its permits, before polling.
    OrphanRecoveryTimer recovery = mock(OrphanRecoveryTimer.class);
    Poller poller = mock(Poller.class);
    RatchetRuntime runtime =
        runtimeWithNode(() -> "node-1", mock(NodeLiveness.class), recovery, poller);
    try {
      runtime.start();
      var order = inOrder(recovery, poller);
      order.verify(recovery).cleanupOwnPermitsAtStartup("node-1");
      order.verify(recovery).releaseOwnBusinessKeyHoldsAtStartup("node-1");
      order.verify(poller).init();
    } finally {
      runtime.onShutdown();
    }
  }
}

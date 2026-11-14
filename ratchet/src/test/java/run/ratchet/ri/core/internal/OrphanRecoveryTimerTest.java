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
package run.ratchet.ri.core.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.NodeEntity;
import run.ratchet.store.spi.ExhaustedOrphan;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.OrphanRecovery;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.ResourcePermitStore.WokenWaiter;

@ExtendWith(MockitoExtension.class)
class OrphanRecoveryTimerTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private JobBulkStore jobBulkStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private PostExecutionHandler lifecycleFacade;
  @Mock private BiConsumer<JobEntity, Throwable> callbackInvoker;
  @Mock private ErrorSanitizer errorSanitizer;
  @Mock private NodeStore nodeStore;
  @Mock private ResourcePermitService resourcePermitService;

  private OrphanRecoveryTimer timer;

  @BeforeEach
  void setUp() {
    lenient()
        .when(jobBulkStore.resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100)))
        .thenReturn(new OrphanRecovery(0, List.of()));
    lenient()
        .when(errorSanitizer.sanitize(any(Throwable.class)))
        .thenAnswer(invocation -> ((Throwable) invocation.getArgument(0)).getMessage());
    timer =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            null,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            120,
            FIXED_CLOCK);
  }

  @Test
  void startupCleanupWakesAfterCommitOrImmediatelyWithoutTransaction() {
    for (var result :
        List.of(
            AfterCommitRegistrar.Result.REGISTERED,
            AfterCommitRegistrar.Result.NO_ACTIVE_TRANSACTION)) {
      ResourcePermitStore store = mock(ResourcePermitStore.class);
      JobWakeupService wakeup = mock(JobWakeupService.class);
      List<Runnable> callbacks = new ArrayList<>();
      AfterCommitRegistrar registrar =
          action -> {
            callbacks.add(action);
            return result;
          };
      when(store.cleanupOrphanedPermits(List.of("node-1"))).thenReturn(1);
      when(store.findResourcesWithWaiters(null, 64)).thenReturn(List.of("api"));
      when(store.wakeWaiters("api"))
          .thenReturn(List.of(new WokenWaiter(UUID.randomUUID(), 3, "workers")));
      var service = new DefaultResourcePermitService(store, wakeup, registrar);
      var configured =
          new OrphanRecoveryTimer(
              jobBulkStore,
              jobCrudStore,
              nodeStore,
              service,
              null,
              lifecycleFacade,
              callbackInvoker,
              errorSanitizer,
              3,
              60,
              120,
              FIXED_CLOCK);

      assertEquals(1, configured.cleanupOwnPermitsAtStartup("node-1"));
      if (result == AfterCommitRegistrar.Result.REGISTERED) {
        verify(store, never()).wakeWaiters(any(String.class));
        callbacks.forEach(Runnable::run);
      }
      verify(store).wakeWaiters("api");
      verify(wakeup).notify(JobPriority.HIGH, true, "workers");
    }
  }

  @Test
  void sweepRunsEveryLeasedScanAndFailureDoesNotStopCleanup() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    LockStore locks = mock(LockStore.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(120)))
        .thenAnswer(inv -> Optional.of(new SingletonLease(locks, "orphanRecovery", "node-1")));
    doThrow(new RatchetTransientStoreException("sweep unavailable"))
        .doNothing()
        .when(resourcePermitService)
        .sweepWaiters();
    var configured =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            leases,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            120,
            FIXED_CLOCK);

    configured.recoverNow();
    configured.recoverNow();

    verify(resourcePermitService, times(2)).sweepWaiters();
    verify(jobBulkStore, times(2)).closeStaleExecutions(FIXED_NOW.minusSeconds(60), 500);
    verify(locks, times(2)).unlock("orphanRecovery", "node-1");
  }

  @Test
  void cleanupOwnPermitsAtStartupDelegatesAndReturnsCount() {
    when(resourcePermitService.cleanupOrphanedPermits(List.of("node-1"))).thenReturn(4);

    assertEquals(4, timer.cleanupOwnPermitsAtStartup("node-1"));
    verify(resourcePermitService).cleanupOrphanedPermits(List.of("node-1"));
  }

  @Test
  void cleanupOwnPermitsAtStartupRetriesTransientFailures() {
    when(resourcePermitService.cleanupOrphanedPermits(List.of("node-1")))
        .thenThrow(new RatchetTransientStoreException("first"))
        .thenThrow(new RatchetTransientStoreException("second"))
        .thenReturn(2);

    assertEquals(2, timer.cleanupOwnPermitsAtStartup("node-1"));
    verify(resourcePermitService, times(3)).cleanupOrphanedPermits(List.of("node-1"));
  }

  @Test
  void cleanupOwnPermitsAtStartupStopsAfterThreeTransientFailures() {
    var failure = new RatchetTransientStoreException("unavailable");
    when(resourcePermitService.cleanupOrphanedPermits(List.of("node-1"))).thenThrow(failure);

    assertEquals(
        failure,
        assertThrows(
            RatchetTransientStoreException.class,
            () -> timer.cleanupOwnPermitsAtStartup("node-1")));
    verify(resourcePermitService, times(3)).cleanupOrphanedPermits(List.of("node-1"));
  }

  @Test
  void cleanupOwnPermitsAtStartupDoesNotRetryOtherFailures() {
    var failure = new IllegalStateException("invalid state");
    when(resourcePermitService.cleanupOrphanedPermits(List.of("node-1"))).thenThrow(failure);

    assertEquals(
        failure,
        assertThrows(
            IllegalStateException.class, () -> timer.cleanupOwnPermitsAtStartup("node-1")));
    verify(resourcePermitService).cleanupOrphanedPermits(List.of("node-1"));
  }

  @Test
  void staleExecutionFailureDoesNotUndoRecovery() {
    Instant cutoff = FIXED_NOW.minusSeconds(60);
    when(jobBulkStore.closeStaleExecutions(cutoff, 500))
        .thenThrow(new IllegalStateException("cleanup failed"));
    NodeEntity stale = new NodeEntity();
    stale.setId("dead");
    when(nodeStore.findInactiveNodesSince(cutoff)).thenReturn(List.of(stale));
    try (LogCapture logs = LogCapture.start(OrphanRecoveryTimer.class)) {
      timer.recoverNow();
      assertTrue(
          logs.records().stream()
              .anyMatch(
                  record ->
                      record.getLevel().equals(Level.WARNING)
                          && record.getMessage().equals("Stale execution cleanup failed")
                          && record.getThrown() instanceof IllegalStateException));
      assertTrue(
          logs.records().stream()
              .noneMatch(record -> record.getMessage().equals("Orphan recovery scan failed")));
    }
    InOrder order = inOrder(jobBulkStore, resourcePermitService, nodeStore);
    order.verify(jobBulkStore).resetOrphanJobsBefore(cutoff, 3, 100);
    order.verify(nodeStore).findInactiveNodesSince(cutoff);
    order.verify(resourcePermitService).cleanupOrphanedPermits(List.of("dead"));
    order.verify(nodeStore).deleteInactiveNodesByIds(List.of("dead"));
    order.verify(jobBulkStore).closeStaleExecutions(cutoff, 500);
  }

  @Test
  void staleExecutionCleanupLogsClosedCount() {
    when(jobBulkStore.closeStaleExecutions(FIXED_NOW.minusSeconds(60), 500)).thenReturn(2);
    try (LogCapture logs = LogCapture.start(OrphanRecoveryTimer.class)) {
      timer.recoverNow();
      assertTrue(
          logs.records().stream()
              .anyMatch(
                  record ->
                      record.getLevel().equals(Level.INFO)
                          && record
                              .getMessage()
                              .equals("Orphan recovery: closed 2 stale execution(s)")));
    }
  }

  @Test
  void recoveryFailureStillClosesStaleExecutions() {
    Instant cutoff = FIXED_NOW.minusSeconds(60);
    when(jobBulkStore.resetOrphanJobsBefore(cutoff, 3, 100))
        .thenThrow(new IllegalStateException("reset failed"));
    try (LogCapture logs = LogCapture.start(OrphanRecoveryTimer.class)) {
      timer.recoverNow();
      assertTrue(
          logs.records().stream()
              .anyMatch(
                  record ->
                      record.getMessage().equals("Orphan recovery scan failed")
                          && record.getLevel().equals(Level.SEVERE)
                          && record.getThrown() instanceof IllegalStateException));
    }
    verify(jobBulkStore).closeStaleExecutions(cutoff, 500);
    verify(nodeStore, never()).findInactiveNodesSince(any(Instant.class));
  }

  @Test
  void transientFailureInsideLeaseLogsWarningAndReleasesLease() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    LockStore locks = mock(LockStore.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(120)))
        .thenReturn(Optional.of(new SingletonLease(locks, "orphanRecovery", "node-1")));
    var failure = new RatchetTransientStoreException("deadlock");
    when(jobBulkStore.resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, 100)).thenThrow(failure);
    var configured =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            leases,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            120,
            FIXED_CLOCK);
    try (LogCapture logs = LogCapture.start(OrphanRecoveryTimer.class)) {
      configured.recoverNow();
      assertTrue(
          logs.records().stream()
              .anyMatch(
                  record ->
                      record.getLevel().equals(Level.WARNING)
                          && record.getMessage().contains("the next scan will retry")
                          && record.getThrown() == failure));
      assertFalse(
          logs.records().stream().anyMatch(record -> record.getLevel().equals(Level.SEVERE)));
    }
    verify(locks).unlock("orphanRecovery", "node-1");
    verify(jobBulkStore).resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, 100);
  }

  @Test
  void configuredLeaseTtlHasNoFloorAndIsIndependentOfScanInterval() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    LockStore locks = mock(LockStore.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(1)))
        .thenReturn(Optional.of(new SingletonLease(locks, "orphanRecovery", "node-1")));
    OrphanRecoveryTimer configured =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            leases,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            1,
            FIXED_CLOCK);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    configured.start(executor, 300);
    configured.recoverNow();
    verify(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(300L), eq(300L), eq(TimeUnit.SECONDS));
    verify(leases).tryAcquire("orphanRecovery", Duration.ofSeconds(1));
    verify(locks).unlock("orphanRecovery", "node-1");
  }

  @Test
  void staleHolderLeaseIsReleasedAndScanRuns() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    LockStore locks = mock(LockStore.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(120)))
        .thenReturn(
            Optional.empty(), Optional.of(new SingletonLease(locks, "orphanRecovery", "node-1")));
    NodeEntity staleNode = new NodeEntity();
    staleNode.setId("dead");
    staleNode.setLastHeartbeat(Instant.parse("2026-05-12T11:50:00Z"));
    when(nodeStore.findInactiveNodesSince(FIXED_NOW.minusSeconds(60)))
        .thenReturn(List.of(staleNode));
    when(nodeStore.findNodeById("dead")).thenReturn(Optional.of(staleNode));
    when(leases.releaseHeldBy("orphanRecovery", "dead")).thenReturn(true);
    OrphanRecoveryTimer configured =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            leases,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            120,
            FIXED_CLOCK);

    configured.recoverNow();

    verify(leases).releaseHeldBy("orphanRecovery", "dead");
    verify(leases, times(2)).tryAcquire("orphanRecovery", Duration.ofSeconds(120));
    verify(jobBulkStore).resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, 100);
    verify(locks).unlock("orphanRecovery", "node-1");
  }

  @Test
  void noStaleNodesSkipsWithoutRelease() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(120))).thenReturn(Optional.empty());
    when(nodeStore.findInactiveNodesSince(FIXED_NOW.minusSeconds(60))).thenReturn(List.of());
    OrphanRecoveryTimer configured =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            leases,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            120,
            FIXED_CLOCK);

    configured.recoverNow();

    verify(leases, never()).releaseHeldBy(any(String.class), any(String.class));
    verify(leases, times(1)).tryAcquire("orphanRecovery", Duration.ofSeconds(120));
    verify(jobBulkStore, never()).resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100));
    verify(jobBulkStore, never()).closeStaleExecutions(any(Instant.class), eq(500));
  }

  @Test
  void freshHeartbeatOnRecheckIsNotReleased() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(120))).thenReturn(Optional.empty());
    NodeEntity staleNode = new NodeEntity();
    staleNode.setId("restarted");
    staleNode.setLastHeartbeat(Instant.parse("2026-05-12T11:50:00Z"));
    when(nodeStore.findInactiveNodesSince(FIXED_NOW.minusSeconds(60)))
        .thenReturn(List.of(staleNode));
    NodeEntity restartedNode = new NodeEntity();
    restartedNode.setId("restarted");
    restartedNode.setLastHeartbeat(Instant.parse("2026-05-12T11:59:30Z"));
    when(nodeStore.findNodeById("restarted")).thenReturn(Optional.of(restartedNode));
    OrphanRecoveryTimer configured =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            leases,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            120,
            FIXED_CLOCK);

    configured.recoverNow();

    verify(leases, never()).releaseHeldBy(any(String.class), any(String.class));
    verify(leases, times(1)).tryAcquire("orphanRecovery", Duration.ofSeconds(120));
    verify(jobBulkStore, never()).resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100));
    verify(jobBulkStore, never()).closeStaleExecutions(any(Instant.class), eq(500));
  }

  @Test
  void missingRowOnRecheckIsNotReleased() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(120))).thenReturn(Optional.empty());
    NodeEntity staleNode = new NodeEntity();
    staleNode.setId("dead");
    staleNode.setLastHeartbeat(Instant.parse("2026-05-12T11:50:00Z"));
    when(nodeStore.findInactiveNodesSince(FIXED_NOW.minusSeconds(60)))
        .thenReturn(List.of(staleNode));
    when(nodeStore.findNodeById("dead")).thenReturn(Optional.empty());
    OrphanRecoveryTimer configured =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            leases,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            120,
            FIXED_CLOCK);

    configured.recoverNow();

    verify(leases, never()).releaseHeldBy(any(String.class), any(String.class));
    verify(leases, times(1)).tryAcquire("orphanRecovery", Duration.ofSeconds(120));
    verify(jobBulkStore, never()).resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100));
    verify(jobBulkStore, never()).closeStaleExecutions(any(Instant.class), eq(500));
  }

  @Test
  void leaseStillHeldAfterReleaseSkipsScan() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(120))).thenReturn(Optional.empty());
    NodeEntity staleNode = new NodeEntity();
    staleNode.setId("dead");
    staleNode.setLastHeartbeat(Instant.parse("2026-05-12T11:50:00Z"));
    when(nodeStore.findInactiveNodesSince(FIXED_NOW.minusSeconds(60)))
        .thenReturn(List.of(staleNode));
    when(nodeStore.findNodeById("dead")).thenReturn(Optional.of(staleNode));
    when(leases.releaseHeldBy("orphanRecovery", "dead")).thenReturn(true);
    OrphanRecoveryTimer configured =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            leases,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            120,
            FIXED_CLOCK);

    configured.recoverNow();

    verify(leases).releaseHeldBy("orphanRecovery", "dead");
    verify(leases, times(2)).tryAcquire("orphanRecovery", Duration.ofSeconds(120));
    verify(jobBulkStore, never()).resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100));
    verify(jobBulkStore, never()).closeStaleExecutions(any(Instant.class), eq(500));
  }

  @Test
  void failedReleaseSkipsRetryAndScan() {
    SingletonLeaseService leases = mock(SingletonLeaseService.class);
    when(leases.tryAcquire("orphanRecovery", Duration.ofSeconds(120))).thenReturn(Optional.empty());
    NodeEntity staleNode = new NodeEntity();
    staleNode.setId("dead");
    staleNode.setLastHeartbeat(Instant.parse("2026-05-12T11:50:00Z"));
    when(nodeStore.findInactiveNodesSince(FIXED_NOW.minusSeconds(60)))
        .thenReturn(List.of(staleNode));
    when(nodeStore.findNodeById("dead")).thenReturn(Optional.of(staleNode));
    when(leases.releaseHeldBy("orphanRecovery", "dead")).thenReturn(false);
    OrphanRecoveryTimer configured =
        new OrphanRecoveryTimer(
            jobBulkStore,
            jobCrudStore,
            nodeStore,
            resourcePermitService,
            leases,
            lifecycleFacade,
            callbackInvoker,
            errorSanitizer,
            3,
            60,
            120,
            FIXED_CLOCK);

    configured.recoverNow();

    verify(leases).releaseHeldBy("orphanRecovery", "dead");
    verify(leases, times(1)).tryAcquire("orphanRecovery", Duration.ofSeconds(120));
    verify(jobBulkStore, never()).resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100));
    verify(jobBulkStore, never()).closeStaleExecutions(any(Instant.class), eq(500));
  }

  @Test
  void constructor_rejectsNullJobBulkStore() {
    assertThrows(
        NullPointerException.class,
        () ->
            new OrphanRecoveryTimer(
                null,
                jobCrudStore,
                nodeStore,
                resourcePermitService,
                null,
                lifecycleFacade,
                callbackInvoker,
                errorSanitizer,
                3,
                60,
                120,
                FIXED_CLOCK));
  }

  @Test
  void constructor_rejectsNullNodeStore() {
    assertThrows(
        NullPointerException.class,
        () ->
            new OrphanRecoveryTimer(
                jobBulkStore,
                jobCrudStore,
                null,
                resourcePermitService,
                null,
                lifecycleFacade,
                callbackInvoker,
                errorSanitizer,
                3,
                60,
                120,
                FIXED_CLOCK));
  }

  @Test
  void constructor_rejectsNullResourcePermitService() {
    assertThrows(
        NullPointerException.class,
        () ->
            new OrphanRecoveryTimer(
                jobBulkStore,
                jobCrudStore,
                nodeStore,
                null,
                null,
                lifecycleFacade,
                callbackInvoker,
                errorSanitizer,
                3,
                60,
                120,
                FIXED_CLOCK));
  }

  @Test
  void recoverOrphans_withStaleNodesCleansPermitsBeforeDeletingNodes() {
    NodeEntity staleNode = new NodeEntity();
    staleNode.setId("node-1");
    when(nodeStore.findInactiveNodesSince(any(Instant.class))).thenReturn(List.of(staleNode));

    timer.recoverNow();

    Instant cutoff = FIXED_NOW.minusSeconds(60);
    verify(jobBulkStore).resetOrphanJobsBefore(cutoff, 3, 100);
    verify(nodeStore).findInactiveNodesSince(cutoff);
    InOrder order = inOrder(resourcePermitService, nodeStore);
    order.verify(resourcePermitService).cleanupOrphanedPermits(List.of("node-1"));
    order.verify(nodeStore).deleteInactiveNodesByIds(List.of("node-1"));
  }

  @Test
  void recoveryOnlyResetsOrphansToPending() {
    timer.recoverNow();

    verify(jobBulkStore).resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, 100);
    verify(jobBulkStore).closeStaleExecutions(FIXED_NOW.minusSeconds(60), 500);
    verifyNoMoreInteractions(jobBulkStore, jobCrudStore, lifecycleFacade, callbackInvoker);
  }

  @Test
  void start_cancelsExistingHandleBeforeReplacingIt() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> first = mock(ScheduledFuture.class);
    ScheduledFuture<?> second = mock(ScheduledFuture.class);
    doReturn(first, second)
        .when(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));

    timer.start(executor, 1);
    timer.start(executor, 1);

    verify(first).cancel(false);
    verify(second, never()).cancel(false);
  }

  @Test
  void stop_clearsHandleBeforeCancelAndIsIdempotent() {
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> handle = mock(ScheduledFuture.class);
    doReturn(handle)
        .when(executor)
        .scheduleAtFixedRate(any(Runnable.class), eq(1L), eq(1L), eq(TimeUnit.SECONDS));

    timer.start(executor, 1);
    timer.stop();
    timer.stop();

    verify(handle, times(1)).cancel(false);
  }

  @Test
  void failsExhaustedClaimAndInvokesCallbackOnlyAfterCommit() {
    UUID id = UUID.randomUUID();
    JobEntity job = new JobEntity();
    job.setId(id);
    job.setClaimSeq(99L);
    when(jobBulkStore.resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100)))
        .thenReturn(new OrphanRecovery(0, List.of(new ExhaustedOrphan(id, 7, 3, "dead"))));
    when(jobCrudStore.findById(id)).thenReturn(Optional.of(job));
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);

    timer.recoverNow();

    assertEquals(7L, job.getClaimSeq());
    assertEquals(
        "Node dead stopped while running this job; crash redelivery limit (3) reached",
        job.getLastError());
    InOrder order = inOrder(lifecycleFacade, callbackInvoker);
    order.verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
    order.verify(callbackInvoker).accept(eq(job), any(Throwable.class));
  }

  @Test
  void fencedCompletionSkipsFailureCallback() {
    UUID id = UUID.randomUUID();
    JobEntity job = new JobEntity();
    job.setId(id);
    when(jobBulkStore.resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100)))
        .thenReturn(new OrphanRecovery(0, List.of(new ExhaustedOrphan(id, 7, 3, null))));
    when(jobCrudStore.findById(id)).thenReturn(Optional.of(job));

    timer.recoverNow();

    assertEquals(
        "Node unknown stopped while running this job; crash redelivery limit (3) reached",
        job.getLastError());
    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
    verifyNoMoreInteractions(callbackInvoker);
  }

  @Test
  void hydrationFailureUsesMetadataAndSkipsCallback() {
    UUID id = UUID.randomUUID();
    JobEntity snapshot = new JobEntity();
    snapshot.setId(id);
    snapshot.setJobType(JobExecutionType.BATCH_CHILD);
    snapshot.setDependsOn(UUID.randomUUID());
    when(jobBulkStore.resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100)))
        .thenReturn(new OrphanRecovery(0, List.of(new ExhaustedOrphan(id, 7, 3, null))));
    when(jobCrudStore.findById(id)).thenThrow(new IllegalStateException("decrypt failed"));
    when(jobBulkStore.findOrphanCompletionSnapshot(id)).thenReturn(Optional.of(snapshot));
    when(lifecycleFacade.completeFailure(snapshot, JobStatus.RUNNING, false)).thenReturn(true);

    timer.recoverNow();

    assertEquals(7L, snapshot.getClaimSeq());
    verify(lifecycleFacade).completeFailure(snapshot, JobStatus.RUNNING, false);
    verifyNoMoreInteractions(callbackInvoker);
  }

  @Test
  void failedCompletionDoesNotBlockLaterOrphansAndIsRetried() {
    UUID badId = UUID.randomUUID();
    UUID goodId = UUID.randomUUID();
    JobEntity bad = new JobEntity();
    bad.setId(badId);
    JobEntity good = new JobEntity();
    good.setId(goodId);
    when(jobBulkStore.resetOrphanJobsBefore(any(Instant.class), eq(3), eq(100)))
        .thenReturn(
            new OrphanRecovery(
                0,
                List.of(
                    new ExhaustedOrphan(badId, 1, 3, "dead"),
                    new ExhaustedOrphan(goodId, 2, 3, "dead"))));
    when(jobCrudStore.findById(badId)).thenReturn(Optional.of(bad));
    when(jobCrudStore.findById(goodId)).thenReturn(Optional.of(good));
    when(lifecycleFacade.completeFailure(bad, JobStatus.RUNNING, false))
        .thenThrow(new IllegalStateException("store unavailable"));

    timer.recoverNow();
    timer.recoverNow();

    verify(lifecycleFacade, times(2)).completeFailure(bad, JobStatus.RUNNING, false);
    verify(lifecycleFacade, times(2)).completeFailure(good, JobStatus.RUNNING, false);
  }

  private static final class LogCapture implements AutoCloseable {
    private final Logger logger;
    private final Handler handler;
    private final Level originalLevel;
    private final boolean originalUseParentHandlers;
    private final List<LogRecord> records = new ArrayList<>();

    private LogCapture(Class<?> type) {
      logger = Logger.getLogger(type.getName());
      originalLevel = logger.getLevel();
      originalUseParentHandlers = logger.getUseParentHandlers();
      handler =
          new Handler() {
            @Override
            public void publish(LogRecord record) {
              records.add(record);
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
          };
      logger.setLevel(Level.ALL);
      logger.setUseParentHandlers(false);
      logger.addHandler(handler);
    }

    static LogCapture start(Class<?> type) {
      return new LogCapture(type);
    }

    List<LogRecord> records() {
      return records;
    }

    @Override
    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
      logger.setUseParentHandlers(originalUseParentHandlers);
    }
  }
}

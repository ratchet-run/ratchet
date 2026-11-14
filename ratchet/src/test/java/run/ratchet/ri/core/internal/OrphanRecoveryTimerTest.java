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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
        .when(
            jobBulkStore.resetOrphanJobsBefore(
                any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenReturn(new OrphanRecovery(0, 0, 0, List.of()));
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
            FIXED_CLOCK,
            new Random(341));
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
    order
        .verify(jobBulkStore)
        .resetOrphanJobsBefore(cutoff, 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
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
    when(jobBulkStore.resetOrphanJobsBefore(cutoff, 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE))
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
    when(jobBulkStore.resetOrphanJobsBefore(
            FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE))
        .thenThrow(failure);
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
    verify(jobBulkStore)
        .resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
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
    verify(jobBulkStore)
        .resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
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
    verify(jobBulkStore, never())
        .resetOrphanJobsBefore(any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE));
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
    verify(jobBulkStore, never())
        .resetOrphanJobsBefore(any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE));
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
    verify(jobBulkStore, never())
        .resetOrphanJobsBefore(any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE));
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
    verify(jobBulkStore, never())
        .resetOrphanJobsBefore(any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE));
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
    verify(jobBulkStore, never())
        .resetOrphanJobsBefore(any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE));
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
    verify(jobBulkStore).resetOrphanJobsBefore(cutoff, 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
    verify(nodeStore).findInactiveNodesSince(cutoff);
    InOrder order = inOrder(resourcePermitService, nodeStore);
    order.verify(resourcePermitService).cleanupOrphanedPermits(List.of("node-1"));
    order.verify(nodeStore).deleteInactiveNodesByIds(List.of("node-1"));
  }

  @Test
  void recoveryOnlyResetsOrphansToPending() {
    timer.recoverNow();

    verify(jobBulkStore)
        .resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
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
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenReturn(new OrphanRecovery(1, 0, 0, List.of(new ExhaustedOrphan(id, 7, 3, "dead"))));
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
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenReturn(new OrphanRecovery(1, 0, 0, List.of(new ExhaustedOrphan(id, 7, 3, null))));
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
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenReturn(new OrphanRecovery(1, 0, 0, List.of(new ExhaustedOrphan(id, 7, 3, null))));
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
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenReturn(
            new OrphanRecovery(
                2,
                0,
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

  @Test
  void pagesUntilShortPageWithOneCutoff() {
    Instant cutoff = FIXED_NOW.minusSeconds(60);
    when(jobBulkStore.resetOrphanJobsBefore(cutoff, 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE))
        .thenReturn(
            new OrphanRecovery(500, 500, 0, List.of()),
            new OrphanRecovery(500, 500, 0, List.of()),
            new OrphanRecovery(3, 3, 0, List.of()));
    timer.recoverNow();
    verify(jobBulkStore, times(3))
        .resetOrphanJobsBefore(cutoff, 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
  }

  @Test
  void stopsAtPageCap() {
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenReturn(new OrphanRecovery(500, 500, 0, List.of()));
    try (LogCapture logs = LogCapture.start(OrphanRecoveryTimer.class)) {
      timer.recoverNow();
      assertTrue(logs.hasInfoContaining("page cap"));
    }
    verify(jobBulkStore, times(OrphanRecoveryTimer.MAX_ORPHAN_PAGES_PER_SCAN))
        .resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
  }

  @Test
  void stopsAtBudgetAndRunsFinally() {
    MutableClock clock = new MutableClock();
    timer = timerWithClock(clock);
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenAnswer(
            invocation -> {
              clock.advance(Duration.ofSeconds(6));
              return new OrphanRecovery(500, 500, 0, List.of());
            });
    try (LogCapture logs = LogCapture.start(OrphanRecoveryTimer.class)) {
      timer.recoverNow();
      assertTrue(logs.hasInfoContaining("budget"));
    }
    verify(jobBulkStore, times(2))
        .resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
    verify(resourcePermitService).sweepWaiters();
    verify(jobBulkStore).closeStaleExecutions(FIXED_NOW.minusSeconds(60), 500);
  }

  @Test
  void stopsAfterFullPageThatResetsNothing() {
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenReturn(new OrphanRecovery(500, 0, 0, List.of()));
    timer.recoverNow();
    verify(jobBulkStore)
        .resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
  }

  @Test
  void exhaustedRowsAreFailedPageByPage() {
    JobEntity first = exhaustedJob(11);
    JobEntity second = exhaustedJob(12);
    JobEntity third = exhaustedJob(13);
    List<Object> events = new ArrayList<>();
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenAnswer(
            invocation -> {
              events.add("page 1");
              return new OrphanRecovery(500, 498, 0, exhaustedRows.subList(0, 2));
            })
        .thenAnswer(
            invocation -> {
              events.add("page 2");
              return new OrphanRecovery(1, 0, 0, exhaustedRows.subList(2, 3));
            });
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenAnswer(
            invocation -> {
              JobEntity job = invocation.getArgument(0);
              events.add(job.getId());
              return true;
            });
    timer.recoverNow();
    assertEquals(5, events.size());
    assertEquals("page 1", events.get(0));
    assertEquals(Set.of(first.getId(), second.getId()), Set.copyOf(events.subList(1, 3)));
    assertEquals("page 2", events.get(3));
    assertEquals(third.getId(), events.get(4));
  }

  @Test
  void fullExhaustedPageContinuesToNextPage() {
    for (int i = 0; i < 510; i++) exhaustedJob(i);
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenReturn(
            new OrphanRecovery(500, 0, 0, exhaustedRows.subList(0, 500)),
            new OrphanRecovery(10, 0, 0, exhaustedRows.subList(500, 510)));
    timer.recoverNow();
    verify(jobBulkStore, times(2)).resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, 500);
    verify(lifecycleFacade, times(510))
        .completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false));
  }

  @Test
  void rejectedCompletionDoesNotBlockNextRow() {
    JobEntity job = exhaustedJob(7);
    JobEntity next = exhaustedJob(8);
    when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(false);
    timer.recoverNow();
    verify(lifecycleFacade).completeFailure(next, JobStatus.RUNNING, false);
    verify(jobBulkStore).resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, 500);
    verify(jobBulkStore).closeStaleExecutions(FIXED_NOW.minusSeconds(60), 500);
    verifyNoMoreInteractions(jobBulkStore);
  }

  @Test
  void missingJobDoesNotBlockNextRow() {
    JobEntity missing = exhaustedJob(8);
    JobEntity next = exhaustedJob(9);
    when(jobCrudStore.findById(missing.getId())).thenReturn(Optional.empty());
    timer.recoverNow();
    verify(lifecycleFacade, never()).completeFailure(missing, JobStatus.RUNNING, false);
    verify(lifecycleFacade).completeFailure(next, JobStatus.RUNNING, false);
    verify(jobBulkStore).resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, 500);
    verify(jobBulkStore).closeStaleExecutions(FIXED_NOW.minusSeconds(60), 500);
    verifyNoMoreInteractions(jobBulkStore);
  }

  @Test
  void completionExceptionDoesNotBlockNextRow() {
    JobEntity first = exhaustedJob(11);
    JobEntity second = exhaustedJob(12);
    when(lifecycleFacade.completeFailure(first, JobStatus.RUNNING, false))
        .thenThrow(new IllegalStateException("completion unavailable"));
    timer.recoverNow();
    verify(lifecycleFacade).completeFailure(second, JobStatus.RUNNING, false);
    verify(jobBulkStore).resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, 500);
    verify(jobBulkStore).closeStaleExecutions(FIXED_NOW.minusSeconds(60), 500);
    verifyNoMoreInteractions(jobBulkStore);
  }

  @Test
  void failureAfterRecordedCompletionDoesNotEscapeScan() {
    JobEntity job = exhaustedJob(11);
    doThrow(new IllegalStateException("callback failed"))
        .when(callbackInvoker)
        .accept(eq(job), any(Throwable.class));
    timer.recoverNow();
    verify(lifecycleFacade).completeFailure(job, JobStatus.RUNNING, false);
  }

  @Test
  void exhaustedBudgetStopsBeforeSecondRow() {
    MutableClock clock = new MutableClock();
    timer = timerWithClock(clock);
    exhaustedJob(11);
    exhaustedJob(12);
    when(jobBulkStore.resetOrphanJobsBefore(any(Instant.class), eq(3), eq(500)))
        .thenReturn(new OrphanRecovery(500, 498, 0, exhaustedRows));
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenAnswer(
            invocation -> {
              clock.advance(OrphanRecoveryTimer.ORPHAN_SCAN_BUDGET.plusSeconds(1));
              return true;
            });
    timer.recoverNow();
    verify(lifecycleFacade).completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false));
    verify(resourcePermitService).sweepWaiters();
    verify(jobBulkStore).resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, 500);
    verify(jobBulkStore).closeStaleExecutions(FIXED_NOW.minusSeconds(60), 500);
  }

  @Test
  void slowFailingHeadDoesNotStarveLaterExhaustedClaims() {
    MutableClock clock = new MutableClock();
    timer = timerWithClock(clock);
    JobEntity first = exhaustedJob(11);
    JobEntity second = exhaustedJob(12);
    JobEntity third = exhaustedJob(13);
    List<UUID> completed = new ArrayList<>();
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenAnswer(
            invocation -> {
              List<ExhaustedOrphan> remaining =
                  exhaustedRows.stream().filter(row -> !completed.contains(row.jobId())).toList();
              return new OrphanRecovery(remaining.size(), 0, 0, remaining);
            });
    when(lifecycleFacade.completeFailure(any(JobEntity.class), eq(JobStatus.RUNNING), eq(false)))
        .thenAnswer(
            invocation -> {
              JobEntity job = invocation.getArgument(0);
              if (job == first) {
                clock.advance(Duration.ofSeconds(11));
                throw new IllegalStateException("store unavailable");
              }
              completed.add(job.getId());
              return true;
            });
    for (int scan = 0; scan < 30 && completed.size() < 2; scan++) {
      timer.recoverNow();
      clock.advance(Duration.ofSeconds(61));
    }
    assertEquals(Set.of(second.getId(), third.getId()), Set.copyOf(completed));
    verify(lifecycleFacade).completeFailure(second, JobStatus.RUNNING, false);
    verify(lifecycleFacade).completeFailure(third, JobStatus.RUNNING, false);
  }

  @Test
  void firstExhaustedRowIsAttemptedAfterPageBudgetExpires() {
    MutableClock clock = new MutableClock();
    timer = timerWithClock(clock);
    JobEntity first = exhaustedJob(11);
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenAnswer(
            invocation -> {
              clock.advance(OrphanRecoveryTimer.ORPHAN_SCAN_BUDGET.plusSeconds(1));
              return new OrphanRecovery(500, 499, 0, exhaustedRows);
            });
    timer.recoverNow();
    verify(lifecycleFacade).completeFailure(first, JobStatus.RUNNING, false);
    verify(jobBulkStore)
        .resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
  }

  @Test
  void throwingPageIsNotRetriedAndFinallyStillRuns() {
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenThrow(new IllegalStateException("page unavailable"));
    timer.recoverNow();
    verify(jobBulkStore)
        .resetOrphanJobsBefore(FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
    verify(resourcePermitService).sweepWaiters();
    verify(jobBulkStore).closeStaleExecutions(FIXED_NOW.minusSeconds(60), 500);
  }

  @Test
  void overlappingRecoverNowWaitsForFirstScan() throws Exception {
    CountDownLatch firstEntered = new CountDownLatch(1);
    CountDownLatch releaseFirst = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    CountDownLatch secondEntered = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    when(jobBulkStore.resetOrphanJobsBefore(
            any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenAnswer(
            invocation -> {
              if (calls.incrementAndGet() == 1) {
                firstEntered.countDown();
                assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
              } else {
                secondEntered.countDown();
              }
              return new OrphanRecovery(0, 0, 0, List.of());
            });
    ExecutorService scans = Executors.newFixedThreadPool(2);
    try {
      Future<?> first = scans.submit(timer::recoverNow);
      assertTrue(firstEntered.await(2, TimeUnit.SECONDS));
      Future<?> second =
          scans.submit(
              () -> {
                secondStarted.countDown();
                timer.recoverNow();
              });
      assertTrue(secondStarted.await(2, TimeUnit.SECONDS));
      assertFalse(secondEntered.await(250, TimeUnit.MILLISECONDS));
      releaseFirst.countDown();
      first.get(3, TimeUnit.SECONDS);
      second.get(3, TimeUnit.SECONDS);
      assertEquals(2, calls.get());
    } finally {
      releaseFirst.countDown();
      scans.shutdownNow();
      assertTrue(scans.awaitTermination(3, TimeUnit.SECONDS));
    }
  }

  @Test
  void scansDoNotUseTimerLifecycleMonitor() throws Exception {
    ExecutorService scans = Executors.newSingleThreadExecutor();
    try {
      synchronized (timer) {
        scans.submit(timer::recoverNow).get(2, TimeUnit.SECONDS);
      }
      verify(jobBulkStore)
          .resetOrphanJobsBefore(
              FIXED_NOW.minusSeconds(60), 3, OrphanRecoveryTimer.ORPHAN_PAGE_SIZE);
    } finally {
      scans.shutdownNow();
      assertTrue(scans.awaitTermination(3, TimeUnit.SECONDS));
    }
  }

  private final List<ExhaustedOrphan> exhaustedRows = new ArrayList<>();

  private JobEntity exhaustedJob(long seq) {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    exhaustedRows.add(new ExhaustedOrphan(job.getId(), seq, 3, "dead"));
    // Each helper call re-stubs the read; lenient so the replaced stubbing is not reported.
    lenient()
        .when(
            jobBulkStore.resetOrphanJobsBefore(
                any(Instant.class), eq(3), eq(OrphanRecoveryTimer.ORPHAN_PAGE_SIZE)))
        .thenAnswer(invocation -> new OrphanRecovery(exhaustedRows.size(), 0, 0, exhaustedRows));
    lenient().when(jobCrudStore.findById(job.getId())).thenReturn(Optional.of(job));
    lenient().when(lifecycleFacade.completeFailure(job, JobStatus.RUNNING, false)).thenReturn(true);
    return job;
  }

  private OrphanRecoveryTimer timerWithClock(Clock clock) {
    return new OrphanRecoveryTimer(
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
        clock,
        new Random(341));
  }

  private static final class MutableClock extends Clock {
    private Instant now = FIXED_NOW;

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return Clock.fixed(now, zone);
    }

    @Override
    public Instant instant() {
      return now;
    }
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

    boolean hasInfoContaining(String text) {
      return records.stream()
          .anyMatch(
              record ->
                  record.getLevel().intValue() == Level.INFO.intValue()
                      && String.valueOf(record.getMessage()).contains(text));
    }

    @Override
    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
      logger.setUseParentHandlers(originalUseParentHandlers);
    }
  }
}

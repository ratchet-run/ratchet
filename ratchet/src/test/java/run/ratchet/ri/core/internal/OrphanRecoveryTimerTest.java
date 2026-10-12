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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.core.SingletonLease;
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
}

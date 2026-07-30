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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.ChainCompletedEvent;
import run.ratchet.api.event.ChainFailedEvent;
import run.ratchet.api.event.ChainStartedEvent;
import run.ratchet.ri.testsupport.StubAfterCommitRegistrar;
import run.ratchet.spi.AfterCommitRegistrar.Outcome;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;

@ExtendWith(MockitoExtension.class)
class ChainSchedulerTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:34:56Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private InternalEventPublisher eventPublisher;

  private ChainScheduler scheduler;
  private StubAfterCommitRegistrar afterCommitRegistrar;

  private static JobEntity pendingJob() {
    return job(JobStatus.PENDING);
  }

  // ── scheduleNext ──────────────────────────────────────────────────────────

  private static JobEntity job(JobStatus status) {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setStatus(status);
    return job;
  }

  @BeforeEach
  void setUp() {
    afterCommitRegistrar = new StubAfterCommitRegistrar();
    scheduler =
        new ChainScheduler(jobCrudStore, jobTerminalStore, FIXED_CLOCK, afterCommitRegistrar);
    lenient().when(jobTerminalStore.cancelJob(any(UUID.class))).thenReturn(true);
  }

  @Test
  void scheduleNext_noChildren_returnsFalse() {
    JobEntity finished = pendingJob();
    when(jobCrudStore.findDependants(eq(finished.getId()), anyInt(), anyInt()))
        .thenReturn(List.of());

    assertFalse(scheduler.scheduleNext(finished));
    verify(jobCrudStore, never()).save(finished);
  }

  @Test
  void scheduleNext_finalChainStepPublishesChainCompletedEvent() {
    scheduler =
        new ChainScheduler(
            jobCrudStore, jobTerminalStore, FIXED_CLOCK, eventPublisher, afterCommitRegistrar);
    JobEntity root = pendingJob();
    root.setJobType(JobExecutionType.SINGLE);
    JobEntity finished = pendingJob();
    finished.setJobType(JobExecutionType.CHAIN_STEP);
    finished.setDependsOn(root.getId());

    when(jobCrudStore.findDependants(eq(finished.getId()), anyInt(), anyInt()))
        .thenReturn(List.of());
    when(jobCrudStore.findById(root.getId())).thenReturn(java.util.Optional.of(root));

    assertFalse(scheduler.scheduleNext(finished));

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    ChainCompletedEvent event = (ChainCompletedEvent) eventCaptor.getValue();
    assertEquals(finished.getId(), event.getJobId());
    assertEquals(root.getId(), event.getParentJobId());
  }

  @Test
  void scheduleNext_chainCompletedEventPublishesExactlyOnceAfterCommit() {
    afterCommitRegistrar.outcome(Outcome.REGISTERED);
    scheduler =
        new ChainScheduler(
            jobCrudStore, jobTerminalStore, FIXED_CLOCK, eventPublisher, afterCommitRegistrar);
    JobEntity root = pendingJob();
    root.setJobType(JobExecutionType.SINGLE);
    JobEntity finished = pendingJob();
    finished.setJobType(JobExecutionType.CHAIN_STEP);
    finished.setDependsOn(root.getId());
    when(jobCrudStore.findDependants(eq(finished.getId()), anyInt(), anyInt()))
        .thenReturn(List.of());
    when(jobCrudStore.findById(root.getId())).thenReturn(Optional.of(root));

    assertFalse(scheduler.scheduleNext(finished));

    verify(eventPublisher, never()).publish(any(ChainCompletedEvent.class));

    afterCommitRegistrar.commit();

    verify(eventPublisher, times(1)).publish(any(ChainCompletedEvent.class));
  }

  @Test
  void scheduleNext_chainCompletedEventIsSuppressedOnRollback() {
    afterCommitRegistrar.outcome(Outcome.REGISTERED);
    scheduler =
        new ChainScheduler(
            jobCrudStore, jobTerminalStore, FIXED_CLOCK, eventPublisher, afterCommitRegistrar);
    JobEntity root = pendingJob();
    root.setJobType(JobExecutionType.SINGLE);
    JobEntity finished = pendingJob();
    finished.setJobType(JobExecutionType.CHAIN_STEP);
    finished.setDependsOn(root.getId());
    when(jobCrudStore.findDependants(eq(finished.getId()), anyInt(), anyInt()))
        .thenReturn(List.of());
    when(jobCrudStore.findById(root.getId())).thenReturn(Optional.of(root));

    assertFalse(scheduler.scheduleNext(finished));
    afterCommitRegistrar.rollBack();

    verify(eventPublisher, never()).publish(any(ChainCompletedEvent.class));
  }

  @Test
  void scheduleNext_pendingChildWithSentinel_setsScheduledTimeAndReturnsTrue() {
    JobEntity finished = pendingJob();
    JobEntity child = pendingJob();
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);

    when(jobCrudStore.findDependants(eq(finished.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));

    assertTrue(scheduler.scheduleNext(finished));

    ArgumentCaptor<JobEntity> saved = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).save(saved.capture());
    assertEquals(FIXED_NOW, saved.getValue().getScheduledTime());
  }

  @Test
  void scheduleNext_firstChainStepPublishesChainStartedEvent() {
    scheduler =
        new ChainScheduler(
            jobCrudStore, jobTerminalStore, FIXED_CLOCK, eventPublisher, afterCommitRegistrar);
    JobEntity root = pendingJob();
    root.setJobType(JobExecutionType.SINGLE);
    JobEntity child = pendingJob();
    child.setJobType(JobExecutionType.CHAIN_STEP);
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);

    when(jobCrudStore.findDependants(eq(root.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));

    assertTrue(scheduler.scheduleNext(root));

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    ChainStartedEvent event = (ChainStartedEvent) eventCaptor.getValue();
    assertEquals(child.getId(), event.getJobId());
    assertEquals(root.getId(), event.getParentJobId());
  }

  @Test
  void scheduleNext_chainStartedEventPublishesExactlyOnceAfterCommit() {
    afterCommitRegistrar.outcome(Outcome.REGISTERED);
    scheduler =
        new ChainScheduler(
            jobCrudStore, jobTerminalStore, FIXED_CLOCK, eventPublisher, afterCommitRegistrar);
    JobEntity root = pendingJob();
    root.setJobType(JobExecutionType.SINGLE);
    JobEntity child = pendingJob();
    child.setJobType(JobExecutionType.CHAIN_STEP);
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    when(jobCrudStore.findDependants(eq(root.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));

    assertTrue(scheduler.scheduleNext(root));

    verify(eventPublisher, never()).publish(any(ChainStartedEvent.class));

    afterCommitRegistrar.commit();

    verify(eventPublisher, times(1)).publish(any(ChainStartedEvent.class));
  }

  @Test
  void scheduleNext_pendingChildWithRealScheduledTime_notUnlocked() {
    // Asymmetry: cancelChain cancels any PENDING child, but scheduleNext only unlocks children
    // whose scheduledTime equals CHAIN_LOCK_TIME. A child manually scheduled with a real time
    // is NOT unlocked by scheduleNext.
    JobEntity finished = pendingJob();
    JobEntity child = pendingJob();
    child.setScheduledTime(Instant.parse("2025-01-01T00:00:00Z"));

    when(jobCrudStore.findDependants(eq(finished.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));

    assertFalse(scheduler.scheduleNext(finished));
    verify(jobCrudStore, never()).save(child);
  }

  @Test
  void scheduleNext_nonPendingChildWithSentinel_notScheduled() {
    JobEntity finished = pendingJob();
    JobEntity child = job(JobStatus.CANCELED);
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);

    when(jobCrudStore.findDependants(eq(finished.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));

    assertFalse(scheduler.scheduleNext(finished));
    verify(jobCrudStore, never()).save(child);
  }

  // ── cancelChain ───────────────────────────────────────────────────────────

  @Test
  void scheduleNext_waitingChildWithSentinel_unlocksScheduleAndKeepsWaiting() {
    JobEntity finished = pendingJob();
    JobEntity child = job(JobStatus.WAITING);
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);

    when(jobCrudStore.findDependants(eq(finished.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));

    assertTrue(scheduler.scheduleNext(finished));

    verify(jobCrudStore).save(child);
    assertEquals(JobStatus.WAITING, child.getStatus());
    assertEquals(FIXED_NOW, child.getScheduledTime());
  }

  @Test
  void scheduleNext_mixedChildren_onlyUnlocksSentinelPendingOnes() {
    JobEntity finished = pendingJob();
    JobEntity unlockable = pendingJob();
    unlockable.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    JobEntity alreadyScheduled = pendingJob();
    alreadyScheduled.setScheduledTime(Instant.now().plusSeconds(60));

    when(jobCrudStore.findDependants(eq(finished.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(unlockable, alreadyScheduled));

    assertTrue(scheduler.scheduleNext(finished));
    verify(jobCrudStore).save(unlockable);
    verify(jobCrudStore, never()).save(alreadyScheduled);
  }

  @Test
  void cancelChain_noChildren_noop() {
    JobEntity failed = pendingJob();
    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobTerminalStore, never()).cancelJob(any(UUID.class));
  }

  @Test
  void cancelChain_pendingChild_isCanceled() {
    JobEntity failed = pendingJob();
    JobEntity child = pendingJob();

    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobTerminalStore).cancelJob(child.getId());
  }

  @Test
  void cancelChain_pendingChildWithRealScheduledTime_isCanceled() {
    // cancelChain does NOT check CHAIN_LOCK_TIME — it cancels any PENDING child regardless.
    // This is the asymmetry with scheduleNext, which does require the sentinel.
    JobEntity failed = pendingJob();
    JobEntity child = pendingJob();
    child.setScheduledTime(Instant.parse("2025-01-01T00:00:00Z"));

    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobTerminalStore).cancelJob(child.getId());
  }

  @Test
  void cancelChain_nonPendingChild_notCanceled() {
    JobEntity failed = pendingJob();
    JobEntity child = job(JobStatus.RUNNING);

    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobTerminalStore, never()).cancelJob(child.getId());
    assertEquals(JobStatus.RUNNING, child.getStatus());
  }

  @Test
  void cancelChain_waitingChild_isCanceled() {
    JobEntity failed = pendingJob();
    JobEntity child = job(JobStatus.WAITING);

    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobTerminalStore).cancelJob(child.getId());
  }

  @Test
  void cancelChain_publishesChainFailedEventWhenDependantsAreCanceled() {
    scheduler =
        new ChainScheduler(
            jobCrudStore, jobTerminalStore, FIXED_CLOCK, eventPublisher, afterCommitRegistrar);
    JobEntity failed = pendingJob();
    failed.setJobType(JobExecutionType.SINGLE);
    failed.setLastError("boom");
    JobEntity child = pendingJob();
    child.setJobType(JobExecutionType.CHAIN_STEP);

    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    ChainFailedEvent event = (ChainFailedEvent) eventCaptor.getValue();
    assertEquals(failed.getId(), event.getJobId());
    assertEquals(failed.getId(), event.getParentJobId());
    assertEquals("boom", event.getErrorMessage());
  }

  @Test
  void cancelChain_chainFailedEventIsSuppressedOnRollback() {
    afterCommitRegistrar.outcome(Outcome.REGISTERED);
    scheduler =
        new ChainScheduler(
            jobCrudStore, jobTerminalStore, FIXED_CLOCK, eventPublisher, afterCommitRegistrar);
    JobEntity failed = pendingJob();
    failed.setJobType(JobExecutionType.SINGLE);
    failed.setLastError("boom");
    JobEntity child = pendingJob();
    child.setJobType(JobExecutionType.CHAIN_STEP);
    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);
    afterCommitRegistrar.rollBack();

    verify(eventPublisher, never()).publish(any(ChainFailedEvent.class));
  }

  @Test
  void cancelChain_chainFailedEventIsSuppressedWhenAfterCommitRegistrationFails() {
    afterCommitRegistrar.outcome(Outcome.ACTIVE_TRANSACTION_REGISTRATION_FAILED);
    scheduler =
        new ChainScheduler(
            jobCrudStore, jobTerminalStore, FIXED_CLOCK, eventPublisher, afterCommitRegistrar);
    JobEntity failed = pendingJob();
    failed.setJobType(JobExecutionType.SINGLE);
    failed.setLastError("boom");
    JobEntity child = pendingJob();
    child.setJobType(JobExecutionType.CHAIN_STEP);
    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(eventPublisher, never()).publish(any(ChainFailedEvent.class));
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  @Test
  void cancelChain_multiLevel_propagatesDepthFirst() {
    // A (failed) → B (PENDING) → C (PENDING)
    JobEntity failed = pendingJob();
    JobEntity b = pendingJob();
    JobEntity c = pendingJob();

    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(b));
    when(jobCrudStore.findDependants(eq(b.getId()), anyInt(), anyInt())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(eq(c.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    InOrder order = inOrder(jobTerminalStore);
    order.verify(jobTerminalStore).cancelJob(b.getId());
    order.verify(jobTerminalStore).cancelJob(c.getId());
  }

  @Test
  void cancelChain_mixedStatuses_onlyCancelsPending() {
    // A (failed) → B (PENDING), C (RUNNING), D (SUCCEEDED)
    JobEntity failed = pendingJob();
    JobEntity b = pendingJob();
    JobEntity c = job(JobStatus.RUNNING);
    JobEntity d = job(JobStatus.SUCCEEDED);

    when(jobCrudStore.findDependants(eq(failed.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(b, c, d));
    when(jobCrudStore.findDependants(eq(b.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobCrudStore.findDependants(eq(c.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobCrudStore.findDependants(eq(d.getId()), anyInt(), anyInt())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobTerminalStore).cancelJob(b.getId());
    verify(jobTerminalStore, never()).cancelJob(c.getId());
    verify(jobTerminalStore, never()).cancelJob(d.getId());
  }
}

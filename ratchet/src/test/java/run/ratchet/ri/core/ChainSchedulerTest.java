package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;

@ExtendWith(MockitoExtension.class)
class ChainSchedulerTest {

  @Mock private JobCrudStore jobCrudStore;

  private ChainScheduler scheduler;

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
    scheduler = new ChainScheduler(jobCrudStore);
  }

  @Test
  void scheduleNext_noChildren_returnsFalse() {
    JobEntity finished = pendingJob();
    when(jobCrudStore.findDependants(finished.getId())).thenReturn(List.of());

    assertFalse(scheduler.scheduleNext(finished));
    verify(jobCrudStore, never()).save(finished);
  }

  @Test
  void scheduleNext_pendingChildWithSentinel_setsScheduledTimeAndReturnsTrue() {
    JobEntity finished = pendingJob();
    JobEntity child = pendingJob();
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);

    when(jobCrudStore.findDependants(finished.getId())).thenReturn(List.of(child));

    assertTrue(scheduler.scheduleNext(finished));

    ArgumentCaptor<JobEntity> saved = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).save(saved.capture());
    assertNotEquals(
        ChainScheduler.CHAIN_LOCK_TIME,
        saved.getValue().getScheduledTime(),
        "scheduleNext must replace CHAIN_LOCK_TIME with a real instant");
  }

  @Test
  void scheduleNext_pendingChildWithRealScheduledTime_notUnlocked() {
    // Asymmetry: cancelChain cancels any PENDING child, but scheduleNext only unlocks children
    // whose scheduledTime equals CHAIN_LOCK_TIME. A child manually scheduled with a real time
    // is NOT unlocked by scheduleNext.
    JobEntity finished = pendingJob();
    JobEntity child = pendingJob();
    child.setScheduledTime(Instant.parse("2025-01-01T00:00:00Z"));

    when(jobCrudStore.findDependants(finished.getId())).thenReturn(List.of(child));

    assertFalse(scheduler.scheduleNext(finished));
    verify(jobCrudStore, never()).save(child);
  }

  @Test
  void scheduleNext_nonPendingChildWithSentinel_notScheduled() {
    JobEntity finished = pendingJob();
    JobEntity child = job(JobStatus.CANCELED);
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);

    when(jobCrudStore.findDependants(finished.getId())).thenReturn(List.of(child));

    assertFalse(scheduler.scheduleNext(finished));
    verify(jobCrudStore, never()).save(child);
  }

  // ── cancelChain ───────────────────────────────────────────────────────────

  @Test
  void scheduleNext_waitingChildWithSentinel_unlocksScheduleAndKeepsWaiting() {
    JobEntity finished = pendingJob();
    JobEntity child = job(JobStatus.WAITING);
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);

    when(jobCrudStore.findDependants(finished.getId())).thenReturn(List.of(child));

    assertTrue(scheduler.scheduleNext(finished));

    verify(jobCrudStore).save(child);
    assertEquals(JobStatus.WAITING, child.getStatus());
    assertNotEquals(ChainScheduler.CHAIN_LOCK_TIME, child.getScheduledTime());
  }

  @Test
  void scheduleNext_mixedChildren_onlyUnlocksSentinelPendingOnes() {
    JobEntity finished = pendingJob();
    JobEntity unlockable = pendingJob();
    unlockable.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    JobEntity alreadyScheduled = pendingJob();
    alreadyScheduled.setScheduledTime(Instant.now().plusSeconds(60));

    when(jobCrudStore.findDependants(finished.getId()))
        .thenReturn(List.of(unlockable, alreadyScheduled));

    assertTrue(scheduler.scheduleNext(finished));
    verify(jobCrudStore).save(unlockable);
    verify(jobCrudStore, never()).save(alreadyScheduled);
  }

  @Test
  void cancelChain_noChildren_noop() {
    JobEntity failed = pendingJob();
    when(jobCrudStore.findDependants(failed.getId())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobCrudStore, never()).save(failed);
  }

  @Test
  void cancelChain_pendingChild_isCanceled() {
    JobEntity failed = pendingJob();
    JobEntity child = pendingJob();

    when(jobCrudStore.findDependants(failed.getId())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobCrudStore).save(child);
    assertEquals(JobStatus.CANCELED, child.getStatus());
  }

  @Test
  void cancelChain_pendingChildWithRealScheduledTime_isCanceled() {
    // cancelChain does NOT check CHAIN_LOCK_TIME — it cancels any PENDING child regardless.
    // This is the asymmetry with scheduleNext, which does require the sentinel.
    JobEntity failed = pendingJob();
    JobEntity child = pendingJob();
    child.setScheduledTime(Instant.parse("2025-01-01T00:00:00Z"));

    when(jobCrudStore.findDependants(failed.getId())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobCrudStore).save(child);
    assertEquals(JobStatus.CANCELED, child.getStatus());
  }

  @Test
  void cancelChain_nonPendingChild_notCanceled() {
    JobEntity failed = pendingJob();
    JobEntity child = job(JobStatus.RUNNING);

    when(jobCrudStore.findDependants(failed.getId())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobCrudStore, never()).save(child);
    assertEquals(JobStatus.RUNNING, child.getStatus());
  }

  @Test
  void cancelChain_waitingChild_isCanceled() {
    JobEntity failed = pendingJob();
    JobEntity child = job(JobStatus.WAITING);

    when(jobCrudStore.findDependants(failed.getId())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    verify(jobCrudStore).save(child);
    assertEquals(JobStatus.CANCELED, child.getStatus());
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  @Test
  void cancelChain_multiLevel_propagatesDepthFirst() {
    // A (failed) → B (PENDING) → C (PENDING)
    JobEntity failed = pendingJob();
    JobEntity b = pendingJob();
    JobEntity c = pendingJob();

    when(jobCrudStore.findDependants(failed.getId())).thenReturn(List.of(b));
    when(jobCrudStore.findDependants(b.getId())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(c.getId())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    assertEquals(JobStatus.CANCELED, b.getStatus());
    assertEquals(JobStatus.CANCELED, c.getStatus());
  }

  @Test
  void cancelChain_mixedStatuses_onlyCancelsPending() {
    // A (failed) → B (PENDING), C (RUNNING), D (SUCCEEDED)
    JobEntity failed = pendingJob();
    JobEntity b = pendingJob();
    JobEntity c = job(JobStatus.RUNNING);
    JobEntity d = job(JobStatus.SUCCEEDED);

    when(jobCrudStore.findDependants(failed.getId())).thenReturn(List.of(b, c, d));
    when(jobCrudStore.findDependants(b.getId())).thenReturn(List.of());
    when(jobCrudStore.findDependants(c.getId())).thenReturn(List.of());
    when(jobCrudStore.findDependants(d.getId())).thenReturn(List.of());

    scheduler.cancelChain(failed);

    assertEquals(JobStatus.CANCELED, b.getStatus());
    assertEquals(JobStatus.RUNNING, c.getStatus());
    assertEquals(JobStatus.SUCCEEDED, d.getStatus());
  }
}

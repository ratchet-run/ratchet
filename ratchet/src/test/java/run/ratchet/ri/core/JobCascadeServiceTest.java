package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;

@ExtendWith(MockitoExtension.class)
class JobCascadeServiceTest {

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobPauseStore jobPauseStore;

  private JobCascadeService cascadeService;

  @BeforeEach
  void setUp() {
    cascadeService = new JobCascadeService(jobCrudStore, jobPauseStore);
  }

  // ── pauseChildrenIterative ─────────────────────────────────────────────────

  @Test
  void pause_noChildren_returnsZeros() {
    UUID rootId = UUID.randomUUID();
    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of());

    assertArrayEquals(new int[] {0, 0}, cascadeService.pauseChildrenIterative(rootId));
  }

  @Test
  void pause_pendingChild_pausedAndCounted() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = pendingJob();

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());
    when(jobPauseStore.transitionToPaused(child.getId(), JobStatus.PENDING)).thenReturn(true);

    assertArrayEquals(new int[] {1, 0}, cascadeService.pauseChildrenIterative(rootId));
  }

  @Test
  void pause_pauseStoreFails_childCountedAsSkipped() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = pendingJob();

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());
    when(jobPauseStore.transitionToPaused(child.getId(), JobStatus.PENDING)).thenReturn(false);

    assertArrayEquals(new int[] {0, 1}, cascadeService.pauseChildrenIterative(rootId));
  }

  @Test
  void pause_nonPendingChild_skipped() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = job(JobStatus.RUNNING);

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());

    assertArrayEquals(new int[] {0, 1}, cascadeService.pauseChildrenIterative(rootId));
    verify(jobPauseStore, never()).transitionToPaused(any(), any());
  }

  @Test
  void pause_multiLevel_cascadesDownTree() {
    // root → A → B
    UUID rootId = UUID.randomUUID();
    JobEntity a = pendingJob();
    JobEntity b = pendingJob();

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(a));
    when(jobCrudStore.findDependants(a.getId())).thenReturn(List.of(b));
    when(jobCrudStore.findDependants(b.getId())).thenReturn(List.of());
    when(jobPauseStore.transitionToPaused(a.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionToPaused(b.getId(), JobStatus.PENDING)).thenReturn(true);

    assertArrayEquals(new int[] {2, 0}, cascadeService.pauseChildrenIterative(rootId));
  }

  @Test
  void pause_diamondGraph_childProcessedOnlyOnce() {
    // root → A, root → B; A → C, B → C (diamond — C has two parents)
    UUID rootId = UUID.randomUUID();
    JobEntity a = pendingJob();
    JobEntity b = pendingJob();
    JobEntity c = pendingJob();

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(a, b));
    when(jobCrudStore.findDependants(a.getId())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(b.getId())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(c.getId())).thenReturn(List.of());
    when(jobPauseStore.transitionToPaused(a.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionToPaused(b.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionToPaused(c.getId(), JobStatus.PENDING)).thenReturn(true);

    int[] result = cascadeService.pauseChildrenIterative(rootId);

    // C visited only once; total paused = A + B + C = 3
    assertArrayEquals(new int[] {3, 0}, result);
    verify(jobPauseStore).transitionToPaused(eq(c.getId()), eq(JobStatus.PENDING));
  }

  // ── resumeChildrenIterative ────────────────────────────────────────────────

  @Test
  void resume_noChildren_returnsZeros() {
    UUID rootId = UUID.randomUUID();
    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of());

    assertArrayEquals(new int[] {0, 0}, cascadeService.resumeChildrenIterative(rootId, false));
  }

  @Test
  void resume_pausedChild_resumedAndCounted() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = job(JobStatus.PAUSED);

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());
    when(jobPauseStore.transitionFromPaused(child.getId(), JobStatus.PENDING)).thenReturn(true);

    assertArrayEquals(new int[] {1, 0}, cascadeService.resumeChildrenIterative(rootId, false));
  }

  @Test
  void resume_nonPausedChild_skipped() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = pendingJob();

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());

    assertArrayEquals(new int[] {0, 1}, cascadeService.resumeChildrenIterative(rootId, false));
    verify(jobPauseStore, never()).transitionFromPaused(any(), any());
  }

  @Test
  void resume_executeImmediately_scheduledTimeUnchanged() {
    // executeImmediately is documented as a no-op via the post-split hot transition SPI.
    // Resumed children keep their original scheduledTime regardless of the flag.
    UUID rootId = UUID.randomUUID();
    JobEntity child = job(JobStatus.PAUSED);
    Instant originalTime = Instant.parse("2025-06-01T10:00:00Z");
    child.setScheduledTime(originalTime);

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());
    when(jobPauseStore.transitionFromPaused(child.getId(), JobStatus.PENDING)).thenReturn(true);

    cascadeService.resumeChildrenIterative(rootId, true);

    // scheduledTime must be unchanged — the flag is ignored
    assertNotNull(child.getScheduledTime());
  }

  @Test
  void resume_multiLevel_cascadesDownTree() {
    // root → A (paused) → B (paused)
    UUID rootId = UUID.randomUUID();
    JobEntity a = job(JobStatus.PAUSED);
    JobEntity b = job(JobStatus.PAUSED);

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(a));
    when(jobCrudStore.findDependants(a.getId())).thenReturn(List.of(b));
    when(jobCrudStore.findDependants(b.getId())).thenReturn(List.of());
    when(jobPauseStore.transitionFromPaused(a.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionFromPaused(b.getId(), JobStatus.PENDING)).thenReturn(true);

    assertArrayEquals(new int[] {2, 0}, cascadeService.resumeChildrenIterative(rootId, false));
  }

  @Test
  void resume_diamondGraph_childProcessedOnlyOnce() {
    // root → A, root → B; A → C, B → C
    UUID rootId = UUID.randomUUID();
    JobEntity a = job(JobStatus.PAUSED);
    JobEntity b = job(JobStatus.PAUSED);
    JobEntity c = job(JobStatus.PAUSED);

    when(jobCrudStore.findDependants(rootId)).thenReturn(List.of(a, b));
    when(jobCrudStore.findDependants(a.getId())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(b.getId())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(c.getId())).thenReturn(List.of());
    when(jobPauseStore.transitionFromPaused(a.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionFromPaused(b.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionFromPaused(c.getId(), JobStatus.PENDING)).thenReturn(true);

    int[] result = cascadeService.resumeChildrenIterative(rootId, false);

    assertArrayEquals(new int[] {3, 0}, result);
    verify(jobPauseStore).transitionFromPaused(eq(c.getId()), eq(JobStatus.PENDING));
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private static JobEntity pendingJob() {
    return job(JobStatus.PENDING);
  }

  private static JobEntity job(JobStatus status) {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setStatus(status);
    return job;
  }
}

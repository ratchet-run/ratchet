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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.event.AbstractJobSchedulerEvent;
import run.ratchet.api.event.JobPausedEvent;
import run.ratchet.api.event.JobResumedEvent;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.testsupport.StubAfterCommitRegistrar;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;

@ExtendWith(MockitoExtension.class)
class JobCascadeServiceTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-18T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobPauseStore jobPauseStore;
  @Mock private InternalEventPublisher eventPublisher;

  private JobCascadeService cascadeService;

  private static JobEntity pendingJob() {
    return job(JobStatus.PENDING);
  }

  // ── pauseChildrenIterative ─────────────────────────────────────────────────

  private static JobEntity job(JobStatus status) {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setStatus(status);
    job.setJobType(JobExecutionType.SINGLE);
    job.setBusinessKey("cascade-key");
    job.setPriority(JobPriority.HIGH);
    job.setPickedBy("node-a");
    return job;
  }

  @BeforeEach
  void setUp() {
    cascadeService =
        new JobCascadeService(
            jobCrudStore,
            jobPauseStore,
            eventPublisher,
            FIXED_CLOCK,
            new StubAfterCommitRegistrar());
  }

  @Test
  void pause_noChildren_returnsZeros() {
    UUID rootId = UUID.randomUUID();
    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of());

    assertArrayEquals(new int[] {0, 0}, cascadeService.pauseChildrenIterative(rootId));
  }

  @Test
  void pause_pendingChild_pausedAndCounted() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = pendingJob();

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobPauseStore.transitionToPaused(child.getId(), JobStatus.PENDING)).thenReturn(true);

    assertArrayEquals(new int[] {1, 0}, cascadeService.pauseChildrenIterative(rootId));
    assertCommonEvent(published(JobPausedEvent.class), child.getId());
  }

  @Test
  void pause_pauseStoreFails_childCountedAsSkipped() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = pendingJob();

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobPauseStore.transitionToPaused(child.getId(), JobStatus.PENDING)).thenReturn(false);

    assertArrayEquals(new int[] {0, 1}, cascadeService.pauseChildrenIterative(rootId));
  }

  @Test
  void pause_pendingChildWhenTransitionRejected_childCountedAsSkipped() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = pendingJob();

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobPauseStore.transitionToPaused(child.getId(), JobStatus.PENDING)).thenReturn(false);

    assertArrayEquals(new int[] {0, 1}, cascadeService.pauseChildrenIterative(rootId));
    verify(jobPauseStore).transitionToPaused(eq(child.getId()), eq(JobStatus.PENDING));
  }

  @Test
  void pause_nonPendingChild_skipped() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = job(JobStatus.RUNNING);

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(child));
    assertArrayEquals(new int[] {0, 1}, cascadeService.pauseChildrenIterative(rootId));
    verify(jobPauseStore, never()).transitionToPaused(any(), any());
  }

  @Test
  void pause_failedChild_skipped() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = job(JobStatus.FAILED);

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(child));

    assertArrayEquals(new int[] {0, 1}, cascadeService.pauseChildrenIterative(rootId));
    verify(jobPauseStore, never()).transitionToPaused(any(), any());
  }

  @Test
  void pause_terminalChild_doesNotTraverseChildSubtree() {
    UUID rootId = UUID.randomUUID();
    JobEntity failedChild = job(JobStatus.FAILED);

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt()))
        .thenReturn(List.of(failedChild));

    assertArrayEquals(new int[] {0, 1}, cascadeService.pauseChildrenIterative(rootId));
    verify(jobCrudStore, never()).findDependants(eq(failedChild.getId()), anyInt(), anyInt());
  }

  // ── resumeChildrenIterative ────────────────────────────────────────────────

  @Test
  void pause_multiLevel_cascadesDownTree() {
    // root → A → B
    UUID rootId = UUID.randomUUID();
    JobEntity a = pendingJob();
    JobEntity b = pendingJob();

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(a));
    when(jobCrudStore.findDependants(eq(a.getId()), anyInt(), anyInt())).thenReturn(List.of(b));
    when(jobCrudStore.findDependants(eq(b.getId()), anyInt(), anyInt())).thenReturn(List.of());
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

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(a, b));
    when(jobCrudStore.findDependants(eq(a.getId()), anyInt(), anyInt())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(eq(b.getId()), anyInt(), anyInt())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(eq(c.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobPauseStore.transitionToPaused(a.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionToPaused(b.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionToPaused(c.getId(), JobStatus.PENDING)).thenReturn(true);

    int[] result = cascadeService.pauseChildrenIterative(rootId);

    // C visited only once; total paused = A + B + C = 3
    assertArrayEquals(new int[] {3, 0}, result);
    verify(jobPauseStore).transitionToPaused(eq(c.getId()), eq(JobStatus.PENDING));
  }

  @Test
  void resume_noChildren_returnsZeros() {
    UUID rootId = UUID.randomUUID();
    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of());

    assertArrayEquals(new int[] {0, 0}, cascadeService.resumeChildrenIterative(rootId));
  }

  @Test
  void resume_pausedChild_resumedAndCounted() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = job(JobStatus.PAUSED);

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobPauseStore.transitionFromPaused(child.getId(), JobStatus.PENDING)).thenReturn(true);

    assertArrayEquals(new int[] {1, 0}, cascadeService.resumeChildrenIterative(rootId));
    assertCommonEvent(published(JobResumedEvent.class), child.getId());
  }

  @Test
  void resume_pausedChildWhenTransitionRejected_childCountedAsSkipped() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = job(JobStatus.PAUSED);

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobPauseStore.transitionFromPaused(child.getId(), JobStatus.PENDING)).thenReturn(false);

    assertArrayEquals(new int[] {0, 1}, cascadeService.resumeChildrenIterative(rootId));
    verify(jobPauseStore).transitionFromPaused(eq(child.getId()), eq(JobStatus.PENDING));
  }

  @Test
  void resume_nonPausedChild_skipped() {
    UUID rootId = UUID.randomUUID();
    JobEntity child = pendingJob();

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());

    assertArrayEquals(new int[] {0, 1}, cascadeService.resumeChildrenIterative(rootId));
    verify(jobPauseStore, never()).transitionFromPaused(any(), any());
  }

  @Test
  void resume_terminalChild_doesNotTraverseChildSubtree() {
    UUID rootId = UUID.randomUUID();
    JobEntity canceledChild = job(JobStatus.CANCELED);

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt()))
        .thenReturn(List.of(canceledChild));

    assertArrayEquals(new int[] {0, 1}, cascadeService.resumeChildrenIterative(rootId));
    verify(jobCrudStore, never()).findDependants(eq(canceledChild.getId()), anyInt(), anyInt());
  }

  @Test
  void resume_multiLevel_cascadesDownTree() {
    // root → A (paused) → B (paused)
    UUID rootId = UUID.randomUUID();
    JobEntity a = job(JobStatus.PAUSED);
    JobEntity b = job(JobStatus.PAUSED);

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(a));
    when(jobCrudStore.findDependants(eq(a.getId()), anyInt(), anyInt())).thenReturn(List.of(b));
    when(jobCrudStore.findDependants(eq(b.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobPauseStore.transitionFromPaused(a.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionFromPaused(b.getId(), JobStatus.PENDING)).thenReturn(true);

    assertArrayEquals(new int[] {2, 0}, cascadeService.resumeChildrenIterative(rootId));
  }

  @Test
  void resume_diamondGraph_childProcessedOnlyOnce() {
    // root → A, root → B; A → C, B → C
    UUID rootId = UUID.randomUUID();
    JobEntity a = job(JobStatus.PAUSED);
    JobEntity b = job(JobStatus.PAUSED);
    JobEntity c = job(JobStatus.PAUSED);

    when(jobCrudStore.findDependants(eq(rootId), anyInt(), anyInt())).thenReturn(List.of(a, b));
    when(jobCrudStore.findDependants(eq(a.getId()), anyInt(), anyInt())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(eq(b.getId()), anyInt(), anyInt())).thenReturn(List.of(c));
    when(jobCrudStore.findDependants(eq(c.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(jobPauseStore.transitionFromPaused(a.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionFromPaused(b.getId(), JobStatus.PENDING)).thenReturn(true);
    when(jobPauseStore.transitionFromPaused(c.getId(), JobStatus.PENDING)).thenReturn(true);

    int[] result = cascadeService.resumeChildrenIterative(rootId);

    assertArrayEquals(new int[] {3, 0}, result);
    verify(jobPauseStore).transitionFromPaused(eq(c.getId()), eq(JobStatus.PENDING));
  }

  private <T> T published(Class<T> type) {
    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    verify(eventPublisher).publish(eventCaptor.capture());
    return assertInstanceOf(type, eventCaptor.getValue());
  }

  private static void assertCommonEvent(AbstractJobSchedulerEvent event, UUID jobId) {
    assertEquals(jobId, event.getJobId());
    assertEquals("cascade-key", event.getBusinessKey());
    assertEquals(JobType.SINGLE, event.getJobType());
    assertEquals(JobPriority.HIGH, event.getPriority());
    assertEquals("node-a", event.getNodeId());
    assertEquals(FIXED_NOW, event.getTimestamp());
  }
}

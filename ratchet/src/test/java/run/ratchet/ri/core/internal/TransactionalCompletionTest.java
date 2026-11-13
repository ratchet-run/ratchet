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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.JobTransaction;
import run.ratchet.ri.core.TransactionalCompletion;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.TransactionalCompletionStore;
import run.ratchet.store.spi.WorkflowConditionStore;

class TransactionalCompletionTest {
  private final JobStore jobs = mock(JobStore.class);
  private final TransactionalCompletionStore reads = mock(TransactionalCompletionStore.class);
  private final BatchService batches = mock(BatchService.class);
  private final AfterCommitRegistrar registrar = mock(AfterCommitRegistrar.class);
  private final BatchCompletionTransaction boundary = mock(BatchCompletionTransaction.class);
  private final WorkflowScheduler scheduler =
      new WorkflowScheduler(
          jobs, jobs, (WorkflowConditionStore) null, null, Clock.systemUTC(), null, registrar);
  private final DeadLetterService deadLetters = mock(DeadLetterService.class);
  private final PostExecutionHandler handler =
      new PostExecutionHandler(batches, scheduler, deadLetters, null, jobs, registrar, boundary);
  private final JobTransaction carrier = new JobTransaction();

  private JobEntity source(JobExecutionType type) {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setJobType(type);
    job.setClaimSeq(12L);
    return job;
  }

  private TransactionalCompletion complete(JobEntity job) {
    Instant now = Instant.now();
    return handler.completeSuccessInCurrentTransaction(job, "result", "type", now, now, 1, 0);
  }

  @Test
  void rejectsMissingCarrierBeforeStoreAccess() {
    assertThrows(IllegalStateException.class, () -> complete(source(JobExecutionType.SINGLE)));
    verifyNoInteractions(jobs, reads, batches, boundary, registrar);
  }

  @Test
  void rejectsMissingCapability() {
    when(jobs.capability(TransactionalCompletionStore.class)).thenReturn(Optional.empty());
    assertThrows(
        IllegalStateException.class,
        () -> carrier.run(() -> complete(source(JobExecutionType.SINGLE))));
    verify(jobs, never()).commitCompletion(any());
  }

  @Test
  void requiresJoinBeforePlanningWithTransactionalReads() throws Exception {
    JobEntity job = source(JobExecutionType.SINGLE);
    when(jobs.capability(TransactionalCompletionStore.class)).thenReturn(Optional.of(reads));
    when(reads.findConditionsForCompletion(job.getId())).thenReturn(List.of());
    when(reads.findDependantsForCompletion(eq(job.getId()), anyInt(), anyInt()))
        .thenReturn(List.of());
    when(jobs.commitCompletion(any())).thenReturn(new JobCompletionResult(true, null));
    carrier.run(() -> complete(job));
    var ordered = inOrder(reads, jobs);
    ordered.verify(reads).requireJoinedTransaction();
    ordered.verify(reads).findConditionsForCompletion(job.getId());
    ordered.verify(reads).findDependantsForCompletion(eq(job.getId()), anyInt(), eq(0));
    ordered.verify(jobs).commitCompletion(any());
    verify(jobs, never()).findDependants(any(), anyInt(), anyInt());
    verifyNoInteractions(boundary, registrar);
  }

  @Test
  void followupsAreReturnedAndRunOnlyOnRequest() throws Exception {
    JobEntity job = source(JobExecutionType.BATCH_CHILD);
    BatchProgress progress = new BatchProgress(UUID.randomUUID(), 1, 1, 0, null);
    when(jobs.capability(TransactionalCompletionStore.class)).thenReturn(Optional.of(reads));
    when(jobs.commitCompletion(any())).thenReturn(new JobCompletionResult(true, progress));
    TransactionalCompletion completion = carrier.run(() -> complete(job));
    assertEquals(List.of(progress), completion.followups());
    verifyNoInteractions(batches, boundary, registrar);
    handler.runFollowups(completion);
    verify(batches).afterChildCompletion(progress);
    verifyNoInteractions(registrar);
  }

  @Test
  void rejectedFenceThrowsAndRestoresCollector() {
    JobEntity job = source(JobExecutionType.BATCH_CHILD);
    when(jobs.capability(TransactionalCompletionStore.class)).thenReturn(Optional.of(reads));
    when(jobs.commitCompletion(any())).thenReturn(new JobCompletionResult(false, null));
    assertThrows(CompletionRejectedException.class, () -> carrier.run(() -> complete(job)));
    assertFalse(JobTransaction.active());
    BatchProgress progress = new BatchProgress(UUID.randomUUID(), 1, 1, 0, null);
    when(jobs.commitCompletion(any())).thenReturn(new JobCompletionResult(true, progress));
    when(registrar.registerAfterCommit(any())).thenReturn(AfterCommitRegistrar.Result.REGISTERED);
    assertTrue(handler.completeTimeoutFailure(job, JobStatus.RUNNING, false, null, List.of()));
    verify(registrar).registerAfterCommit(any());
    verifyNoInteractions(batches, boundary);
  }
}

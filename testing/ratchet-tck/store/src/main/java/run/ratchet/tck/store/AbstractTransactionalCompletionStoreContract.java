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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.TerminalOutcome;

/** Contract for resolving transactional completion and reading its current planning inputs. */
public abstract class AbstractTransactionalCompletionStoreContract
    implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanStore() {
    cleanupStore();
  }

  @Test
  void runningClaimIsRolledBackButAnotherClaimIsSuperseded() {
    var capability = transactionalCompletionStore();
    JobEntity job = runningJob();
    assertTrue(job.getClaimSeq() > 0);
    assertEquals(
        TerminalOutcome.ROLLED_BACK, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(job.getId(), job.getClaimSeq() + 1));
  }

  @Test
  void resetAndReclaimedAttemptsAreSuperseded() {
    var capability = transactionalCompletionStore();
    JobEntity first = runningJob();
    store().resetRunningJobs(first.getPickedBy());
    assertEquals(JobStatus.PENDING, store().findById(first.getId()).orElseThrow().getStatus());
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(first.getId(), first.getClaimSeq()));
    assertTrue(store().tryPickUpJob(first.getId(), "completion-node"));
    JobEntity next = store().findById(first.getId()).orElseThrow();
    assertTrue(next.getClaimSeq() > first.getClaimSeq());
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(first.getId(), first.getClaimSeq()));
    assertEquals(
        TerminalOutcome.ROLLED_BACK, capability.terminalOutcome(next.getId(), next.getClaimSeq()));
  }

  @Test
  void fencedSuccessWithResultCopiesClaimToColdRow() {
    assertSuccessCopiesClaim("\"result\"");
  }

  @Test
  void fencedSuccessWithoutResultCopiesClaimToColdRow() {
    assertSuccessCopiesClaim(null);
  }

  @Test
  void minimalSuccessCopiesClaimToColdRow() {
    var capability = transactionalCompletionStore();
    JobEntity job = runningJob();
    Instant now = Instant.now();
    assertTrue(store().markJobSucceededMinimal(job.getId(), now, now, 0L, 0L));
    assertTrue(store().currentClaim(job.getId()).isEmpty());
    assertEquals(
        TerminalOutcome.COMMITTED, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(job.getId(), job.getClaimSeq() + 1));
  }

  private void assertSuccessCopiesClaim(String result) {
    var capability = transactionalCompletionStore();
    JobEntity first = runningJob();
    store().resetRunningJobs(first.getPickedBy());
    assertTrue(store().tryPickUpJob(first.getId(), "completion-node"));
    JobEntity job = store().findById(first.getId()).orElseThrow();
    assertTrue(job.getClaimSeq() > first.getClaimSeq());
    assertTrue(store().commitCompletion(plan(job, JobStatus.SUCCEEDED, result)).committed());
    assertTrue(store().currentClaim(job.getId()).isEmpty(), "success removes the hot row");
    // findById reads claim_seq from the queue row, which success deletes; terminalOutcome reads the
    // cold row, so COMMITTED below proves the copy.
    assertEquals(
        TerminalOutcome.COMMITTED, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(job.getId(), first.getClaimSeq()));
  }

  @Test
  void failedAttemptIsSuperseded() {
    var capability = transactionalCompletionStore();
    JobEntity job = runningJob();
    assertTrue(store().commitCompletion(plan(job, JobStatus.FAILED, null)).committed());
    assertTrue(store().currentClaim(job.getId()).isEmpty());
    assertEquals(
        TerminalOutcome.SUPERSEDED, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
  }

  @Test
  void canceledJobIsCanceledRegardlessOfClaim() {
    var capability = transactionalCompletionStore();
    JobEntity job = runningJob();
    assertTrue(store().cancelJob(job.getId()));
    assertTrue(store().currentClaim(job.getId()).isEmpty());
    assertEquals(
        TerminalOutcome.CANCELED, capability.terminalOutcome(job.getId(), job.getClaimSeq()));
    assertEquals(
        TerminalOutcome.CANCELED, capability.terminalOutcome(job.getId(), job.getClaimSeq() + 1));
  }

  @Test
  void pendingAndMissingJobsAreSuperseded() {
    var capability = transactionalCompletionStore();
    JobEntity pending = persist(newPendingJob());
    assertEquals(
        TerminalOutcome.SUPERSEDED,
        capability.terminalOutcome(pending.getId(), pending.getClaimSeq()));
    assertEquals(TerminalOutcome.SUPERSEDED, capability.terminalOutcome(UUID.randomUUID(), 0));
  }

  @Test
  void planningJobReadsMatchPlainReadsIncludingEmptyResults() {
    var capability = transactionalCompletionStore();
    JobEntity job = persist(newPendingJob("completion-tag"));
    JobEntity other = persist(newPendingJob());
    UUID missing = UUID.randomUUID();
    assertEquals(store().findById(job.getId()), capability.findByIdForCompletion(job.getId()));
    assertEquals(
        store().findById(job.getId()).orElseThrow().getTags(),
        capability.findByIdForCompletion(job.getId()).orElseThrow().getTags());
    assertEquals(store().findById(missing), capability.findByIdForCompletion(missing));
    List<UUID> ids = List.of(job.getId(), other.getId(), missing);
    assertEquals(
        new HashSet<>(store().findByIds(ids)),
        new HashSet<>(capability.findByIdsForCompletion(ids)));
    assertEquals(store().findByIds(List.of()), capability.findByIdsForCompletion(List.of()));
    assertEquals(
        store().findByIds(List.of(missing)), capability.findByIdsForCompletion(List.of(missing)));
  }

  @Test
  void planningDependantsMatchPlainPagesAndEmptyResults() {
    var capability = transactionalCompletionStore();
    JobEntity parent = persist(newPendingJob());
    for (int i = 0; i < 3; i++) {
      JobEntity child = newPendingJob("child-tag");
      child.setDependsOn(parent.getId());
      child.setStatus(JobStatus.WAITING);
      persist(child);
    }
    assertEquals(3, capability.findDependantsForCompletion(parent.getId(), 10, 0).size());
    for (int offset : List.of(0, 1, 2, 3)) {
      assertEquals(
          store().findDependants(parent.getId(), 2, offset),
          capability.findDependantsForCompletion(parent.getId(), 2, offset));
    }
    UUID missing = UUID.randomUUID();
    assertEquals(
        store().findDependants(missing, 2, 0),
        capability.findDependantsForCompletion(missing, 2, 0));
  }

  @Test
  void planningConditionsMatchPlainOrderingAndEmptyResults() {
    var capability = transactionalCompletionStore();
    JobEntity parent = persist(newPendingJob());
    for (int order : List.of(2, 0, 1)) {
      JobEntity child = persist(newPendingJob());
      WorkflowConditionEntity condition = new WorkflowConditionEntity();
      condition.setId(UUID.randomUUID());
      condition.setParentJobId(parent.getId());
      condition.setChildJobId(child.getId());
      condition.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
      condition.setDefinitionOrder(order);
      condition.setCreatedAt(Instant.now());
      workflowConditionStore().saveCondition(condition);
    }
    assertEquals(3, capability.findConditionsForCompletion(parent.getId()).size());
    assertEquals(
        workflowConditionStore().findConditionsByParentJobId(parent.getId()),
        capability.findConditionsForCompletion(parent.getId()));
    UUID missing = UUID.randomUUID();
    assertEquals(
        workflowConditionStore().findConditionsByParentJobId(missing),
        capability.findConditionsForCompletion(missing));
  }

  private JobEntity runningJob() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "completion-node"));
    return store().findById(job.getId()).orElseThrow();
  }

  private static JobCompletionPlan plan(JobEntity job, JobStatus terminal, String result) {
    Instant now = Instant.now();
    return new JobCompletionPlan(
        job.getId(),
        JobStatus.RUNNING,
        terminal,
        result,
        null,
        "completion",
        job.getAttempts(),
        now,
        now,
        0L,
        0L,
        null,
        null,
        List.of(),
        job.getClaimSeq(),
        List.of());
  }
}

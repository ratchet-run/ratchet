package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class WorkflowSchedulerTest {

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private WorkflowConditionStore conditionStore;
  @Mock private WorkflowConditionEvaluator conditionEvaluator;

  private WorkflowScheduler scheduler;

  private static WorkflowConditionEntity condition(UUID parentId, UUID childId, int priority) {
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setId(new UUID(0L, 100L + priority));
    condition.setParentJobId(parentId);
    condition.setChildJobId(childId);
    condition.setConditionType(WorkflowCondition.ConditionType.SUCCESS);
    condition.setConditionPriority(priority);
    return condition;
  }

  private static JobEntity job(UUID id, JobStatus status) {
    JobEntity job = new JobEntity();
    job.setId(id);
    job.setStatus(status);
    return job;
  }

  @BeforeEach
  void setUp() {
    scheduler =
        new WorkflowScheduler(
            jobCrudStore,
            jobBatchStatusStore,
            jobTerminalStore,
            conditionStore,
            conditionEvaluator);
  }

  @Test
  void scheduleNext_matchedWaitingBranchKeepsWaitingStatusAndUnlocksSchedule() {
    JobEntity parent = job(new UUID(0L, 1L), JobStatus.SUCCEEDED);
    JobEntity child = job(new UUID(0L, 2L), JobStatus.WAITING);
    WorkflowConditionEntity condition = condition(parent.getId(), child.getId(), 0);
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of(condition));
    when(conditionEvaluator.evaluate(condition, parent)).thenReturn(true);
    when(jobCrudStore.findById(child.getId())).thenReturn(Optional.of(child));

    scheduler.scheduleNext(parent);

    verify(jobCrudStore).save(child);
    assertEquals(JobStatus.WAITING, child.getStatus());
    assertEquals(JobExecutionType.WORKFLOW_BRANCH, child.getJobType());
    assertNotNull(child.getScheduledTime());
  }

  @Test
  void scheduleNext_cancelsUnmatchedWaitingBranches() {
    JobEntity parent = job(new UUID(0L, 10L), JobStatus.SUCCEEDED);
    JobEntity matched = job(new UUID(0L, 11L), JobStatus.PENDING);
    JobEntity unmatched = job(new UUID(0L, 12L), JobStatus.WAITING);
    WorkflowConditionEntity first = condition(parent.getId(), matched.getId(), 0);
    WorkflowConditionEntity second = condition(parent.getId(), unmatched.getId(), 1);
    when(conditionStore.findConditionsByParentJobId(parent.getId()))
        .thenReturn(List.of(first, second));
    when(conditionEvaluator.evaluate(first, parent)).thenReturn(true);
    when(jobCrudStore.findById(matched.getId())).thenReturn(Optional.of(matched));
    when(jobCrudStore.findById(unmatched.getId())).thenReturn(Optional.of(unmatched));
    when(jobTerminalStore.cancelJob(unmatched.getId())).thenReturn(true);

    scheduler.scheduleNext(parent);

    verify(jobTerminalStore).cancelJob(unmatched.getId());
  }

  @Test
  void scheduleNext_noConditionsFallsBackToLinearChain() {
    JobEntity parent = job(new UUID(0L, 30L), JobStatus.SUCCEEDED);
    JobEntity child = job(new UUID(0L, 31L), JobStatus.PENDING);
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of());
    when(jobCrudStore.findDependants(parent.getId())).thenReturn(List.of(child));

    assertTrue(scheduler.scheduleNext(parent));

    verify(jobCrudStore).save(child);
    assertNotNull(child.getScheduledTime());
  }

  @Test
  void scheduleNext_failedParentWithNoConditionsFallsBackToCancelChain() {
    JobEntity parent = job(new UUID(0L, 32L), JobStatus.FAILED);
    JobEntity child = job(new UUID(0L, 33L), JobStatus.PENDING);
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of());
    when(jobCrudStore.findDependants(parent.getId())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());

    assertFalse(scheduler.scheduleNext(parent));

    verify(jobCrudStore).save(child);
    assertEquals(JobStatus.CANCELED, child.getStatus());
  }

  @Test
  void scheduleNext_multipleConditionsSchedulesFirstMatchingBranchAndCancelsRest() {
    JobEntity parent = job(new UUID(0L, 40L), JobStatus.SUCCEEDED);
    JobEntity firstChild = job(new UUID(0L, 41L), JobStatus.PENDING);
    JobEntity secondChild = job(new UUID(0L, 42L), JobStatus.PENDING);
    JobEntity thirdChild = job(new UUID(0L, 43L), JobStatus.WAITING);
    WorkflowConditionEntity first = condition(parent.getId(), firstChild.getId(), 0);
    WorkflowConditionEntity second = condition(parent.getId(), secondChild.getId(), 1);
    WorkflowConditionEntity third = condition(parent.getId(), thirdChild.getId(), 2);
    when(conditionStore.findConditionsByParentJobId(parent.getId()))
        .thenReturn(List.of(first, second, third));
    when(conditionEvaluator.evaluate(first, parent)).thenReturn(false);
    when(conditionEvaluator.evaluate(second, parent)).thenReturn(true);
    when(jobCrudStore.findById(secondChild.getId())).thenReturn(Optional.of(secondChild));
    when(jobCrudStore.findById(firstChild.getId())).thenReturn(Optional.of(firstChild));
    when(jobCrudStore.findById(thirdChild.getId())).thenReturn(Optional.of(thirdChild));
    when(jobTerminalStore.cancelJob(firstChild.getId())).thenReturn(true);
    when(jobTerminalStore.cancelJob(thirdChild.getId())).thenReturn(true);

    assertTrue(scheduler.scheduleNext(parent));

    verify(jobCrudStore).save(secondChild);
    verify(conditionEvaluator, never()).evaluate(third, parent);
    verify(jobTerminalStore).cancelJob(firstChild.getId());
    verify(jobTerminalStore).cancelJob(thirdChild.getId());
  }

  @Test
  void scheduleNext_matchingConditionWithMissingChildContinuesToNextCondition() {
    JobEntity parent = job(new UUID(0L, 50L), JobStatus.SUCCEEDED);
    JobEntity existingChild = job(new UUID(0L, 52L), JobStatus.PENDING);
    UUID missingChildId = new UUID(0L, 51L);
    WorkflowConditionEntity missing = condition(parent.getId(), missingChildId, 0);
    WorkflowConditionEntity existing = condition(parent.getId(), existingChild.getId(), 1);
    when(conditionStore.findConditionsByParentJobId(parent.getId()))
        .thenReturn(List.of(missing, existing));
    when(conditionEvaluator.evaluate(missing, parent)).thenReturn(true);
    when(conditionEvaluator.evaluate(existing, parent)).thenReturn(true);
    when(jobCrudStore.findById(missingChildId)).thenReturn(Optional.empty());
    when(jobCrudStore.findById(existingChild.getId())).thenReturn(Optional.of(existingChild));

    assertTrue(scheduler.scheduleNext(parent));

    verify(jobCrudStore).save(existingChild);
    verify(jobTerminalStore, never()).cancelJob(missingChildId);
  }

  @Test
  void scheduleNext_conditionEvaluatorExceptionMarksParentFailedAndCancelsBranches() {
    JobEntity parent = job(new UUID(0L, 60L), JobStatus.PENDING);
    parent.setAttempts(2);
    JobEntity child = job(new UUID(0L, 61L), JobStatus.WAITING);
    WorkflowConditionEntity condition = condition(parent.getId(), child.getId(), 0);
    RuntimeException failure = new RuntimeException("predicate exploded");
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of(condition));
    when(conditionEvaluator.evaluate(condition, parent)).thenThrow(failure);
    when(jobBatchStatusStore.tryPickUpJob(
            parent.getId(), DefaultBatchBuilder.BATCH_LIFECYCLE_NODE_ID))
        .thenReturn(true);
    when(jobCrudStore.findDependants(parent.getId())).thenReturn(List.of());
    when(jobCrudStore.findById(child.getId())).thenReturn(Optional.of(child));
    when(jobTerminalStore.cancelJob(child.getId())).thenReturn(true);

    assertFalse(scheduler.scheduleNext(parent));

    assertEquals(JobStatus.FAILED, parent.getStatus());
    assertEquals("Workflow condition evaluation failed: predicate exploded", parent.getLastError());
    verify(jobTerminalStore)
        .markJobFailedTerminal(parent.getId(), parent.getLastError(), parent.getAttempts());
    verify(jobTerminalStore).cancelJob(child.getId());
  }

  @Test
  void cancelChain_noConditionalBranchesOnlyCancelsLinearDependants() {
    JobEntity parent = job(new UUID(0L, 70L), JobStatus.FAILED);
    JobEntity child = job(new UUID(0L, 71L), JobStatus.PENDING);
    when(jobCrudStore.findDependants(parent.getId())).thenReturn(List.of(child));
    when(jobCrudStore.findDependants(child.getId())).thenReturn(List.of());
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of());

    scheduler.cancelChain(parent);

    verify(jobCrudStore).save(child);
    assertEquals(JobStatus.CANCELED, child.getStatus());
  }

  @Test
  void cancelChain_multipleConditionalBranchesCancelsOnlyPendingOrWaitingChildren() {
    JobEntity parent = job(new UUID(0L, 80L), JobStatus.FAILED);
    JobEntity pending = job(new UUID(0L, 81L), JobStatus.PENDING);
    JobEntity waiting = job(new UUID(0L, 82L), JobStatus.WAITING);
    JobEntity running = job(new UUID(0L, 83L), JobStatus.RUNNING);
    WorkflowConditionEntity first = condition(parent.getId(), pending.getId(), 0);
    WorkflowConditionEntity second = condition(parent.getId(), waiting.getId(), 1);
    WorkflowConditionEntity third = condition(parent.getId(), running.getId(), 2);
    when(jobCrudStore.findDependants(parent.getId())).thenReturn(List.of());
    when(conditionStore.findConditionsByParentJobId(parent.getId()))
        .thenReturn(List.of(first, second, third));
    when(jobCrudStore.findById(pending.getId())).thenReturn(Optional.of(pending));
    when(jobCrudStore.findById(waiting.getId())).thenReturn(Optional.of(waiting));
    when(jobCrudStore.findById(running.getId())).thenReturn(Optional.of(running));
    when(jobTerminalStore.cancelJob(pending.getId())).thenReturn(true);
    when(jobTerminalStore.cancelJob(waiting.getId())).thenReturn(true);

    scheduler.cancelChain(parent);

    verify(jobTerminalStore).cancelJob(pending.getId());
    verify(jobTerminalStore).cancelJob(waiting.getId());
    verify(jobTerminalStore, never()).cancelJob(running.getId());
  }

  @Test
  void cancelChain_missingConditionalChildDoesNotCancelAnythingForThatCondition() {
    JobEntity parent = job(new UUID(0L, 90L), JobStatus.FAILED);
    UUID missingChildId = new UUID(0L, 91L);
    WorkflowConditionEntity condition = condition(parent.getId(), missingChildId, 0);
    when(jobCrudStore.findDependants(parent.getId())).thenReturn(List.of());
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of(condition));
    when(jobCrudStore.findById(missingChildId)).thenReturn(Optional.empty());

    scheduler.cancelChain(parent);

    verify(jobTerminalStore, never()).cancelJob(missingChildId);
  }

  @Test
  void cancelChain_cancelsWaitingConditionalBranches() {
    JobEntity parent = job(new UUID(0L, 20L), JobStatus.FAILED);
    JobEntity child = job(new UUID(0L, 21L), JobStatus.WAITING);
    WorkflowConditionEntity condition = condition(parent.getId(), child.getId(), 0);
    when(jobCrudStore.findDependants(parent.getId())).thenReturn(List.of());
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of(condition));
    when(jobCrudStore.findById(child.getId())).thenReturn(Optional.of(child));
    when(jobTerminalStore.cancelJob(child.getId())).thenReturn(true);

    scheduler.cancelChain(parent);

    verify(jobTerminalStore).cancelJob(child.getId());
  }
}

package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
}

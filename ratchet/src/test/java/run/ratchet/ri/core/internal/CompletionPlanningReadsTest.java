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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.CompletionReads;
import run.ratchet.ri.core.WorkflowConditionEvaluator;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.TransactionalCompletionStore;
import run.ratchet.store.spi.WorkflowConditionStore;

class CompletionPlanningReadsTest {
  private final JobStore jobs = mock(JobStore.class);
  private final WorkflowConditionStore conditions = mock(WorkflowConditionStore.class);
  private final TransactionalCompletionStore transactional =
      mock(TransactionalCompletionStore.class);
  private final WorkflowConditionEvaluator evaluator = mock(WorkflowConditionEvaluator.class);
  private final WorkflowScheduler scheduler =
      new WorkflowScheduler(
          jobs, jobs, jobs, null, conditions, evaluator, Clock.systemUTC(), null, null);

  private JobEntity job(JobExecutionType type) {
    JobEntity job = new JobEntity();
    job.setId(UUID.randomUUID());
    job.setJobType(type);
    job.setStatus(JobStatus.SUCCEEDED);
    return job;
  }

  @Test
  void explicitViewSuppliesConditionsChildrenDependantsAndAncestors() {
    JobEntity parent = job(JobExecutionType.CHAIN_STEP);
    JobEntity root = job(JobExecutionType.SINGLE);
    parent.setDependsOn(root.getId());
    JobEntity child = job(JobExecutionType.WORKFLOW_BRANCH);
    child.setStatus(JobStatus.PENDING);
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setId(UUID.randomUUID());
    condition.setChildJobId(child.getId());
    when(transactional.findConditionsForCompletion(parent.getId())).thenReturn(List.of(condition));
    when(transactional.findByIdsForCompletion(List.of(child.getId()))).thenReturn(List.of(child));
    when(transactional.findDependantsForCompletion(eq(parent.getId()), anyInt(), anyInt()))
        .thenReturn(List.of());
    when(transactional.findByIdForCompletion(root.getId())).thenReturn(Optional.of(root));
    var plan =
        scheduler.planCompletion(parent, false, null, CompletionReads.transactional(transactional));
    assertEquals(1, plan.dependencies().size());
    assertEquals(JobStatus.CANCELED, plan.dependencies().get(0).status());
    verify(transactional).findByIdsForCompletion(List.of(child.getId()));
    verify(transactional).findByIdForCompletion(root.getId());
    verifyNoInteractions(jobs, conditions);
  }

  @Test
  void oldOverloadsKeepPlainReadBehavior() {
    JobEntity parent = job(JobExecutionType.SINGLE);
    when(conditions.findConditionsByParentJobId(parent.getId())).thenReturn(List.of());
    when(jobs.findDependants(eq(parent.getId()), anyInt(), anyInt())).thenReturn(List.of());
    scheduler.planCompletion(parent, false);
    scheduler.planCompletion(parent, false, "combined");
    verify(conditions, times(2)).findConditionsByParentJobId(parent.getId());
    verify(jobs, times(2)).findDependants(eq(parent.getId()), anyInt(), eq(0));
    verifyNoInteractions(transactional);
  }

  @Test
  void canceledDescendantsAndRootUseExplicitView() {
    JobEntity parent = job(JobExecutionType.CHAIN_STEP);
    JobEntity child = job(JobExecutionType.CHAIN_STEP);
    child.setStatus(JobStatus.PENDING);
    parent.setDependsOn(UUID.randomUUID());
    when(transactional.findByIdForCompletion(parent.getDependsOn())).thenReturn(Optional.empty());
    when(transactional.findDependantsForCompletion(eq(parent.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    when(transactional.findDependantsForCompletion(eq(child.getId()), anyInt(), anyInt()))
        .thenReturn(List.of());
    var plan =
        scheduler.planCompletion(parent, true, null, CompletionReads.transactional(transactional));
    assertEquals(JobStatus.CANCELED, plan.dependencies().get(0).status());
    verify(transactional).findByIdForCompletion(parent.getDependsOn());
    verifyNoInteractions(jobs, conditions);
  }
}

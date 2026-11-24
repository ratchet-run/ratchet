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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.ri.core.WorkflowConditionEvaluator;
import run.ratchet.ri.testsupport.EncryptionTestKit;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.ProtectedSurface;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.converter.PayloadSerializerHolder;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

@ExtendWith(MockitoExtension.class)
class WorkflowSchedulerTest {

  private static final Instant FIXED_NOW = Instant.parse("2026-05-12T12:00:00Z");
  private static final Clock FIXED_CLOCK = Clock.fixed(FIXED_NOW, ZoneOffset.UTC);

  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private WorkflowConditionStore conditionStore;
  @Mock private WorkflowConditionEvaluator conditionEvaluator;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private TransactionSynchronizationRegistry txRegistry;

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
    lenient().when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
    lenient()
        .when(
            jobBatchStatusStore.compareAndSwapStatus(
                any(), any(), eq(JobStatus.CANCELED), isNull()))
        .thenReturn(false);
    scheduler =
        new WorkflowScheduler(
            jobCrudStore,
            jobTerminalStore,
            jobBatchStatusStore,
            RatchetOptions.defaults(),
            conditionStore,
            conditionEvaluator,
            FIXED_CLOCK,
            null,
            new JakartaAfterCommitRegistrar(txRegistry));
    lenient()
        .when(jobCrudStore.findByIds(anyList()))
        .thenAnswer(
            invocation -> {
              List<UUID> ids = invocation.getArgument(0);
              List<JobEntity> jobs = new ArrayList<>();
              for (UUID id : ids) {
                jobCrudStore.findById(id).ifPresent(jobs::add);
              }
              return jobs;
            });
  }

  @Test
  void batchCancellationCancelsCallbackDescendantsAndSkipsBatchChildren() {
    JobEntity parent = job(UUID.randomUUID(), JobStatus.CANCELED);
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    JobEntity child = job(UUID.randomUUID(), JobStatus.CANCELED);
    child.setJobType(JobExecutionType.BATCH_CHILD);
    JobEntity callback = job(UUID.randomUUID(), JobStatus.PENDING);
    callback.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    JobEntity step = job(UUID.randomUUID(), JobStatus.WAITING);
    step.setJobType(JobExecutionType.CHAIN_STEP);
    when(conditionStore.findConditionsByParentJobId(parent.getId()))
        .thenReturn(List.of(condition(parent.getId(), callback.getId(), 1)));
    when(jobCrudStore.findById(callback.getId())).thenReturn(Optional.of(callback));
    when(jobCrudStore.findDependants(eq(parent.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child, callback));
    when(jobCrudStore.findDependants(eq(callback.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(step));
    when(jobBatchStatusStore.compareAndSwapStatus(
            callback.getId(), JobStatus.PENDING, JobStatus.CANCELED, null))
        .thenAnswer(
            invocation -> {
              callback.setStatus(JobStatus.CANCELED);
              return true;
            });
    when(jobBatchStatusStore.compareAndSwapStatus(
            step.getId(), JobStatus.WAITING, JobStatus.CANCELED, null))
        .thenReturn(true);

    assertEquals(1, scheduler.cancelBatchCallbacks(parent));
    verify(jobBatchStatusStore)
        .compareAndSwapStatus(callback.getId(), JobStatus.PENDING, JobStatus.CANCELED, null);
    verify(jobBatchStatusStore)
        .compareAndSwapStatus(step.getId(), JobStatus.WAITING, JobStatus.CANCELED, null);
    verify(jobBatchStatusStore, never())
        .compareAndSwapStatus(eq(child.getId()), any(), eq(JobStatus.CANCELED), isNull());
    verify(jobTerminalStore, never()).cancelRunningJob(eq(child.getId()), any());
    verify(jobCrudStore, never()).findDependants(eq(child.getId()), anyInt(), anyInt());
  }

  @Test
  void batchCancellationProcessesPagesAndPreservesCallbackAndLinearDescendants() {
    JobEntity parent = job(UUID.randomUUID(), JobStatus.CANCELED);
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    JobEntity linear = job(UUID.randomUUID(), JobStatus.PENDING);
    JobEntity callback = job(UUID.randomUUID(), JobStatus.PENDING);
    JobEntity linearDescendant = job(UUID.randomUUID(), JobStatus.WAITING);
    JobEntity callbackDescendant = job(UUID.randomUUID(), JobStatus.PENDING);
    List<JobEntity> firstPage = new ArrayList<>();
    for (int i = 0; i < JobCrudStore.DEFAULT_PAGE_LIMIT - 1; i++) {
      JobEntity child = job(UUID.randomUUID(), JobStatus.CANCELED);
      child.setJobType(JobExecutionType.BATCH_CHILD);
      firstPage.add(child);
    }
    firstPage.add(linear);
    when(conditionStore.findConditionsByParentJobId(parent.getId()))
        .thenReturn(List.of(condition(parent.getId(), callback.getId(), 1)));
    when(jobCrudStore.findById(callback.getId())).thenReturn(Optional.of(callback));
    when(jobCrudStore.findDependants(parent.getId(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0))
        .thenReturn(firstPage);
    when(jobCrudStore.findDependants(
            parent.getId(), JobCrudStore.DEFAULT_PAGE_LIMIT, JobCrudStore.DEFAULT_PAGE_LIMIT))
        .thenReturn(List.of(callback));
    when(jobCrudStore.findDependants(eq(linear.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(linearDescendant));
    when(jobCrudStore.findDependants(eq(callback.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(callbackDescendant));
    when(jobCrudStore.findDependants(eq(linearDescendant.getId()), anyInt(), anyInt()))
        .thenReturn(List.of());
    when(jobCrudStore.findDependants(eq(callbackDescendant.getId()), anyInt(), anyInt()))
        .thenReturn(List.of());
    when(jobBatchStatusStore.compareAndSwapStatus(any(), any(), eq(JobStatus.CANCELED), isNull()))
        .thenAnswer(
            invocation -> {
              if (linearDescendant.getId().equals(invocation.getArgument(0))
                  && invocation.getArgument(1) != JobStatus.WAITING) {
                return false;
              }
              if (callback.getId().equals(invocation.getArgument(0))) {
                callback.setStatus(JobStatus.CANCELED);
              }
              return true;
            });

    assertEquals(1, scheduler.cancelBatchCallbacks(parent));

    InOrder order = inOrder(jobCrudStore, jobBatchStatusStore);
    order.verify(jobCrudStore).findDependants(parent.getId(), JobCrudStore.DEFAULT_PAGE_LIMIT, 0);
    order
        .verify(jobBatchStatusStore)
        .compareAndSwapStatus(linear.getId(), JobStatus.PENDING, JobStatus.CANCELED, null);
    order
        .verify(jobCrudStore)
        .findDependants(
            parent.getId(), JobCrudStore.DEFAULT_PAGE_LIMIT, JobCrudStore.DEFAULT_PAGE_LIMIT);
    verify(jobBatchStatusStore)
        .compareAndSwapStatus(callback.getId(), JobStatus.PENDING, JobStatus.CANCELED, null);
    verify(jobBatchStatusStore)
        .compareAndSwapStatus(
            linearDescendant.getId(), JobStatus.WAITING, JobStatus.CANCELED, null);
    verify(jobBatchStatusStore)
        .compareAndSwapStatus(
            callbackDescendant.getId(), JobStatus.PENDING, JobStatus.CANCELED, null);
    for (JobEntity child : firstPage) {
      if (child.getJobType() == JobExecutionType.BATCH_CHILD) {
        verify(jobCrudStore, never()).findDependants(eq(child.getId()), anyInt(), anyInt());
      }
    }
  }

  @Test
  void reducingCompletionBindsAndClearsCallbackSlot() {
    assertReducingCallbackBound(false);
    assertReducingCallbackBound(true);
  }

  private void assertReducingCallbackBound(boolean encrypted) {
    JobEntity parent = job(new UUID(0L, 901L), JobStatus.SUCCEEDED);
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    JobEntity child = job(new UUID(0L, 902L), JobStatus.PENDING);
    child.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    child.setPayload(
        new JobPayload(
            "example.Callback",
            "accept",
            "(Ljava/lang/Long;)V",
            true,
            Collections.singletonList(null),
            List.of(0)));
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    WorkflowConditionEntity condition = condition(parent.getId(), child.getId(), 0);
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of(condition));
    when(conditionEvaluator.evaluate(condition, parent)).thenReturn(true);
    when(jobCrudStore.findById(child.getId())).thenReturn(Optional.of(child));
    var serializer = new JsonbTestPayloadSerializer();
    child.setEncryptedPayload(encrypted);
    EncryptionTestKit.install(encrypted);
    PayloadSerializerHolder.set(serializer);
    try {
      var plan = scheduler.planCompletion(parent, false, 42L);
      JobPayload bound =
          serializer.deserialize(
              PayloadEncryptor.decryptArgs(
                  plan.dependencies().get(0).payload(),
                  EncryptionTarget.rowBound(ProtectedSurface.PAYLOAD_ARGS, child.getId())),
              JobPayload.class);
      if (encrypted) {
        assertThrows(
            PayloadDecryptionException.class,
            () ->
                PayloadEncryptor.decryptArgs(
                    plan.dependencies().get(0).payload(),
                    EncryptionTarget.rowBound(ProtectedSurface.PAYLOAD_ARGS, parent.getId())));
      }
      assertEquals("42", bound.args().get(0).toString());
      assertEquals(null, bound.runtimeArgIndexes());
      assertEquals(null, scheduler.planCompletion(parent, false).dependencies().get(0).payload());
    } finally {
      PayloadSerializerHolder.set(null);
      EncryptionHolder.disable();
    }
  }

  @Test
  void completionPlanPreservesWaitingStatusAndDoesNotWriteBeforeCommit() {
    JobEntity parent = job(new UUID(0L, 801L), JobStatus.SUCCEEDED);
    parent.setJobType(JobExecutionType.SINGLE);
    JobEntity child = job(new UUID(0L, 802L), JobStatus.WAITING);
    child.setJobType(JobExecutionType.WORKFLOW_BRANCH);
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    child.setVersion(7);
    WorkflowConditionEntity condition = condition(parent.getId(), child.getId(), 0);
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of(condition));
    when(conditionEvaluator.evaluate(condition, parent)).thenReturn(true);
    when(jobCrudStore.findById(child.getId())).thenReturn(Optional.of(child));
    WorkflowCompletionPlan plan = scheduler.planCompletion(parent, false);
    assertEquals(1, plan.dependencies().size());
    var mutation = plan.dependencies().get(0);
    assertEquals(JobStatus.WAITING, mutation.status());
    assertEquals(7, mutation.expectedVersion());
    assertEquals(ChainScheduler.CHAIN_LOCK_TIME, mutation.expectedScheduledTime());
    assertEquals(FIXED_NOW, mutation.scheduledTime());
    assertEquals(ChainScheduler.CHAIN_LOCK_TIME, child.getScheduledTime());
    verify(jobCrudStore, never()).save(any());
    verify(jobBatchStatusStore, never())
        .compareAndSwapStatus(any(), any(), eq(JobStatus.CANCELED), isNull());
    verify(jobTerminalStore, never()).cancelRunningJob(any(), any());
    assertTrue(plan.newWorkAvailable());
  }

  @Test
  void failedCompletionPlansCancellationOfAllLinearDescendants() {
    JobEntity parent = job(new UUID(0L, 811L), JobStatus.FAILED);
    parent.setJobType(JobExecutionType.CHAIN_STEP);
    JobEntity child = job(new UUID(0L, 812L), JobStatus.PENDING);
    child.setJobType(JobExecutionType.CHAIN_STEP);
    child.setScheduledTime(ChainScheduler.CHAIN_LOCK_TIME);
    when(jobCrudStore.findDependants(eq(parent.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    WorkflowCompletionPlan plan = scheduler.planCompletion(parent, false);
    assertEquals(JobStatus.CANCELED, plan.dependencies().get(0).status());
    assertEquals(JobStatus.PENDING, child.getStatus());
    verify(jobBatchStatusStore, never())
        .compareAndSwapStatus(any(), any(), eq(JobStatus.CANCELED), isNull());
    verify(jobTerminalStore, never()).cancelRunningJob(any(), any());
  }

  @Test
  void cancelChain_noConditionalBranchesOnlyCancelsLinearDependants() {
    JobEntity parent = job(new UUID(0L, 70L), JobStatus.FAILED);
    JobEntity child = job(new UUID(0L, 71L), JobStatus.PENDING);
    when(jobCrudStore.findDependants(eq(parent.getId()), anyInt(), anyInt()))
        .thenReturn(List.of(child));
    when(jobCrudStore.findDependants(eq(child.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of());
    when(jobBatchStatusStore.compareAndSwapStatus(
            child.getId(), JobStatus.PENDING, JobStatus.CANCELED, null))
        .thenReturn(true);

    scheduler.cancelChain(parent);

    verify(jobBatchStatusStore)
        .compareAndSwapStatus(child.getId(), JobStatus.PENDING, JobStatus.CANCELED, null);
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
    when(jobCrudStore.findDependants(eq(parent.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(conditionStore.findConditionsByParentJobId(parent.getId()))
        .thenReturn(List.of(first, second, third));
    when(jobCrudStore.findById(pending.getId())).thenReturn(Optional.of(pending));
    when(jobCrudStore.findById(waiting.getId())).thenReturn(Optional.of(waiting));
    when(jobCrudStore.findById(running.getId())).thenReturn(Optional.of(running));
    when(jobBatchStatusStore.compareAndSwapStatus(
            pending.getId(), JobStatus.PENDING, JobStatus.CANCELED, null))
        .thenReturn(true);
    when(jobBatchStatusStore.compareAndSwapStatus(
            waiting.getId(), JobStatus.WAITING, JobStatus.CANCELED, null))
        .thenReturn(true);

    scheduler.cancelChain(parent);

    verify(jobBatchStatusStore)
        .compareAndSwapStatus(pending.getId(), JobStatus.PENDING, JobStatus.CANCELED, null);
    verify(jobBatchStatusStore)
        .compareAndSwapStatus(waiting.getId(), JobStatus.WAITING, JobStatus.CANCELED, null);
    verify(jobBatchStatusStore, never())
        .compareAndSwapStatus(eq(running.getId()), any(), eq(JobStatus.CANCELED), isNull());
    verify(jobTerminalStore, never()).cancelRunningJob(eq(running.getId()), any());
  }

  @Test
  void cancelChain_missingConditionalChildDoesNotCancelAnythingForThatCondition() {
    JobEntity parent = job(new UUID(0L, 90L), JobStatus.FAILED);
    UUID missingChildId = new UUID(0L, 91L);
    WorkflowConditionEntity condition = condition(parent.getId(), missingChildId, 0);
    when(jobCrudStore.findDependants(eq(parent.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of(condition));
    when(jobCrudStore.findById(missingChildId)).thenReturn(Optional.empty());

    scheduler.cancelChain(parent);

    verify(jobBatchStatusStore, never())
        .compareAndSwapStatus(eq(missingChildId), any(), eq(JobStatus.CANCELED), isNull());
    verify(jobTerminalStore, never()).cancelRunningJob(eq(missingChildId), any());
  }

  @Test
  void cancelChain_cancelsWaitingConditionalBranches() {
    JobEntity parent = job(new UUID(0L, 20L), JobStatus.FAILED);
    JobEntity child = job(new UUID(0L, 21L), JobStatus.WAITING);
    WorkflowConditionEntity condition = condition(parent.getId(), child.getId(), 0);
    when(jobCrudStore.findDependants(eq(parent.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of(condition));
    when(jobCrudStore.findById(child.getId())).thenReturn(Optional.of(child));
    when(jobBatchStatusStore.compareAndSwapStatus(
            child.getId(), JobStatus.WAITING, JobStatus.CANCELED, null))
        .thenReturn(true);

    scheduler.cancelChain(parent);

    verify(jobBatchStatusStore)
        .compareAndSwapStatus(child.getId(), JobStatus.WAITING, JobStatus.CANCELED, null);
  }

  @Test
  void cancelChain_branchThatStartedSinceItWasLoadedKeepsItsKeyUntilItsBodyEnds() {
    // A branch claimed after it was loaded is cancelled as a running job and keeps its key.
    JobEntity parent = job(new UUID(0L, 22L), JobStatus.FAILED);
    JobEntity child = job(new UUID(0L, 23L), JobStatus.PENDING);
    child.setBusinessKey("branch-key");
    child.setTimeoutSec(30);
    Instant heldUntil =
        FIXED_NOW.plusSeconds(
            30
                + RatchetOptions.defaults().node().orphanGraceSeconds()
                + RatchetOptions.defaults().node().orphanScanIntervalSeconds());
    WorkflowConditionEntity condition = condition(parent.getId(), child.getId(), 0);
    when(jobCrudStore.findDependants(eq(parent.getId()), anyInt(), anyInt())).thenReturn(List.of());
    when(conditionStore.findConditionsByParentJobId(parent.getId())).thenReturn(List.of(condition));
    when(jobCrudStore.findById(child.getId())).thenReturn(Optional.of(child));
    when(jobBatchStatusStore.compareAndSwapStatus(
            child.getId(), JobStatus.PENDING, JobStatus.CANCELED, null))
        .thenReturn(false);
    when(jobTerminalStore.cancelRunningJob(child.getId(), heldUntil)).thenReturn(true);

    scheduler.cancelChain(parent);

    verify(jobTerminalStore).cancelRunningJob(child.getId(), heldUntil);
    verify(jobTerminalStore, never()).cancelJob(child.getId());
  }

  private WorkflowScheduler eventPublishingScheduler() {
    return new WorkflowScheduler(
        jobCrudStore,
        jobTerminalStore,
        jobBatchStatusStore,
        RatchetOptions.defaults(),
        conditionStore,
        conditionEvaluator,
        FIXED_CLOCK,
        eventPublisher,
        new JakartaAfterCommitRegistrar(txRegistry));
  }

  private AtomicReference<Synchronization> activeTransaction() {
    AtomicReference<Synchronization> synchronization = new AtomicReference<>();
    when(txRegistry.getTransactionStatus()).thenReturn(Status.STATUS_ACTIVE);
    doAnswer(
            invocation -> {
              synchronization.set(invocation.getArgument(0));
              return null;
            })
        .when(txRegistry)
        .registerInterposedSynchronization(any());
    return synchronization;
  }
}

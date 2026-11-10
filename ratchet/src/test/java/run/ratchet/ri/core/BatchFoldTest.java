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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.SerializableBinaryOperator;
import run.ratchet.api.exception.KeyNotFoundException;
import run.ratchet.api.exception.KeyProviderUnavailableException;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.api.exception.UnsupportedEnvelopeVersionException;
import run.ratchet.ri.core.internal.DeadLetterService;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.WorkflowCompletionPlan;
import run.ratchet.ri.core.internal.WorkflowScheduler;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.ri.security.DefaultErrorSanitizer;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.AfterCommitRegistrar.Result;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.ChildResult;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;

class BatchFoldTest {
  private final BatchStore batches = mock(BatchStore.class);
  private final JobCrudStore jobs = mock(JobCrudStore.class);
  private final JobTerminalStore terminal = mock(JobTerminalStore.class);
  private final WorkflowScheduler workflow = mock(WorkflowScheduler.class);
  private final ClassPolicy policy = mock(ClassPolicy.class);
  private final ExecutorProvider executor = mock(ExecutorProvider.class);
  private final ExecutorService pool = mock(ExecutorService.class);
  private final PollerScheduler poller = mock(PollerScheduler.class);
  private final PayloadSerializer serializer = new JsonbTestPayloadSerializer();
  private final UUID id = UUID.randomUUID();
  private final UUID child = UUID.randomUUID();
  private final BatchEntity batch = new BatchEntity();
  private final JobEntity parent = new JobEntity();
  private BatchService service;

  @BeforeEach
  void setup() {
    parent.setId(id);
    parent.setStatus(JobStatus.PENDING);
    parent.setJobType(JobExecutionType.BATCH_PARENT);
    batch.setId(id);
    batch.setTotalItems(1);
    batch.setCompletedItems(1);
    setCombiner(Long::sum);
    when(batches.findBatchById(id)).thenReturn(Optional.of(batch));
    when(jobs.findById(id)).thenReturn(Optional.of(parent));
    when(batches.claimFold(eq(id), any(), any())).thenReturn(true);
    when(batches.findSucceededChildResults(id, null, 500))
        .thenReturn(List.of(new ChildResult(child, "7", "java.lang.Long")));
    when(batches.findSucceededChildResults(id, child, 500)).thenReturn(List.of());
    when(workflow.planCompletion(any(), eq(false), any()))
        .thenReturn(WorkflowCompletionPlan.empty());
    when(workflow.planCompletion(any(), eq(false))).thenReturn(WorkflowCompletionPlan.empty());
    when(terminal.commitCompletion(any())).thenReturn(new JobCompletionResult(true, null));
    when(executor.getJobExecutor()).thenReturn(pool);
    service =
        new BatchService(
            batches,
            jobs,
            mock(JobBatchStatusStore.class),
            terminal,
            mock(MetricsCollector.class),
            mock(InternalEventPublisher.class),
            mock(DeadLetterService.class),
            workflow,
            policy,
            mock(BeanResolver.class),
            Clock.fixed(Instant.parse("2026-11-10T00:00:00Z"), ZoneOffset.UTC),
            action -> Result.NO_ACTIVE_TRANSACTION,
            new BatchCompletionTransaction(),
            serializer);
    service.configureFold(executor, RatchetOptions.defaults(), poller, new DefaultErrorSanitizer());
  }

  @Test
  void successfulFoldPersistsCombinedValue() {
    service.fold(id);
    var plan = ArgumentCaptor.forClass(JobCompletionPlan.class);
    verify(terminal).commitCompletion(plan.capture());
    assertEquals(JobStatus.SUCCEEDED, plan.getValue().terminalStatus());
    assertEquals("7", plan.getValue().resultJson());
    verify(workflow).planCompletion(parent, false, 7L);
  }

  @Test
  void allCompletionPathsScheduleFold() {
    service.afterChildCompletion(new BatchProgress(id, 1, 1, 0, null));
    service.completeIfFinished(id);
    when(batches.findRecoverableBatchIds(100)).thenReturn(List.of(id));
    when(batches.findBatchesByIds(List.of(id))).thenReturn(List.of(batch));
    when(jobs.findByIds(List.of(id))).thenReturn(List.of(parent));
    service.recoverStuckBatches();
    verify(pool, times(3)).submit(any(Runnable.class));
    verifyNoInteractions(terminal);
  }

  @Test
  void rejectedExecutorSubmissionLeavesBatchToRecovery() {
    when(pool.submit(any(Runnable.class))).thenThrow(new RejectedExecutionException("stopping"));
    assertDoesNotThrow(() -> service.scheduleFold(id));
    verifyNoInteractions(terminal);
  }

  @Test
  void failedChildrenCompleteInline() {
    batch.setCompletedItems(0);
    batch.setFailedItems(1);
    service.afterChildCompletion(new BatchProgress(id, 1, 0, 1, null));
    verify(terminal).commitCompletion(any());
    verify(pool, never()).submit(any(Runnable.class));
  }

  @Test
  void pausedParentAndRefusedClaimCannotFold() {
    parent.setStatus(JobStatus.PAUSED);
    service.fold(id);
    verify(batches, never()).claimFold(any(), any(), any());
    parent.setStatus(JobStatus.PENDING);
    when(batches.claimFold(any(), any(), any())).thenReturn(false);
    service.fold(id);
    verifyNoInteractions(terminal);
  }

  @Test
  void missingNullAndTruncatedPartialsFail() {
    for (String value : new String[] {null, "null", "{\"_truncated\":true}"}) {
      parent.setStatus(JobStatus.PENDING);
      when(batches.findSucceededChildResults(id, null, 500))
          .thenReturn(List.of(new ChildResult(child, value, "java.lang.Long")));
      service.fold(id);
      assertEquals(JobStatus.FAILED, parent.getStatus());
      assertTrue(parent.getLastError().contains(child.toString()));
    }
  }

  @Test
  void rowMismatchDefersOnceThenFails() {
    when(batches.findSucceededChildResults(id, null, 500)).thenReturn(List.of());
    service.fold(id);
    verifyNoInteractions(terminal);
    service.fold(id);
    assertEquals(JobStatus.FAILED, parent.getStatus());
    assertTrue(parent.getLastError().contains("count mismatch"));
  }

  @Test
  void transientReadsDefer() {
    when(batches.findSucceededChildResults(id, null, 500))
        .thenThrow(new RatchetTransientStoreException("busy"));
    service.fold(id);
    verifyNoInteractions(terminal);
  }

  @Test
  void keyOutageAndFutureEnvelopeDefer() {
    when(batches.findSucceededChildResults(id, null, 500))
        .thenThrow(
            new KeyProviderUnavailableException("offline", new IllegalStateException("network")));
    service.fold(id);
    verifyNoInteractions(terminal);
    doThrow(new UnsupportedEnvelopeVersionException(99, 1))
        .when(batches)
        .findSucceededChildResults(id, null, 500);
    service.fold(id);
    verifyNoInteractions(terminal);
  }

  @Test
  void permanentCryptoFailuresFailBatch() {
    for (RuntimeException failure :
        List.of(
            new PayloadDecryptionException("bad ciphertext"),
            new KeyNotFoundException("missing key"))) {
      parent.setStatus(JobStatus.PENDING);
      doThrow(failure).when(batches).findSucceededChildResults(id, null, 500);
      service.fold(id);
      assertEquals(JobStatus.FAILED, parent.getStatus());
      assertTrue(parent.getLastError().contains(failure.getClass().getSimpleName()));
    }
  }

  @Test
  void invalidJsonAndOversizedCombinedResultFail() {
    when(batches.findSucceededChildResults(id, null, 500))
        .thenReturn(List.of(new ChildResult(child, "{broken", "java.lang.Long")));
    service.fold(id);
    assertEquals(JobStatus.FAILED, parent.getStatus());
    parent.setStatus(JobStatus.PENDING);
    service.configureFold(
        executor,
        RatchetOptions.builder().payload(p -> p.maxResultBytes(1L)).build(),
        poller,
        new DefaultErrorSanitizer());
    when(batches.findSucceededChildResults(id, null, 500))
        .thenReturn(List.of(new ChildResult(child, "77", "java.lang.Long")));
    service.fold(id);
    assertEquals(JobStatus.FAILED, parent.getStatus());
    assertTrue(parent.getLastError().contains("PayloadTooLarge"));
  }

  @Test
  void staleCountersDiscardOutcomeAndNewWorkWakesPoller() {
    when(workflow.planCompletion(any(), eq(false), any()))
        .thenReturn(new WorkflowCompletionPlan(List.of(), List.of(), true));
    service.fold(id);
    verify(poller).wakeup();
    parent.setStatus(JobStatus.PENDING);
    BatchEntity changed = new BatchEntity();
    changed.setTotalItems(2);
    changed.setCompletedItems(1);
    when(batches.findBatchById(id)).thenReturn(Optional.of(batch), Optional.of(changed));
    service.fold(id);
    verify(terminal, times(1)).commitCompletion(any());
  }

  @Test
  void reducingCompletionRequiresAnOutcome() throws Exception {
    var method =
        BatchService.class.getDeclaredMethod(
            "processBatchCompletion", UUID.class, BatchEntity.class, JobEntity.class);
    method.setAccessible(true);
    var failure =
        assertThrows(
            InvocationTargetException.class, () -> method.invoke(service, id, batch, parent));
    assertTrue(failure.getCause() instanceof IllegalStateException);
  }

  @Test
  void pausedReducingBatchIsLeftAloneByRecoveryAndResumeChecks() {
    parent.setStatus(JobStatus.PAUSED);
    assertDoesNotThrow(() -> assertFalse(service.recoverCompletedBatch(id, batch, parent)));
    assertDoesNotThrow(() -> assertFalse(service.completeIfFinished(id)));
    verify(pool, never()).submit(any(Runnable.class));
    verifyNoInteractions(terminal);
  }

  @Test
  void errorWhileDeserializingPartialFailsBatch() {
    var throwing = mock(PayloadSerializer.class);
    when(throwing.serialize(any())).thenAnswer(call -> serializer.serialize(call.getArgument(0)));
    when(throwing.deserialize(any(String.class), any(Class.class)))
        .thenAnswer(
            call ->
                serializer.deserialize(call.<String>getArgument(0), call.<Class<?>>getArgument(1)));
    when(throwing.deserialize(any(String.class), any(Type.class)))
        .thenThrow(new NoClassDefFoundError("example/Missing"));
    service =
        new BatchService(
            batches,
            jobs,
            mock(JobBatchStatusStore.class),
            terminal,
            mock(MetricsCollector.class),
            mock(InternalEventPublisher.class),
            mock(DeadLetterService.class),
            workflow,
            policy,
            mock(BeanResolver.class),
            Clock.fixed(Instant.parse("2026-11-10T00:00:00Z"), ZoneOffset.UTC),
            action -> Result.NO_ACTIVE_TRANSACTION,
            new BatchCompletionTransaction(),
            throwing);
    service.configureFold(executor, RatchetOptions.defaults(), poller, new DefaultErrorSanitizer());

    service.fold(id);

    assertEquals(JobStatus.FAILED, parent.getStatus());
    assertTrue(parent.getLastError().contains("NoClassDefFoundError"));
  }

  @Test
  void throwingCombinerFailsBatch() {
    when(policy.isAllowed(BatchFoldTest.class.getName())).thenReturn(true);
    setCombiner(BatchFoldTest::broken);
    service.fold(id);
    assertEquals(JobStatus.FAILED, parent.getStatus());
    assertTrue(parent.getLastError().contains("broken reducer"));
    assertFalse(parent.getLastError().contains("s3cret"));
  }

  @Test
  void emptyBatchUsesIdentityWithoutCallingCombiner() {
    batch.setTotalItems(0);
    batch.setCompletedItems(0);
    service.completeEmptyBatch(id, batch);
    assertEquals("0", parent.getJobResult());
    verify(batches, never()).findSucceededChildResults(any(), any(), anyInt());
  }

  private void setCombiner(SerializableBinaryOperator<Long> combiner) {
    batch.setReducer(
        serializer.serialize(
            new BatchReducer.Stored(JobPayloadFactory.fromLambda(combiner), 0L, "java.lang.Long")));
  }

  public static Long broken(Long a, Long b) {
    throw new IllegalStateException("broken reducer password=s3cret");
  }
}

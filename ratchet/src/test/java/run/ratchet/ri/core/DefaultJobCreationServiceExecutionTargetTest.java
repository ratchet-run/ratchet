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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobResult;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.ri.testsupport.EncryptionTestKit;
import run.ratchet.ri.testsupport.StubAfterCommitRegistrar;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

@ExtendWith(MockitoExtension.class)
class DefaultJobCreationServiceExecutionTargetTest {

  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private RecurringJobStore recurringJobStore;
  @Mock private RecurringScheduler recurringScheduler;

  private DefaultJobCreationService service;

  public static void noopTask() {}

  public static void consumeString(String value) {}

  public static boolean jobSucceeded(JobResult<Void> result) {
    return result.isSuccess();
  }

  private static class NoopJobWakeupService extends JobWakeupService {
    @Override
    public void notify(JobPriority priority, boolean immediate, String executionTarget) {}

    @Override
    public void notifyIfNeeded(
        JobExecutionType jobType, JobPriority priority, Duration delay, String executionTarget) {}
  }

  @BeforeEach
  void setUp() {
    service =
        new DefaultJobCreationService(
            jobBatchStatusStore,
            jobTerminalStore,
            jobCrudStore,
            jobBulkStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            recurringJobStore,
            new NoopJobWakeupService(),
            recurringScheduler,
            new DefaultJobInvocationResolver(),
            new JobPayloadInputValidator(),
            null,
            null,
            null,
            null,
            null,
            null,
            Clock.fixed(Instant.parse("2026-05-27T12:00:00Z"), ZoneOffset.UTC),
            new StubAfterCommitRegistrar());

    lenient()
        .when(jobCrudStore.create(any(JobEntity.class)))
        .thenAnswer(invocation -> persist(invocation));
  }

  @AfterEach
  void resetEncryption() {
    EncryptionHolder.disable();
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void batchSubmit_propagatesExecutionTargetToParentAndChildren() {
    DefaultBatchBuilder builder = new DefaultBatchBuilder("batch", service);
    builder.virtual();
    builder.forEach(
        List.of("one", "two"), DefaultJobCreationServiceExecutionTargetTest::consumeString);

    service.submit(builder);

    ArgumentCaptor<JobEntity> parentCaptor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(parentCaptor.capture());
    assertEquals(ExecutorTargets.VIRTUAL, parentCaptor.getValue().getExecutionTarget());

    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore).bulkInsert(childrenCaptor.capture());
    assertExecutionTarget(childrenCaptor.getValue(), ExecutorTargets.VIRTUAL);
  }

  @Test
  void batchSubmit_doesNotGateFrameworkCoordinationPayloadThroughClassPolicy() {
    // An application allowlists only its own packages, which do not cover the framework's
    // coordination class. The batch-parent noop targets that framework class, so gating it would
    // reject every batch submission. This policy allows the child target's package but not
    // run.ratchet.ri.util (JobPlaceholders), isolating the parent-noop path.
    ClassPolicy appPolicy =
        className -> className != null && className.startsWith("run.ratchet.ri.core.");
    DefaultJobCreationService gated =
        new DefaultJobCreationService(
            jobBatchStatusStore,
            jobTerminalStore,
            jobCrudStore,
            jobBulkStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            recurringJobStore,
            new NoopJobWakeupService(),
            recurringScheduler,
            new DefaultJobInvocationResolver(),
            new JobPayloadInputValidator(),
            null,
            null,
            null,
            appPolicy,
            null,
            null,
            Clock.fixed(Instant.parse("2026-05-27T12:00:00Z"), ZoneOffset.UTC),
            new StubAfterCommitRegistrar());

    DefaultBatchBuilder builder = new DefaultBatchBuilder("batch", gated);
    builder.forEach(
        List.of("one", "two"), DefaultJobCreationServiceExecutionTargetTest::consumeString);

    assertDoesNotThrow(() -> gated.submit(builder));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void streamingBatchSubmit_propagatesExecutionTargetToParentAndChildren() {
    DefaultStreamingBatchBuilder<String> builder =
        new DefaultStreamingBatchBuilder<>("streaming-batch", service);
    builder.platform();
    builder.fromStream(Stream.of("one", "two", "three"));
    builder.withChunkSize(2);
    builder.process(DefaultJobCreationServiceExecutionTargetTest::consumeString);

    service.submit(builder);

    ArgumentCaptor<JobEntity> parentCaptor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(parentCaptor.capture());
    assertEquals(ExecutorTargets.PLATFORM, parentCaptor.getValue().getExecutionTarget());

    ArgumentCaptor<List<JobEntity>> childrenCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore, times(2)).bulkInsert(childrenCaptor.capture());
    List<JobEntity> children =
        childrenCaptor.getAllValues().stream().flatMap(List::stream).toList();
    assertExecutionTarget(children, ExecutorTargets.PLATFORM);
  }

  @Test
  void chainSteps_inheritRootExecutionTarget() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceExecutionTargetTest::noopTask, Duration.ZERO);
    builder
        .virtual()
        .then(DefaultJobCreationServiceExecutionTargetTest::noopTask)
        .then(DefaultJobCreationServiceExecutionTargetTest::noopTask);

    service.submit(builder);

    ArgumentCaptor<JobEntity> jobCaptor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore, times(3)).create(jobCaptor.capture());
    assertExecutionTarget(jobCaptor.getAllValues(), ExecutorTargets.VIRTUAL);
  }

  @Test
  void chainSteps_inheritParentEncryptionOptIn() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceExecutionTargetTest::noopTask, Duration.ZERO);
    builder.withEncryptedPayload();
    builder
        .virtual()
        .then(DefaultJobCreationServiceExecutionTargetTest::noopTask)
        .then(DefaultJobCreationServiceExecutionTargetTest::noopTask);

    service.submit(builder);

    ArgumentCaptor<JobEntity> jobCaptor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore, times(3)).create(jobCaptor.capture());
    // Parent plus both chain steps carry the opt-in, so the row mapper encrypts each step's args.
    org.junit.jupiter.api.Assertions.assertTrue(
        jobCaptor.getAllValues().stream().allMatch(JobEntity::isEncryptedPayload));
  }

  @Test
  void workflowBranches_inheritRootExecutionTarget() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceExecutionTargetTest::noopTask, Duration.ZERO);
    builder.platform().thenOnSuccess(DefaultJobCreationServiceExecutionTargetTest::noopTask);

    service.submit(builder);

    ArgumentCaptor<JobEntity> jobCaptor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore, times(2)).create(jobCaptor.capture());
    assertExecutionTarget(jobCaptor.getAllValues(), ExecutorTargets.PLATFORM);
  }

  @Test
  void workflowBranches_persistBuilderRegistrationOrder() {
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceExecutionTargetTest::noopTask, Duration.ZERO);
    builder
        .thenOnSuccess(DefaultJobCreationServiceExecutionTargetTest::noopTask)
        .thenOnFailure(DefaultJobCreationServiceExecutionTargetTest::noopTask);

    service.submit(builder);

    ArgumentCaptor<WorkflowConditionEntity> conditionCaptor =
        ArgumentCaptor.forClass(WorkflowConditionEntity.class);
    verify(workflowConditionStore, times(2)).saveCondition(conditionCaptor.capture());

    assertEquals(
        List.of(0, 1),
        conditionCaptor.getAllValues().stream()
            .map(WorkflowConditionEntity::getDefinitionOrder)
            .toList());
  }

  @Test
  void workflowBranchPredicateExpression_encryptsStoredPayloadArgs() {
    EncryptionTestKit.install(true);
    when(jobCrudStore.findByIdempotencyKey(anyString())).thenReturn(Optional.empty());
    DefaultJobBuilder builder =
        (DefaultJobBuilder)
            DefaultJobBuilder.create(
                service, DefaultJobCreationServiceExecutionTargetTest::noopTask, Duration.ZERO);
    builder.<Void>when(
        DefaultJobCreationServiceExecutionTargetTest::jobSucceeded,
        DefaultJobCreationServiceExecutionTargetTest::noopTask);

    service.submit(builder);

    ArgumentCaptor<WorkflowConditionEntity> conditionCaptor =
        ArgumentCaptor.forClass(WorkflowConditionEntity.class);
    verify(workflowConditionStore).saveCondition(conditionCaptor.capture());
    WorkflowConditionEntity condition = conditionCaptor.getValue();
    String storedExpression = condition.getConditionExpression();

    // args is now a framed ciphertext string, so the key "args" is present but the [] is not.
    assertTrue(storedExpression.contains("\"args\":\""));
    assertFalse(storedExpression.contains("\"args\":[]"));
    assertTrue(
        PayloadEncryptor.decryptArgs(
                storedExpression, EncryptionTarget.predicate(condition.getParentJobId()))
            .contains("\"args\":[]"));
  }

  @Test
  void recurringSubmit_persistsExecutionTargetOnMasterDefinition() {
    when(recurringJobStore.createRecurring(any(RecurringJobDefinition.class)))
        .thenAnswer(invocation -> invocation.<RecurringJobDefinition>getArgument(0).id());

    DefaultRecurringJobBuilder builder =
        new DefaultRecurringJobBuilder(
            "0 0 12 * * ?",
            ZoneOffset.UTC,
            DefaultJobCreationServiceExecutionTargetTest::noopTask,
            service);
    builder.virtual();

    service.submit(builder);

    ArgumentCaptor<RecurringJobDefinition> definitionCaptor =
        ArgumentCaptor.forClass(RecurringJobDefinition.class);
    verify(recurringJobStore).createRecurring(definitionCaptor.capture());
    assertEquals(ExecutorTargets.VIRTUAL, definitionCaptor.getValue().executionTarget());
  }

  private static JobEntity persist(org.mockito.invocation.InvocationOnMock invocation) {
    JobEntity job = invocation.getArgument(0);
    if (job.getId() == null) {
      job.setId(UUID.randomUUID());
    }
    return job;
  }

  private static void assertExecutionTarget(List<JobEntity> jobs, String executionTarget) {
    for (JobEntity job : jobs) {
      assertEquals(executionTarget, job.getExecutionTarget());
    }
  }
}

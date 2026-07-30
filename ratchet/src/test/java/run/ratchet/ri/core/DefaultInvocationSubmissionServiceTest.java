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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.api.event.BatchChunkFailureEvent;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.ri.testsupport.StubAfterCommitRegistrar;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.JobInvocation;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

@ExtendWith(MockitoExtension.class)
class DefaultInvocationSubmissionServiceTest {

  private static final String TARGET = DefaultInvocationSubmissionServiceTest.class.getName();

  @Mock private JobBatchStatusStore jobBatchStatusStore;
  @Mock private JobTerminalStore jobTerminalStore;
  @Mock private JobCrudStore jobCrudStore;
  @Mock private JobBulkStore jobBulkStore;
  @Mock private BatchStore batchStore;
  @Mock private TagStore tagStore;
  @Mock private WorkflowConditionStore workflowConditionStore;
  @Mock private RecurringJobStore recurringJobStore;
  @Mock private RecurringScheduler recurringScheduler;

  private DefaultJobCreationService creationService;
  private DefaultInvocationSubmissionService service;

  public static void sendInvoice(String invoiceId) {}

  public static boolean wasPaid(Object result) {
    return true;
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
    creationService = newCreationService(null);
    service =
        new DefaultInvocationSubmissionService(creationService, new DefaultJobInvocationResolver());
    lenient()
        .when(jobCrudStore.create(any(JobEntity.class)))
        .thenAnswer(DefaultInvocationSubmissionServiceTest::persist);
  }

  private DefaultJobCreationService newCreationService(ClassPolicy classPolicy) {
    return newCreationService(classPolicy, null);
  }

  private DefaultJobCreationService newCreationService(
      ClassPolicy classPolicy, InternalEventPublisher eventPublisher) {
    return new DefaultJobCreationService(
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
        classPolicy,
        eventPublisher,
        null,
        Clock.fixed(Instant.parse("2026-05-27T12:00:00Z"), ZoneOffset.UTC),
        new StubAfterCommitRegistrar());
  }

  private static JobInvocation sendInvoiceInvocation() {
    return new JobInvocation(
        TARGET, "sendInvoice", "(Ljava/lang/String;)V", true, List.of("inv_123"));
  }

  @Test
  void enqueueInvocation_persistsThePreResolvedInvocation() {
    service.enqueueInvocation(sendInvoiceInvocation()).submit();

    ArgumentCaptor<JobEntity> captor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(captor.capture());
    JobEntity job = captor.getValue();
    assertEquals(TARGET, job.getPayload().target());
    assertEquals("sendInvoice", job.getPayload().method());
    assertEquals(List.of("inv_123"), job.getPayload().args());
  }

  @Test
  void scheduleInvocation_appliesTheDelay() {
    service.scheduleInvocation(Duration.ofMinutes(5), sendInvoiceInvocation()).submit();

    ArgumentCaptor<JobEntity> captor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(captor.capture());
    assertEquals(Instant.parse("2026-05-27T12:05:00Z"), captor.getValue().getScheduledTime());
  }

  @Test
  void builderOptions_delegateToTheLambdaBuilder() {
    service
        .enqueueInvocation(sendInvoiceInvocation())
        .withPriority(JobPriority.HIGH)
        .withBusinessKey("invoice-42")
        .withTags("billing")
        .submit();

    ArgumentCaptor<JobEntity> captor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore).create(captor.capture());
    JobEntity job = captor.getValue();
    assertEquals(JobPriority.HIGH, job.getPriority());
    assertEquals("invoice-42", job.getBusinessKey());
  }

  @Test
  void invocationBuilderRejectsBusinessKeyOutsideThePortableContract() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> service.enqueueInvocation(sendInvoiceInvocation()).withBusinessKey("invoice-😀"));

    assertEquals(
        "Business key must contain only printable ASCII characters (U+0020-U+007E)",
        exception.getMessage());
  }

  @Test
  void then_chainsASecondInvocationStep() {
    JobInvocation next =
        new JobInvocation(TARGET, "sendInvoice", "(Ljava/lang/String;)V", true, List.of("inv_2"));

    service.enqueueInvocation(sendInvoiceInvocation()).then(next).submit();

    ArgumentCaptor<JobEntity> captor = ArgumentCaptor.forClass(JobEntity.class);
    verify(jobCrudStore, atLeastOnce()).create(captor.capture());
    List<JobEntity> created = captor.getAllValues();
    assertEquals(2, created.size(), "head job plus one chain step");
    assertEquals(List.of("inv_2"), created.get(1).getPayload().args());
  }

  @Test
  void invocationCondition_persistsAsCustomConditionPayload() {
    JobInvocation predicate =
        new JobInvocation(TARGET, "wasPaid", "(Ljava/lang/Object;)Z", true, List.of());
    WorkflowCondition condition = service.invocationCondition(predicate);
    assertEquals(WorkflowCondition.ConditionType.CUSTOM, condition.type());

    service
        .enqueueInvocation(sendInvoiceInvocation())
        .when(condition, sendInvoiceInvocation())
        .submit();

    ArgumentCaptor<WorkflowConditionEntity> captor =
        ArgumentCaptor.forClass(WorkflowConditionEntity.class);
    verify(workflowConditionStore).saveCondition(captor.capture());
    WorkflowConditionEntity entity = captor.getValue();
    assertEquals(WorkflowCondition.ConditionType.CUSTOM, entity.getConditionType());
    assertNotNull(entity.getConditionExpression());
    org.junit.jupiter.api.Assertions.assertTrue(
        entity.getConditionExpression().contains("wasPaid"),
        "condition expression must persist the invocation's payload JSON");
  }

  @Test
  @SuppressWarnings("unchecked")
  void invocationBatch_persistsOneChildPerItemFromTheFactory() {
    service
        .enqueueInvocationBatch("invoices")
        .forEach(
            List.of("inv_1", "inv_2"),
            id ->
                new JobInvocation(
                    TARGET, "sendInvoice", "(Ljava/lang/String;)V", true, List.of(id)))
        .withMaxRetries(4)
        .withBackoff(BackoffPolicy.FIXED, Duration.ofSeconds(3))
        .submit();

    ArgumentCaptor<List<JobEntity>> captor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore).bulkInsert(captor.capture());
    List<JobEntity> children = captor.getValue();
    assertEquals(2, children.size());
    assertEquals(List.of("inv_1"), children.get(0).getPayload().args());
    assertEquals(List.of("inv_2"), children.get(1).getPayload().args());
    assertEquals(4, children.get(0).getMaxRetries());
    assertEquals(BackoffPolicy.FIXED, children.get(0).getBackoffPolicy());
    assertEquals(3_000, children.get(0).getBackoffParamMs());
  }

  @Test
  void classPolicy_gatesInvocationSubmissions() {
    DefaultJobCreationService gated = newCreationService(className -> false);
    DefaultInvocationSubmissionService gatedService =
        new DefaultInvocationSubmissionService(gated, new DefaultJobInvocationResolver());

    assertThrows(
        SecurityException.class,
        () -> gatedService.enqueueInvocation(sendInvoiceInvocation()).submit());
  }

  @Test
  @SuppressWarnings("unchecked")
  void invocationStreamingBatch_persistsChildrenInChunksFromTheFactory() {
    service
        .invocationStreamingBatch("stream")
        .fromStream(Stream.of("inv_1", "inv_2", "inv_3"))
        .withChunkSize(2)
        .process(
            id ->
                new JobInvocation(
                    TARGET, "sendInvoice", "(Ljava/lang/String;)V", true, List.of(id)))
        .withMaxRetries(2)
        .withBackoff(BackoffPolicy.EXPONENTIAL, Duration.ofMillis(500))
        .start();

    ArgumentCaptor<List<JobEntity>> captor = ArgumentCaptor.forClass(List.class);
    verify(jobBulkStore, org.mockito.Mockito.times(2)).bulkInsert(captor.capture());
    List<List<JobEntity>> chunks = captor.getAllValues();
    assertEquals(2, chunks.get(0).size());
    assertEquals(1, chunks.get(1).size());
    assertEquals(List.of("inv_1"), chunks.get(0).get(0).getPayload().args());
    assertEquals(List.of("inv_3"), chunks.get(1).get(0).getPayload().args());
    assertEquals(2, chunks.get(0).get(0).getMaxRetries());
    assertEquals(BackoffPolicy.EXPONENTIAL, chunks.get(0).get(0).getBackoffPolicy());
    assertEquals(500, chunks.get(0).get(0).getBackoffParamMs());
  }

  @Test
  void chunkFailure_emitsBestEffortEventAndPropagates() {
    List<Object> events = new java.util.concurrent.CopyOnWriteArrayList<>();
    InternalEventPublisher publisher = new InternalEventPublisher() {};
    publisher.addListener(events::add);
    DefaultInvocationSubmissionService failing =
        new DefaultInvocationSubmissionService(
            newCreationService(null, publisher), new DefaultJobInvocationResolver());
    org.mockito.Mockito.doThrow(new RuntimeException("boom")).when(jobBulkStore).bulkInsert(any());

    assertThrows(
        RuntimeException.class,
        () ->
            failing
                .invocationStreamingBatch("stream")
                .fromStream(Stream.of("inv_1"))
                .process(
                    id ->
                        new JobInvocation(
                            TARGET, "sendInvoice", "(Ljava/lang/String;)V", true, List.of(id)))
                .start());

    BatchChunkFailureEvent event =
        events.stream()
            .filter(BatchChunkFailureEvent.class::isInstance)
            .map(BatchChunkFailureEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(0, event.getChunkIndex());
    assertEquals(1, event.getChunkSize());
    assertEquals("boom", event.getFailureReason());
  }

  @Test
  void chunkFailure_withoutMessage_usesExceptionClassNameAsFailureReason() {
    List<Object> events = new java.util.concurrent.CopyOnWriteArrayList<>();
    InternalEventPublisher publisher = new InternalEventPublisher() {};
    publisher.addListener(events::add);
    DefaultInvocationSubmissionService failing =
        new DefaultInvocationSubmissionService(
            newCreationService(null, publisher), new DefaultJobInvocationResolver());
    org.mockito.Mockito.doThrow(new RuntimeException()).when(jobBulkStore).bulkInsert(any());

    assertThrows(
        RuntimeException.class,
        () ->
            failing
                .invocationStreamingBatch("stream")
                .fromStream(Stream.of("inv_1"))
                .process(
                    id ->
                        new JobInvocation(
                            TARGET, "sendInvoice", "(Ljava/lang/String;)V", true, List.of(id)))
                .start());

    BatchChunkFailureEvent event =
        events.stream()
            .filter(BatchChunkFailureEvent.class::isInstance)
            .map(BatchChunkFailureEvent.class::cast)
            .findFirst()
            .orElseThrow();
    assertEquals(RuntimeException.class.getName(), event.getFailureReason());
  }

  private static JobEntity persist(org.mockito.invocation.InvocationOnMock invocation) {
    JobEntity job = invocation.getArgument(0);
    if (job.getId() == null) {
      job.setId(UUID.randomUUID());
    }
    return job;
  }
}

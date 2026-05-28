package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.JobPriority;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.JobPayloadInputValidator;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

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

  private static class NoopJobWakeupService extends JobWakeupService {
    @Override
    public void notify(JobPriority priority, boolean immediate) {}

    @Override
    public void notifyIfNeeded(JobExecutionType jobType, JobPriority priority, Duration delay) {}
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
            Clock.fixed(Instant.parse("2026-05-27T12:00:00Z"), ZoneOffset.UTC));

    lenient()
        .when(jobCrudStore.create(any(JobEntity.class)))
        .thenAnswer(invocation -> persist(invocation));
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

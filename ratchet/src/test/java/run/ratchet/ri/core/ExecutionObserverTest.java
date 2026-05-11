package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionStore;

@ExtendWith(MockitoExtension.class)
class ExecutionObserverTest {

  @Mock private MetricsCollector metricsCollector;
  @Mock private TracingCollector tracingCollector;
  @Mock private InternalEventPublisher eventPublisher;
  @Mock private ExecutionStore executionStore;
  @Mock private ExecutorProvider executorProvider;

  private ExecutionObserver observer;

  private static JobEntity job(long id) {
    JobEntity job = new JobEntity();
    job.setId(new UUID(0L, id));
    job.setJobType(JobExecutionType.SINGLE);
    job.setPriority(JobPriority.NORMAL);
    return job;
  }

  @BeforeEach
  void setUp() {
    observer =
        new ExecutionObserver(
            metricsCollector,
            tracingCollector,
            eventPublisher,
            executionStore,
            executorProvider,
            null);
  }

  @Test
  void recordJobStart_forwardsJobIdentityTypeAndPriorityToMetricsCollector() {
    JobEntity job = job(42L);

    observer.recordJobStart(job);

    verify(metricsCollector).jobStarted(job.getId(), job.getPublicJobType(), job.getPriority());
  }

  @Test
  void recordJobSuccess_passesExecutionDurationToMetricsCollector() {
    JobEntity job = job(42L);

    observer.recordJobSuccess(job, 123L);

    verify(metricsCollector).jobCompleted(job.getId(), job.getPublicJobType(), 123L);
  }

  @Test
  void recordJobFailure_passesProvidedAttemptNumber() {
    JobEntity job = job(42L);
    RuntimeException error = new RuntimeException("boom");

    observer.recordJobFailure(job, error, 2);

    verify(metricsCollector).jobFailed(job.getId(), job.getPublicJobType(), error, 2);
  }

  @Test
  void recordCallbackFailure_passesProvidedAttemptNumber() {
    JobEntity job = job(42L);
    RuntimeException error = new RuntimeException("callback boom");

    observer.recordCallbackFailure(job, error, 3);

    verify(metricsCollector).callbackFailed(job.getId(), job.getPublicJobType(), error, 3);
  }

  @Test
  void recordJobCancellation_doesNotReportFalseCompletion() {
    observer.recordJobCancellation(job(42L));

    verify(metricsCollector, never())
        .jobCompleted(
            ArgumentMatchers.any(UUID.class), ArgumentMatchers.any(), ArgumentMatchers.anyLong());
  }

  @Test
  void publishEvent_delegatesToEventPublisher() {
    Object event = new Object();

    observer.publishEvent(event);

    verify(eventPublisher).publish(event);
  }

  @Test
  void recordSuccessFinalizationRetry_forwardsToMetricsCollector() {
    JobEntity job = job(42L);

    observer.recordSuccessFinalizationRetry(job);

    verify(metricsCollector).successFinalizationRetried(job.getId(), job.getPublicJobType());
  }

  @Test
  void recordSuccessFinalizationMinimal_forwardsToMetricsCollector() {
    JobEntity job = job(42L);

    observer.recordSuccessFinalizationMinimal(job);

    verify(metricsCollector).successFinalizationMinimal(job.getId(), job.getPublicJobType());
  }

  @Test
  void recordSuccessFinalizationStuck_forwardsToMetricsCollector() {
    JobEntity job = job(42L);

    observer.recordSuccessFinalizationStuck(job);

    verify(metricsCollector).successFinalizationStuck(job.getId(), job.getPublicJobType());
  }

  @Test
  void startExecution_delegatesToExecutionStoreWithAttemptAndNode() {
    UUID jobId = new UUID(0L, 46L);
    JobExecutionEntity saved = JobExecutionEntity.start(jobId, 4, "node-a");
    when(executionStore.saveExecution(ArgumentMatchers.any(JobExecutionEntity.class)))
        .thenReturn(saved);

    JobExecutionEntity result = observer.startExecution(jobId, 4, "node-a");

    assertSame(saved, result);
    ArgumentCaptor<JobExecutionEntity> executionCaptor =
        ArgumentCaptor.forClass(JobExecutionEntity.class);
    verify(executionStore).saveExecution(executionCaptor.capture());
    assertEquals(jobId, executionCaptor.getValue().getJobId());
    assertEquals(4, executionCaptor.getValue().getAttempt());
    assertEquals("node-a", executionCaptor.getValue().getNodeId());
  }

  @Test
  void saveExecution_returnsStoreResult() {
    JobExecutionEntity execution = JobExecutionEntity.start(new UUID(0L, 47L), 1, "node-a");
    JobExecutionEntity saved = JobExecutionEntity.start(new UUID(0L, 47L), 1, "node-a");
    when(executionStore.saveExecution(execution)).thenReturn(saved);

    assertSame(saved, observer.saveExecution(execution));
  }

  @Test
  void scheduleDelayedJobReadyCallback_schedulesCallbackOnExecutor() {
    Runnable callback = () -> {};
    ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
    observer =
        new ExecutionObserver(
            metricsCollector,
            tracingCollector,
            eventPublisher,
            executionStore,
            executorProvider,
            callback);
    when(executorProvider.getScheduledExecutor()).thenReturn(scheduledExecutor);

    observer.scheduleDelayedJobReadyCallback(250L);

    verify(scheduledExecutor).schedule(callback, 250L, TimeUnit.MILLISECONDS);
  }

  @Test
  void scheduleDelayedJobReadyCallback_doesNothingWhenCallbackIsMissing() {
    observer.scheduleDelayedJobReadyCallback(250L);

    verifyNoInteractions(executorProvider);
  }

  @Test
  void startExecutionScope_addsSignalTracingAttributes() {
    JobEntity job = job(42L);
    job.setSignalKey("approval");
    job.setSignalOutcome("REJECTED");
    job.setSignalDeliveredBy("admin");
    job.setCreatedAt(Instant.parse("2026-01-01T00:00:00Z"));
    job.setSignalDeliveredAt(Instant.parse("2026-01-01T00:00:02Z"));

    observer.startExecutionScope(job);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> attributesCaptor = ArgumentCaptor.forClass(Map.class);
    verify(tracingCollector)
        .jobExecutionStarted(
            ArgumentMatchers.eq(job.getId()),
            ArgumentMatchers.eq(job.getPublicJobType()),
            ArgumentMatchers.eq(job.getPriority()),
            ArgumentMatchers.eq(Map.of()),
            attributesCaptor.capture());
    assertEquals("approval", attributesCaptor.getValue().get("ratchet.signal.key"));
    assertEquals("REJECTED", attributesCaptor.getValue().get("ratchet.signal.outcome"));
    assertEquals("true", attributesCaptor.getValue().get("ratchet.signal.delivered_by.present"));
    assertEquals("2000", attributesCaptor.getValue().get("ratchet.signal.wait_ms"));
  }

  @Test
  void startExecutionScope_withoutTracingCollectorReturnsNoOpScope() {
    observer =
        new ExecutionObserver(
            metricsCollector, null, eventPublisher, executionStore, executorProvider, null);

    TracingCollector.ExecutionScope scope = observer.startExecutionScope(job(48L));

    assertSame(TracingCollector.NoOpExecutionScope.INSTANCE, scope);
  }
}

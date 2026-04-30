package run.ratchet.ri.core;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import run.ratchet.api.JobPriority;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionStore;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExecutionObserverTest {

  @Mock private MetricsCollector metricsCollector;
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
            metricsCollector, eventPublisher, executionStore, executorProvider, null);
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
  void recordJobCancellation_doesNotReportFalseCompletion() {
    observer.recordJobCancellation(job(42L));

    verify(metricsCollector, never())
        .jobCompleted(
            ArgumentMatchers.any(UUID.class), ArgumentMatchers.any(), ArgumentMatchers.anyLong());
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
}

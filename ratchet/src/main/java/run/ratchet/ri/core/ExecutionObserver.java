package run.ratchet.ri.core;

import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.spi.ExecutionStore;
import java.util.concurrent.TimeUnit;

/**
 * Bundles metrics, event publishing, and execution history to reduce {@link JobTask}'s dependency
 * count.
 *
 * @see JobTask
 */
public class ExecutionObserver {

  private final MetricsCollector metricsCollector;
  private final InternalEventPublisher eventPublisher;
  private final ExecutionStore executionStore;
  private final ExecutorProvider executorProvider;
  private final Runnable delayedJobReadyCallback;

  // Required by CDI proxy
  protected ExecutionObserver() {
    this.metricsCollector = null;
    this.eventPublisher = null;
    this.executionStore = null;
    this.executorProvider = null;
    this.delayedJobReadyCallback = null;
  }

  public ExecutionObserver(
      MetricsCollector metricsCollector,
      InternalEventPublisher eventPublisher,
      ExecutionStore executionStore,
      ExecutorProvider executorProvider,
      Runnable delayedJobReadyCallback) {
    this.metricsCollector = metricsCollector;
    this.eventPublisher = eventPublisher;
    this.executionStore = executionStore;
    this.executorProvider = executorProvider;
    this.delayedJobReadyCallback = delayedJobReadyCallback;
  }

  public void recordJobStart(JobEntity job) {
    metricsCollector.jobStarted(job.getId(), job.getPublicJobType(), job.getPriority());
  }

  public void recordJobSuccess(JobEntity job, long executionTimeMs) {
    metricsCollector.jobCompleted(job.getId(), job.getPublicJobType(), executionTimeMs);
  }

  public void recordJobFailure(JobEntity job, Throwable ex, int attempt) {
    metricsCollector.jobFailed(job.getId(), job.getPublicJobType(), ex, attempt);
  }

  public void recordCallbackFailure(JobEntity job, Throwable ex, int attempt) {
    metricsCollector.callbackFailed(job.getId(), job.getPublicJobType(), ex, attempt);
  }

  public void recordJobCancellation(JobEntity job) {
    // The public MetricsCollector SPI has no cancellation callback.
  }

  public void publishEvent(Object event) {
    eventPublisher.publish(event);
  }

  public void scheduleDelayedJobReadyCallback(long delayMs) {
    if (delayedJobReadyCallback != null) {
      executorProvider
          .getScheduledExecutor()
          .schedule(delayedJobReadyCallback, delayMs, TimeUnit.MILLISECONDS);
    }
  }

  public JobExecutionEntity startExecution(Long jobId, int attemptNumber, String nodeId) {
    JobExecutionEntity execution = JobExecutionEntity.start(jobId, attemptNumber, nodeId);
    return executionStore.saveExecution(execution);
  }

  public JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    return executionStore.saveExecution(execution);
  }
}

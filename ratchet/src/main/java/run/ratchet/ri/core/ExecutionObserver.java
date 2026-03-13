package run.ratchet.ri.core;

import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.spi.ExecutionStore;
import java.util.concurrent.TimeUnit;

/**
 * Consolidates observability-related dependencies for job execution.
 *
 * <p>This facade bundles metrics collection, event publishing, and execution history tracking into
 * a single service to reduce dependency coupling in {@link JobTask}.
 *
 * @see JobTask
 * @see PostExecutionHandler
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

  /**
   * Creates a new ExecutionObserver.
   *
   * @param metricsCollector collects job execution metrics
   * @param eventPublisher publishes internal scheduler events
   * @param executionStore persists execution history entries
   * @param executorProvider provides scheduled executor for delayed callbacks
   * @param delayedJobReadyCallback callback invoked when a delayed job becomes ready; may be null
   */
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

  /**
   * Records the start of a job execution in the metrics collector.
   *
   * @param job the job entity being started
   */
  public void recordJobStart(JobEntity job) {
    metricsCollector.jobStarted(job.getId(), job.getPublicJobType(), job.getPriority());
  }

  /**
   * Records a successful job completion in the metrics collector.
   *
   * @param job the job entity that succeeded
   */
  public void recordJobSuccess(JobEntity job) {
    metricsCollector.jobCompleted(job.getId(), job.getPublicJobType(), 0);
  }

  /**
   * Records a job failure in the metrics collector.
   *
   * @param job the job entity that failed
   * @param ex the exception that caused the failure
   */
  public void recordJobFailure(JobEntity job, Throwable ex) {
    metricsCollector.jobFailed(job.getId(), job.getPublicJobType(), ex, job.getAttempts());
  }

  /**
   * Records a job cancellation. Delegates to metrics collector for cancellation tracking.
   *
   * @param job the job entity that was cancelled
   */
  public void recordJobCancellation(JobEntity job) {
    // Cancellation is treated as a completion with zero execution time
    metricsCollector.jobCompleted(job.getId(), job.getPublicJobType(), 0);
  }

  /**
   * Records a job retry in the metrics collector.
   *
   * @param job the job entity being retried
   */
  public void recordJobRetry(JobEntity job) {
    metricsCollector.jobFailed(job.getId(), job.getPublicJobType(), null, job.getAttempts());
  }

  /**
   * Publishes a scheduler event through the internal event publisher.
   *
   * @param event the event to publish
   */
  public void publishEvent(Object event) {
    eventPublisher.publish(event);
  }

  /**
   * Schedules a callback to notify when a delayed job becomes ready for execution.
   *
   * @param delayMs the delay in milliseconds before the job becomes ready
   */
  public void scheduleDelayedJobReadyCallback(long delayMs) {
    if (delayedJobReadyCallback != null) {
      executorProvider
          .getScheduledExecutor()
          .schedule(delayedJobReadyCallback, delayMs, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Creates and persists a new execution history entry for a job attempt.
   *
   * @param jobId the job being executed
   * @param attemptNumber the current attempt number (1-based)
   * @param nodeId the cluster node executing the job
   * @return the persisted execution entity
   */
  public JobExecutionEntity startExecution(Long jobId, int attemptNumber, String nodeId) {
    JobExecutionEntity execution = JobExecutionEntity.start(jobId, attemptNumber, nodeId);
    return executionStore.saveExecution(execution);
  }

  /**
   * Persists an updated execution entity (e.g., after marking it as succeeded, failed, or
   * canceled).
   *
   * @param execution the execution entity to save
   * @return the persisted entity
   */
  public JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    return executionStore.saveExecution(execution);
  }
}

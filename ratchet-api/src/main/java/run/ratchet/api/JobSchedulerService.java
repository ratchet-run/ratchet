package run.ratchet.api;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZoneId;
import java.util.function.Consumer;

/**
 * Primary entry point for scheduling background jobs.
 *
 * <p>Implementations provide job creation, batch processing, recurring scheduling, and job
 * replacement capabilities. All operations are transactional.
 */
public interface JobSchedulerService {

  /** Enqueues a task for immediate execution, returning a builder for further configuration. */
  JobBuilder enqueue(SerializableCheckedRunnable task);

  /** Enqueues a task for immediate execution with no further configuration. */
  JobHandle enqueueNow(SerializableCheckedRunnable task);

  /** Schedules a task to execute after the specified delay. */
  JobBuilder schedule(Duration delay, SerializableCheckedRunnable task);

  /** Creates a batch builder for parallel execution of multiple tasks. */
  BatchBuilder enqueueBatch(String name);

  /** Creates a streaming batch builder for memory-efficient processing of large datasets. */
  <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name);

  /** Schedules a recurring job based on a cron expression. */
  RecurringJobBuilder scheduleRecurring(
      String cron, ZoneId zone, SerializableCheckedRunnable task);

  /** Replaces an existing job with a new one. */
  JobHandle replace(
      long jobId, Duration delay, SerializableCheckedRunnable newTask, JobOptions opts);

  /**
   * Cancels a job by its ID.
   *
   * <p>If the job is PENDING, it transitions directly to CANCELED. If the job is RUNNING, it
   * transitions to CANCELED and the executor should check status before committing results. Jobs in
   * terminal states (SUCCEEDED, FAILED, CANCELED) cannot be canceled.
   *
   * @param jobId the ID of the job to cancel
   * @return true if the job was successfully canceled, false if the job was not found or already in
   *     a terminal state
   */
  boolean cancelJob(long jobId);

  /**
   * Registers a programmatic event listener that receives all scheduler events.
   *
   * <p>For type-safe event observation, use CDI {@code @Observes} with specific event types
   * instead. This method is intended for non-CDI contexts or when receiving all events is desired.
   *
   * <p><b>Synchronous dispatch — latency warning.</b> Listeners are invoked synchronously on the
   * publishing thread, which is typically the job execution thread. A slow listener creates
   * unbounded latency on the job hot path and can stall the scheduler. This is intentional — event
   * publication participates in the same transaction as the state change it announces — but it
   * means any listener that does heavyweight work (I/O, network calls, cross-system notifications)
   * MUST offload to its own thread pool. For CDI observers of the same events, prefer
   * {@code @ObservesAsync} when the observer does not need to participate in the source
   * transaction.
   *
   * <p>Event types delivered (all extend {@link
   * run.ratchet.api.event.AbstractJobSchedulerEvent}):
   *
   * <ul>
   *   <li><b>Job lifecycle:</b> {@code JobStartedEvent}, {@code JobCompletedEvent}, {@code
   *       JobFailedEvent}, {@code JobCancellingEvent}, {@code JobCancelledEvent}, {@code
   *       JobPausedEvent}, {@code JobResumedEvent}, {@code JobRetryingEvent}
   *   <li><b>Batch:</b> {@code BatchCompletingEvent}, {@code BatchCompletedEvent}
   *   <li><b>Chain/Workflow:</b> {@code ChainStartedEvent}, {@code ChainCompletedEvent}, {@code
   *       ChainFailedEvent}, {@code WorkflowBranchTriggeredEvent}
   *   <li><b>Observability:</b> {@code JobDlqEvent}, {@code PerformanceMetricsEvent}
   * </ul>
   *
   * @param listener a consumer that receives all scheduler events
   */
  void addEventListener(Consumer<Object> listener);

  /**
   * Removes a previously registered event listener.
   *
   * @param listener the listener to remove
   */
  void removeEventListener(Consumer<Object> listener);

  /**
   * Pauses a job, preventing it from being picked up for execution.
   *
   * <p>Only PENDING or FAILED jobs can be paused. The job's previous status is recorded so it can
   * be restored on resume. Jobs in RUNNING state cannot be paused (cancel them instead).
   *
   * <p>Idempotent: pausing an already-PAUSED job returns {@code true} without error.
   *
   * @param jobId the ID of the job to pause
   * @return true if the job was paused or was already paused, false if the job was not found or in
   *     an incompatible state (RUNNING, SUCCEEDED, CANCELED)
   */
  boolean pauseJob(long jobId);

  /**
   * Resumes a paused job, making it eligible for execution again.
   *
   * <p>The job returns to the status it had before being paused. Resuming a previously PENDING job
   * makes it eligible for polling again. Resuming a previously FAILED job restores it to FAILED
   * without retrying it.
   *
   * <p>Idempotent: resuming a non-paused job returns {@code false} without error.
   *
   * @param jobId the ID of the job to resume
   * @return true if the job was resumed, false if the job was not found or not in PAUSED state
   */
  boolean resumeJob(long jobId);

  /**
   * Retries a failed job by resetting it to PENDING status.
   *
   * <p>This is the primary mechanism for manual retry of jobs in the Dead Letter Queue. The job's
   * attempt counter is reset to 0, error information is cleared, and scheduled time is set to now
   * so the job becomes immediately eligible for execution.
   *
   * <p>Only FAILED jobs can be retried. Jobs in other states return {@code false}.
   *
   * @param jobId the ID of the failed job to retry
   * @return true if the job was successfully reset to PENDING, false if not found or not FAILED
   */
  boolean retryJob(long jobId);

  /**
   * Cancels all recurring jobs associated with the specified tag.
   *
   * @param tag the tag identifying the recurring jobs to cancel
   * @return the number of jobs canceled
   */
  int cancelRecurringJobsByTag(String tag);

  /**
   * Cancels the active recurring job with the specified business key.
   *
   * <p>This is the primary mechanism for replacing a recurring job definition during redeployment.
   * Only jobs in active states (PENDING, RUNNING, PAUSED) with matching business key and recurring
   * job type are affected.
   *
   * @param businessKey the business key identifying the recurring job to cancel
   * @return the number of jobs canceled (0 or 1, since business keys are active-unique)
   */
  int cancelRecurringJobByBusinessKey(String businessKey);
}

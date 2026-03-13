package run.ratchet.spi;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * An interface for collecting job execution metrics. This interface is intended to be implemented
 * by custom monitoring tools or integrations, enabling the capture of lifecycle events for
 * scheduled jobs.
 *
 * <p>The collector is notified for three lifecycle transitions:
 *
 * <ul>
 *   <li>job start
 *   <li>successful completion
 *   <li>failure
 * </ul>
 *
 * <p>Implementers can use this interface to gather metrics for analytics, monitoring, and debugging
 * purposes. Examples may include tracking the number of jobs started, completion times, failure
 * rates, and failure causes.
 */
public interface MetricsCollector {

  /**
   * Notifies that a job has started execution.
   *
   * @param jobId The unique identifier of the job that has started.
   * @param type The type of the job, indicating its purpose and execution pattern.
   * @param priority The priority level of the job, influencing scheduling and processing order.
   */
  void jobStarted(long jobId, JobType type, JobPriority priority);

  /**
   * Notifies that a job has completed execution.
   *
   * @param jobId The unique identifier of the job that has completed.
   * @param type The type of the job, indicating its purpose and execution pattern.
   * @param executionTimeMs the terminal execution time for the completed attempt in milliseconds
   */
  void jobCompleted(long jobId, JobType type, long executionTimeMs);

  /**
   * Notifies that a job has failed during execution.
   *
   * @param jobId The unique identifier of the job that has failed.
   * @param type The type of the job, indicating its purpose and execution pattern.
   * @param cause The exception or error that caused the job to fail.
   * @param attempt the 1-based attempt number, including the failure being reported
   */
  void jobFailed(long jobId, JobType type, Throwable cause, int attempt);
}

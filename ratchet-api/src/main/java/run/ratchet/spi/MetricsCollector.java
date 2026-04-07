package run.ratchet.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * An interface for collecting job execution metrics. This interface is intended to be implemented
 * by custom monitoring tools or integrations, enabling the capture of lifecycle events for
 * scheduled jobs.
 *
 * <p>This interface is marked {@link Incubating} — additional lifecycle callbacks (e.g. retry,
 * timeout, DLQ) may be added in future releases without following the normal deprecation cycle.
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
@Incubating
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

  /**
   * Notifies that a job lifecycle callback (for example {@code onSuccess} or {@code onFailure})
   * threw an exception.
   *
   * <p>Callback failures do not fail the parent job by design — the job's primary work has already
   * completed. This hook exists so operators can observe otherwise-silent callback breakage via
   * their metrics backend. Default implementation is a no-op; collectors that care should override.
   *
   * @param jobId the unique identifier of the parent job whose callback failed
   * @param type the type of the parent job
   * @param cause the exception thrown from the callback
   * @param attempt the 1-based invocation count for the callback
   */
  default void callbackFailed(long jobId, JobType type, Throwable cause, int attempt) {
    // default no-op
  }
}

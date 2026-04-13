package run.ratchet.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Receives job lifecycle events (start, success, failure) for metrics collection. Additional
 * callbacks may be added in future releases.
 */
@Incubating
public interface MetricsCollector {

  /** Called when a job starts execution. */
  void jobStarted(long jobId, JobType type, JobPriority priority);

  /**
   * Called when a job completes successfully.
   *
   * @param executionTimeMs wall-clock duration of the completed attempt
   */
  void jobCompleted(long jobId, JobType type, long executionTimeMs);

  /**
   * Called when a job fails.
   *
   * @param attempt the 1-based attempt number including this failure
   */
  void jobFailed(long jobId, JobType type, Throwable cause, int attempt);

  /**
   * Called when a lifecycle callback ({@code onSuccess}/{@code onFailure}) throws. Callback
   * failures never fail the parent job. Default is a no-op.
   */
  default void callbackFailed(long jobId, JobType type, Throwable cause, int attempt) {
    // default no-op
  }
}

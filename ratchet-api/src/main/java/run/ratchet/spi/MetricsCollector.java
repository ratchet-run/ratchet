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

  /** Called when a successful execution must retry a transient store finalization conflict. */
  void successFinalizationRetried(long jobId, JobType type);

  /**
   * Called when a successful execution falls back to a minimal terminal success write after
   * exhausting full-result finalization retries.
   */
  void successFinalizationMinimal(long jobId, JobType type);

  /**
   * Called when a successful execution cannot persist either full or minimal success due to
   * repeated transient store conflicts and must be left RUNNING for later recovery.
   */
  void successFinalizationStuck(long jobId, JobType type);

  /** Called when the poller hits a transient store conflict while claiming work for an execution role. */
  void claimTransientFailure(String executionType);

  /** Called after the poller claims executable work for a specific execution role. */
  void jobsClaimed(String executionType, int claimedCount);

  /** Called when a submission gate blocks local execution of a claimed job. */
  void gateRejected(String executionType, String gateStatus);

  /** Called when the local node directly wakes its poller in response to new work. */
  void localWakeup(String source);

  /**
   * Called when a cluster wakeup publish attempt is observed.
   *
   * @param transport cluster transport, e.g. {@code jms}
   * @param outcome publish outcome, e.g. {@code success}, {@code failure}, {@code skipped}
   */
  void clusterWakeupPublished(String transport, String outcome);

  /**
   * Called when a cluster wakeup message is observed on the receiving side.
   *
   * @param transport cluster transport, e.g. {@code jms}
   * @param outcome receive outcome, e.g. {@code delivered}, {@code ignored_self}
   */
  void clusterWakeupReceived(String transport, String outcome);

  /**
   * Called when a lifecycle callback ({@code onSuccess}/{@code onFailure}) throws. Callback
   * failures never fail the parent job. Default is a no-op.
   */
  default void callbackFailed(long jobId, JobType type, Throwable cause, int attempt) {
    // default no-op
  }
}

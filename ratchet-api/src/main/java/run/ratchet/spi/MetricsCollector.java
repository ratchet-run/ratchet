package run.ratchet.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.api.SignalDecision;

/**
 * Receives job lifecycle events (start, success, failure) for metrics collection. Additional
 * callbacks may be added in future releases.
 */
@Incubating
public interface MetricsCollector {

  /** Called when a job starts execution. */
  void jobStarted(UUID jobId, JobType type, JobPriority priority);

  /**
   * Called when a job completes successfully.
   *
   * @param jobId job that completed; never {@code null}
   * @param type public job type; never {@code null}
   * @param executionTimeMs wall-clock duration of the completed attempt
   */
  void jobCompleted(UUID jobId, JobType type, long executionTimeMs);

  /**
   * Called when a job fails.
   *
   * @param jobId job that failed; never {@code null}
   * @param type public job type; never {@code null}
   * @param cause failure that ended the attempt; never {@code null}
   * @param attempt the 1-based attempt number including this failure
   */
  void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt);

  /** Called when a successful execution must retry a transient store finalization conflict. */
  void successFinalizationRetried(UUID jobId, JobType type);

  /**
   * Called when a successful execution falls back to a minimal terminal success write after
   * exhausting full-result finalization retries.
   */
  void successFinalizationMinimal(UUID jobId, JobType type);

  /**
   * Called when a successful execution cannot persist either full or minimal success due to
   * repeated transient store conflicts and must be left RUNNING for later recovery.
   */
  void successFinalizationStuck(UUID jobId, JobType type);

  /**
   * Called when the poller hits a transient store conflict while claiming work for an execution
   * role.
   */
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
  default void callbackFailed(UUID jobId, JobType type, Throwable cause, int attempt) {
    // default no-op
  }

  /** Called when a job is created in WAITING status for an external signal. */
  default void signalWaiting(UUID jobId, JobType type, String signalKey) {
    // default no-op
  }

  /** Called after a signal delivery transitions a job from WAITING to PENDING. */
  default void signalDelivered(
      UUID jobId, JobType type, String signalKey, SignalDecision.Outcome outcome) {
    // default no-op
  }

  /** Called when a signal-waiting job times out. */
  default void signalTimedOut(UUID jobId, JobType type, String signalKey) {
    // default no-op
  }

  /** Called when a signal-waiting job is cancelled before delivery. */
  default void signalCancelled(UUID jobId, JobType type, String signalKey) {
    // default no-op
  }

  /**
   * Called when the store finishes a timed operation on the hot path.
   *
   * @param store backend/store identifier, e.g. {@code mysql}
   * @param operation logical operation, e.g. {@code claim_lookup} or {@code mark_succeeded}
   * @param outcome outcome label, e.g. {@code success}, {@code miss}, or {@code transient_failure}
   * @param durationNanos elapsed wall-clock time in nanoseconds
   */
  default void storeOperation(String store, String operation, String outcome, long durationNanos) {
    // default no-op
  }

  /**
   * Called when the poller updates its claim-path circuit breaker state.
   *
   * @param breakerName logical breaker identifier, e.g. {@code store.claim}
   * @param state breaker state enum name, e.g. {@code CLOSED} or {@code OPEN}
   */
  default void pollerBreakerState(String breakerName, String state) {
    // default no-op
  }
}

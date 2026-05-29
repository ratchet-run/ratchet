package run.ratchet.store.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobStatus;

/**
 * Non-terminal status operations: generic status updates, CAS transitions, pickup, and batch tag
 * cancellation.
 *
 * <p>Terminal transitions live on {@link JobTerminalStore}; retry scheduling on {@link
 * JobRetryStore}; pause semantics on {@link JobPauseStore}; recurring-master cancellation on {@link
 * RecurringJobStore}.
 */
@Incubating
public interface JobBatchStatusStore {

  /**
   * Updates a job status. Transaction attribute: {@code REQUIRED}.
   *
   * @param id job id to update; never {@code null}
   * @param status new status to persist; never {@code null}
   * @param errorMessage error message to record on the row, or {@code null} to leave the column
   *     untouched
   */
  void updateJobStatus(UUID id, JobStatus status, String errorMessage);

  /**
   * Atomic compare-and-swap status transition. Transaction attribute: {@code REQUIRED}.
   *
   * @param id job id to transition; never {@code null}
   * @param expected status the row must currently hold for the swap to succeed; never {@code null}
   * @param newStatus status to write when {@code expected} matches; never {@code null}
   * @param error error message to record alongside {@code newStatus}, or {@code null} to leave the
   *     column untouched
   * @return {@code true} when the row was at {@code expected} and was updated to {@code newStatus},
   *     {@code false} otherwise (lost race or row missing)
   */
  boolean compareAndSwapStatus(UUID id, JobStatus expected, JobStatus newStatus, String error);

  /**
   * Claims one job for a node. Transaction attribute: {@code REQUIRED}.
   *
   * @param id job id to claim; never {@code null}
   * @param nodeId stable identity of the claiming node; never {@code null} or blank
   * @return {@code true} when the claim succeeded, {@code false} when the job was already claimed
   *     or had transitioned out of a claimable state
   */
  boolean tryPickUpJob(UUID id, String nodeId);

  /**
   * Resets one running job owned by a node. Transaction attribute: {@code REQUIRED}.
   *
   * @param id job id to reset; never {@code null}
   * @param nodeId stable identity of the owning node; never {@code null} or blank
   * @return {@code true} when the row was RUNNING and owned by {@code nodeId} and was reset, {@code
   *     false} otherwise
   */
  boolean resetRunningJob(UUID id, String nodeId);

  /**
   * Resets all running jobs owned by a node. Transaction attribute: {@code REQUIRED}.
   *
   * @param nodeId stable identity of the owning node whose claims should all be released; never
   *     {@code null} or blank
   * @return number of rows reset to PENDING
   */
  int resetRunningJobs(String nodeId);

  /**
   * Cancels all active non-recurring jobs with the given tag in a single bulk operation.
   *
   * <p>Affects only jobs in PENDING, PAUSED, or WAITING. Skips RUNNING and recurring jobs.
   * Implementations MUST execute as a bulk statement, not a per-row loop.
   *
   * @param tag tag whose jobs should be cancelled; never {@code null} or blank
   * @return the number of non-recurring jobs transitioned to CANCELED
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  int cancelJobsByTag(String tag);
}

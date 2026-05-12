package run.ratchet.store.spi;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobStatus;

/**
 * Non-terminal status operations: generic status updates, CAS transitions, pickup, and batch /
 * orphan / recurring-cancel bulk operations.
 *
 * <p>Terminal transitions live on {@link JobTerminalStore}; retry scheduling on {@link
 * JobRetryStore}; pause semantics on {@link JobPauseStore}.
 */
@Incubating
public interface JobBatchStatusStore {

  /** Updates a job status. Transaction attribute: {@code REQUIRED}. */
  void updateJobStatus(UUID id, JobStatus status, String errorMessage);

  /** Atomic compare-and-swap status transition. Transaction attribute: {@code REQUIRED}. */
  boolean compareAndSwapStatus(UUID id, JobStatus expected, JobStatus newStatus, String error);

  /** Claims one job for a node. Transaction attribute: {@code REQUIRED}. */
  boolean tryPickUpJob(UUID id, String nodeId);

  /** Resets one running job owned by a node. Transaction attribute: {@code REQUIRED}. */
  boolean resetRunningJob(UUID id, String nodeId);

  /** Resets all running jobs owned by a node. Transaction attribute: {@code REQUIRED}. */
  int resetRunningJobs(String nodeId);

  /**
   * Cancels all active non-recurring jobs with the given tag in a single bulk operation.
   *
   * <p>Affects only jobs in PENDING, PAUSED, or WAITING. Skips RUNNING and recurring jobs.
   * Implementations MUST execute as a bulk statement, not a per-row loop.
   *
   * @return the number of non-recurring jobs transitioned to CANCELED
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  int cancelJobsByTag(String tag);

  /** Cancels recurring jobs by tag. Transaction attribute: {@code REQUIRED}. */
  int cancelRecurringJobsByTag(String tag);

  /** Cancels a recurring job by business key. Transaction attribute: {@code REQUIRED}. */
  int cancelRecurringJobByBusinessKey(String businessKey);

  /** Cancels recurring jobs by business key. Transaction attribute: {@code REQUIRED}. */
  int cancelRecurringJobsByBusinessKeys(Set<String> businessKeys);

  /** Cancels unregistered recurring annotation jobs. Transaction attribute: {@code REQUIRED}. */
  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);
}

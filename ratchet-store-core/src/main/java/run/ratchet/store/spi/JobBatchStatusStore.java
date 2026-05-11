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

  void updateJobStatus(UUID id, JobStatus status, String errorMessage);

  /** Atomic compare-and-swap status transition. */
  boolean compareAndSwapStatus(UUID id, JobStatus expected, JobStatus newStatus, String error);

  boolean tryPickUpJob(UUID id, String nodeId);

  boolean resetRunningJob(UUID id, String nodeId);

  int resetRunningJobs(String nodeId);

  /**
   * Cancels all active non-recurring jobs with the given tag in a single bulk operation.
   *
   * <p>Affects only jobs in PENDING, PAUSED, or WAITING. Skips RUNNING and recurring jobs.
   * Implementations MUST execute as a bulk statement, not a per-row loop.
   *
   * @return the number of non-recurring jobs transitioned to CANCELED
   */
  int cancelJobsByTag(String tag);

  int cancelRecurringJobsByTag(String tag);

  int cancelRecurringJobByBusinessKey(String businessKey);

  int cancelRecurringJobsByBusinessKeys(Set<String> businessKeys);

  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);
}

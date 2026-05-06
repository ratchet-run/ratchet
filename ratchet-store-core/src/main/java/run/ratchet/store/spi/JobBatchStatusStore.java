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

  int cancelRecurringJobsByTag(String tag);

  int cancelRecurringJobByBusinessKey(String businessKey);

  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);
}

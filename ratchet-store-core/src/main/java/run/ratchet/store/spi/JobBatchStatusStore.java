package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.Set;

/**
 * Non-terminal status operations: generic status updates, CAS transitions, pickup, and batch /
 * orphan / recurring-cancel bulk operations.
 *
 * <p>Separated from {@link JobStatusStore} during the status-SPI decomposition. Terminal
 * transitions live on {@link JobTerminalStore}; retry scheduling on {@link JobRetryStore}; pause
 * semantics on {@link JobPauseStore}.
 */
@Incubating
public interface JobBatchStatusStore {

  void updateJobStatus(long id, JobStatus status, String errorMessage);

  /** Atomic compare-and-swap status transition. */
  boolean compareAndSwapStatus(long id, JobStatus expected, JobStatus newStatus, String error);

  boolean tryPickUpJob(long id, String nodeId);

  boolean resetRunningJob(long id, String nodeId);

  int resetRunningJobs(String nodeId);

  int cancelRecurringJobsByTag(String tag);

  int cancelRecurringJobByBusinessKey(String businessKey);

  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);
}

package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.Set;

/**
 * Status transition and update operations for jobs.
 *
 * <p>These are raw persistence operations without business-level validation. State machine
 * validation belongs in the RI module.
 */
@Incubating
public interface JobStatusStore {

  void updateJobStatus(long id, JobStatus status, String errorMessage);

  /** Atomic compare-and-swap status transition. */
  boolean compareAndSwapStatus(long id, JobStatus expected, JobStatus newStatus, String error);

  int incrementRetryAttempt(long id);

  boolean tryPickUpJob(long id, String nodeId);

  boolean markJobSucceeded(
      long id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs);

  boolean markJobSucceededMinimal(
      long id, Instant start, Instant end, Long durationMs, Long queueWaitMs);

  /** Marks a batch child as succeeded and advances the parent batch counters atomically. */
  boolean markJobSucceededAndUpdateBatch(
      long jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      long batchId);

  boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts);

  boolean resetRunningJob(long id, String nodeId);

  int resetRunningJobs(String nodeId);

  int cancelRecurringJobsByTag(String tag);

  int cancelRecurringJobByBusinessKey(String businessKey);

  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);

  /**
   * Atomically resets FAILED to PENDING including retry metadata in one operation to avoid TOCTOU
   * gaps.
   */
  boolean resetFailedToPending(long id);

  /**
   * Atomically transitions to PAUSED, recording the original status for later resume in the same
   * operation to avoid TOCTOU gaps.
   */
  boolean transitionToPaused(long id, JobStatus expected);

  /**
   * Atomically transitions from PAUSED to the target status, clearing the stored paused-from
   * status.
   */
  boolean transitionFromPaused(long id, JobStatus target);

  /**
   * Atomically transitions from PAUSED to the stored paused-from status, reading the target from
   * the database row in the same operation to avoid TOCTOU races.
   */
  JobStatus transitionFromPausedAtomic(long id);
}

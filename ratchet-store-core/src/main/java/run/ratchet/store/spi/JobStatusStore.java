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

  /** Updates a job's status and optional error message without additional business validation. */
  void updateJobStatus(long id, JobStatus status, String errorMessage);

  /** Performs an atomic compare-and-swap status transition. */
  boolean compareAndSwapStatus(long id, JobStatus expected, JobStatus newStatus, String error);

  /** Increments the persisted retry-attempt counter and returns the new value. */
  int incrementRetryAttempt(long id);

  /** Attempts to claim a pending job for a node by transitioning it to {@code RUNNING}. */
  boolean tryPickUpJob(long id, String nodeId);

  /** Marks a running job as succeeded and persists result and timing metadata. */
  boolean markJobSucceeded(
      long id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs);

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

  /** Resets a failed or running job into a future pending retry state. */
  boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts);

  /** Resets one running job owned by the supplied node back to {@code PENDING}. */
  boolean resetRunningJob(long id, String nodeId);

  /** Resets all running jobs owned by the supplied node. */
  int resetRunningJobs(String nodeId);

  /** Cancels active recurring jobs carrying the supplied tag. */
  int cancelRecurringJobsByTag(String tag);

  /** Cancels the active recurring job identified by the supplied business key. */
  int cancelRecurringJobByBusinessKey(String businessKey);

  /** Cancels recurring annotation jobs that were not re-registered during startup. */
  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);

  /**
   * Atomically transitions a FAILED job to PENDING, resetting retry metadata in a single operation
   * to avoid the TOCTOU gap where a job could be claimed between a CAS and a subsequent save.
   *
   * @return true if the job was in FAILED state
   */
  boolean resetFailedToPending(long id);

  /**
   * Atomically transitions a job from the expected status to PAUSED, recording the original status
   * in {@code paused_from_status}. This avoids a TOCTOU gap between the status CAS and storing the
   * previous status for resume.
   *
   * @param expected the expected current status (typically PENDING or FAILED)
   * @return true if the transition succeeded
   */
  boolean transitionToPaused(long id, JobStatus expected);

  /**
   * Atomically transitions a job from PAUSED to the target status, clearing {@code
   * paused_from_status}. The target status should be determined from a prior read of the job's
   * {@code paused_from_status} field.
   *
   * @param target the status to resume to (typically PENDING or FAILED)
   * @return true if the job was in PAUSED state
   */
  boolean transitionFromPaused(long id, JobStatus target);

  /**
   * Atomically transitions a job from PAUSED to its stored {@code paused_from_status} (defaulting
   * to PENDING if null), reading the target status from the database row in the same atomic
   * operation. This avoids the TOCTOU race of reading {@code paused_from_status} from an in-memory
   * entity snapshot and then passing it as a parameter.
   *
   * @return the status the job was resumed to, or {@code null} if the job was not in PAUSED state
   */
  JobStatus transitionFromPausedAtomic(long id);
}

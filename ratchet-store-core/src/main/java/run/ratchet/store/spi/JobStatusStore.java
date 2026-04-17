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

  /**
   * Atomically transitions a RUNNING job to terminal FAILED state. Captures total attempts and
   * terminal error in a single store call. Replaces the legacy {@code setStatus(FAILED)+save}
   * pattern that is incompatible with the hot/cold split (hot DELETE + cold UPDATE +
   * bkres DELETE in one tx).
   */
  boolean markJobFailedTerminal(long id, String terminalError, int totalAttempts);

  /**
   * Cancels a job by id. Dispatches by job_type internally: executable jobs DELETE the live queue
   * row + UPDATE cold to terminal CANCELED; recurring masters clear the recurring shim and set
   * cold terminal CANCELED. Single-table store implementations may treat this as an UPDATE to
   * CANCELED. Returns true iff the job transitioned to CANCELED.
   */
  boolean cancelJob(long id);

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

  /**
   * Pauses a recurring master. Post hot/cold-split, recurring masters live in cold with the
   * rec_status shim ('P' PENDING, 'A' PAUSED) and have no hot row. Single-table store
   * implementations may treat this as a status flip on the live row. Returns true iff the master
   * transitioned from PENDING to PAUSED.
   */
  boolean pauseRecurring(long id);

  /**
   * Resumes a recurring master. Returns true iff the master transitioned from PAUSED to PENDING.
   */
  boolean resumeRecurring(long id);
}

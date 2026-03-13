package run.ratchet.store.spi;

import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.Set;

/**
 * Status transition and update operations for jobs.
 *
 * <p>These are raw persistence operations without business-level validation. State machine
 * validation belongs in the RI module.
 */
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
}

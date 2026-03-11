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

  void updateJobStatus(long id, JobStatus status, String errorMessage);

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

  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);
}

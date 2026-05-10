package run.ratchet.store.spi;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/** Core CRUD operations and query methods for jobs. */
@Incubating
public interface JobCrudStore {

  int DEFAULT_PAGE_LIMIT = 100;

  /** Inserts a new job row and returns the persisted entity view. */
  JobEntity create(JobEntity job);

  /** Updates an existing job row and returns the persisted entity view. */
  JobEntity save(JobEntity job);

  Optional<JobEntity> findById(UUID id);

  /**
   * Loads the latest job row by primary key. Despite the method name, no row-level lock is acquired
   * — backends rely on optimistic version checks at the actual mutation site ({@code
   * findOneAndUpdate} on Mongo, {@code WHERE version = ?} on SQL). Callers MUST use a
   * version-checked update path; this method is read-only.
   */
  Optional<JobEntity> findByIdLatest(UUID id);

  void delete(UUID id);

  /**
   * Returns the current persisted status for a job.
   *
   * @param id job id to inspect
   * @return current status, or {@code null} when no job exists for {@code id}
   */
  JobStatus getJobStatus(UUID id);

  /** Batch-loads jobs by primary key for hot-path recovery and draining flows. */
  List<JobEntity> findByIds(List<UUID> ids);

  /** Finds the active job currently associated with a business key, if any. */
  Optional<JobEntity> findActiveByBusinessKey(String businessKey);

  Optional<JobEntity> findByIdempotencyKey(String idempotencyKey);

  /**
   * Returns the first page of direct dependant jobs whose {@code dependsOn} points at the supplied
   * parent.
   *
   * @deprecated use {@link #findDependants(UUID, int, int)} when callers need to walk more than the
   *     default page.
   */
  @Deprecated(since = "0.1.0", forRemoval = false)
  default List<JobEntity> findDependants(UUID parentJobId) {
    return findDependants(parentJobId, DEFAULT_PAGE_LIMIT, 0);
  }

  /**
   * Returns a page of direct dependant jobs whose {@code dependsOn} points at the supplied parent.
   */
  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset);

  /** Returns the next fire time of the earliest pending recurring master job. */
  Optional<Instant> findEarliestRecurringNextFire();

  long countPendingJobs();

  long countJobsByStatus(JobStatus status);

  /** Counts active jobs of the supplied type. */
  long countActiveJobs(JobExecutionType jobType);

  /** Counts currently registered scheduler nodes. */
  long countActiveNodes();

  /** Counts jobs ready to execute at or before the supplied instant. */
  long countReadyJobs(Instant now);

  /** Counts running jobs whose pickup timestamp is older than the supplied threshold. */
  long countStuckJobs(Instant stuckThreshold);

  /** Counts running jobs whose execution start time is older than the supplied threshold. */
  long countLongRunningJobs(Instant threshold);

  long countPendingBatchChildren();

  /** Counts pending jobs at the supplied priority. */
  long countPendingJobsByPriority(JobPriority priority);

  /** Counts pending jobs grouped by priority. */
  default Map<JobPriority, Long> countPendingJobsByPriorities() {
    Map<JobPriority, Long> counts = new EnumMap<>(JobPriority.class);
    for (JobPriority priority : JobPriority.values()) {
      long count = countPendingJobsByPriority(priority);
      if (count > 0) {
        counts.put(priority, count);
      }
    }
    return counts;
  }

  /** Counts pending jobs of the supplied type. */
  long countPendingJobsByType(JobExecutionType jobType);

  /** Counts pending jobs grouped by internal execution type. */
  default Map<JobExecutionType, Long> countPendingJobsByTypes() {
    Map<JobExecutionType, Long> counts = new EnumMap<>(JobExecutionType.class);
    for (JobExecutionType jobType : JobExecutionType.values()) {
      long count = countPendingJobsByType(jobType);
      if (count > 0) {
        counts.put(jobType, count);
      }
    }
    return counts;
  }

  /** Counts jobs in a status whose last update was at or after the supplied instant. */
  long countJobsByStatusSince(JobStatus status, Instant since);

  /** Counts jobs that have recorded at least one retry attempt. */
  long countJobsWithRetries();

  /** Returns the fraction of recently updated jobs that have retried at least once. */
  double getRetryRateStats(Instant since);

  /** Returns average execution duration for jobs included in the store's metric definition. */
  double getAverageProcessingTime(Instant since);

  /** Returns the average number of child items for batches updated since the cutoff. */
  double getAverageBatchSize(Instant since);

  /** Returns the scheduled time of the oldest pending job. */
  Optional<Instant> getOldestPendingJobTime();

  /**
   * Returns the queue wait time at the given percentile for succeeded jobs.
   *
   * @param percentile a fraction in the range [0.0, 1.0], e.g. 0.95 for p95
   * @return queue wait time in milliseconds at the requested percentile, or 0 if no data
   */
  long getQueueWaitTimePercentile(double percentile);
}

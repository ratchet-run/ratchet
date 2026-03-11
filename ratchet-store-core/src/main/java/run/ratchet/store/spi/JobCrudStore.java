package run.ratchet.store.spi;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Core CRUD operations and query methods for jobs. */
public interface JobCrudStore {

  JobEntity save(JobEntity job);

  Optional<JobEntity> findById(long id);

  Optional<JobEntity> findByIdForUpdate(long id);

  void delete(long id);

  JobStatus getJobStatus(long id);

  Optional<JobEntity> findActiveByBusinessKey(String businessKey);

  Optional<JobEntity> findByIdempotencyKey(String idempotencyKey);

  List<JobEntity> findDependants(long parentJobId);

  List<JobEntity> findExistingRecurringJobsByTag(String tag);

  Optional<Instant> findEarliestRecurringNextFire();

  long countPendingJobs();

  long countJobsByStatus(JobStatus status);

  long countActiveJobs(JobType jobType);

  long countActiveNodes();

  long countReadyJobs(Instant now);

  long countStuckJobs(Instant stuckThreshold);

  long countLongRunningJobs(Instant threshold);

  long countPendingBatchChildren();

  long countPendingJobsByPriority(JobPriority priority);

  long countPendingJobsByType(JobType jobType);

  long countJobsByStatusSince(JobStatus status, Instant since);

  long countJobsWithRetries();

  double getRetryRateStats(Instant since);

  double getAverageProcessingTime(Instant since);

  double getAverageBatchSize(Instant since);

  Optional<Instant> getOldestPendingJobTime();

  long getQueueWaitTimePercentile(double percentile);
}

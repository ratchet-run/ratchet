package run.ratchet.store.mysql;

import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

final class MysqlJobCrudOperations implements JobCrudStore, JobBulkStore {

  private final MysqlJobReadOperations reads;
  private final MysqlJobCountOperations counts;
  private final MysqlJobDeleteOperations deletes;
  private final MysqlJobWriteOperations writes;

  MysqlJobCrudOperations(
      MysqlStoreContext ctx,
      MysqlJobRowMapper mapper,
      MysqlBusinessKeyReservations reservations,
      MysqlTagOperations tags) {
    this.reads = new MysqlJobReadOperations(ctx, mapper, tags);
    this.counts = new MysqlJobCountOperations(ctx);
    this.deletes = new MysqlJobDeleteOperations(ctx, reservations);
    this.writes = new MysqlJobWriteOperations(ctx, mapper, reservations, tags);
  }

  @Override
  public JobEntity create(JobEntity job) {
    writes.saveInsert(job);
    return job;
  }

  @Override
  public JobEntity save(JobEntity job) {
    return writes.save(job);
  }

  @Override
  public Optional<JobEntity> findById(UUID id) {
    return reads.findById(id);
  }

  @Override
  public Optional<JobEntity> findByIdLatest(UUID id) {
    return reads.findByIdLatest(id);
  }

  @Override
  public void delete(UUID id) {
    deletes.delete(id);
  }

  @Override
  public JobStatus getJobStatus(UUID id) {
    return reads.getJobStatus(id);
  }

  @Override
  public List<JobEntity> findByIds(List<UUID> ids) {
    return reads.findByIds(ids);
  }

  @Override
  public Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    return reads.findActiveByBusinessKey(businessKey);
  }

  @Override
  public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    return reads.findByIdempotencyKey(idempotencyKey);
  }

  @Override
  public List<JobEntity> findDependants(UUID parentJobId) {
    return reads.findDependants(parentJobId);
  }

  @Override
  public Optional<Instant> findEarliestRecurringNextFire() {
    return reads.findEarliestRecurringNextFire();
  }

  @Override
  public long countPendingJobs() {
    return counts.countPendingJobs();
  }

  @Override
  public long countJobsByStatus(JobStatus status) {
    return counts.countJobsByStatus(status);
  }

  @Override
  public long countActiveJobs(JobExecutionType jobType) {
    return counts.countActiveJobs(jobType);
  }

  @Override
  public long countActiveNodes() {
    return counts.countActiveNodes();
  }

  @Override
  public long countReadyJobs(Instant now) {
    return counts.countReadyJobs(now);
  }

  @Override
  public long countStuckJobs(Instant stuckThreshold) {
    return counts.countStuckJobs(stuckThreshold);
  }

  @Override
  public long countLongRunningJobs(Instant threshold) {
    return counts.countLongRunningJobs(threshold);
  }

  @Override
  public long countPendingBatchChildren() {
    return counts.countPendingBatchChildren();
  }

  @Override
  public long countPendingJobsByPriority(JobPriority priority) {
    return counts.countPendingJobsByPriority(priority);
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return counts.countPendingJobsByType(jobType);
  }

  @Override
  public long countJobsByStatusSince(JobStatus status, Instant since) {
    return counts.countJobsByStatusSince(status, since);
  }

  @Override
  public long countJobsWithRetries() {
    return counts.countJobsWithRetries();
  }

  @Override
  public double getRetryRateStats(Instant since) {
    return counts.getRetryRateStats(since);
  }

  @Override
  public double getAverageProcessingTime(Instant since) {
    return counts.getAverageProcessingTime(since);
  }

  @Override
  public double getAverageBatchSize(Instant since) {
    return counts.getAverageBatchSize(since);
  }

  @Override
  public Optional<Instant> getOldestPendingJobTime() {
    return counts.getOldestPendingJobTime();
  }

  @Override
  public long getQueueWaitTimePercentile(double percentile) {
    return counts.getQueueWaitTimePercentile(percentile);
  }

  @Override
  public void bulkInsert(List<JobEntity> jobs) {
    writes.bulkInsert(jobs);
  }

  @Override
  public int deleteJobsByIds(List<UUID> ids) {
    return deletes.deleteJobsByIds(ids);
  }

  @Override
  public int deleteDlqOlderThan(Instant cutoff) {
    return deletes.deleteDlqOlderThan(cutoff);
  }

  @Override
  public int resetOrphanJobs(Duration grace) {
    return deletes.resetOrphanJobs(grace);
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    return deletes.resetOrphanJobsForNode(nodeId);
  }
}

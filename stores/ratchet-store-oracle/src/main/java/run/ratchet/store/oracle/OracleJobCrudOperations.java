/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.oracle;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;

final class OracleJobCrudOperations implements JobCrudStore, JobBulkStore, JobAnalyticsStore {

  private final OracleJobReadOperations reads;
  private final OracleJobCountOperations counts;
  private final OracleJobDeleteOperations deletes;
  private final OracleJobWriteOperations writes;
  private final OracleTagOperations tags;

  OracleJobCrudOperations(
      OracleJobReadOperations reads,
      OracleJobCountOperations counts,
      OracleJobDeleteOperations deletes,
      OracleJobWriteOperations writes,
      OracleTagOperations tags) {
    this.reads = reads;
    this.counts = counts;
    this.deletes = deletes;
    this.writes = writes;
    this.tags = tags;
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
  public List<JobEntity> findDependants(UUID parentJobId, int limit, int offset) {
    return reads.findDependants(parentJobId, limit, offset);
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
  public Map<JobStatus, Long> countJobsByStatuses() {
    return counts.countJobsByStatuses();
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
  public Map<JobPriority, Long> countPendingJobsByPriorities() {
    return counts.countPendingJobsByPriorities();
  }

  @Override
  public long countPendingJobsByType(JobExecutionType jobType) {
    return counts.countPendingJobsByType(jobType);
  }

  @Override
  public Map<JobExecutionType, Long> countPendingJobsByTypes() {
    return counts.countPendingJobsByTypes();
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

  int deleteTerminalJobsByIds(List<UUID> ids) {
    return deletes.deleteTerminalJobsByIds(ids);
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
  public int resetOrphanJobsBefore(Instant cutoff) {
    return deletes.resetOrphanJobsBefore(cutoff);
  }

  @Override
  public int resetOrphanJobsForNode(String nodeId) {
    return deletes.resetOrphanJobsForNode(nodeId);
  }

  @Override
  public Map<JobStatus, Long> countJobsByStatusForTag(String tag) {
    return tags.countJobsByStatusForTag(tag);
  }

  @Override
  public Map<String, Long> countJobsByParamForTag(String tag, String paramKey) {
    return tags.countJobsByParamForTag(tag, paramKey);
  }

  @Override
  public Map<String, Long> countJobsByExecutionNodeForTag(String tag) {
    return tags.countJobsByExecutionNodeForTag(tag);
  }
}

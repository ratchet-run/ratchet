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
import run.ratchet.api.Nullable;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

/** Core CRUD operations and query methods for jobs. */
@Incubating
public interface JobCrudStore {

  int DEFAULT_PAGE_LIMIT = 100;

  /**
   * Inserts a new job row and returns the persisted entity view.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   */
  JobEntity create(JobEntity job);

  /**
   * Updates an existing job row and returns the persisted entity view.
   *
   * <p>Transaction attribute: {@code REQUIRED}.
   */
  JobEntity save(JobEntity job);

  /** Finds a job by primary key. Transaction attribute: {@code SUPPORTS}. */
  Optional<JobEntity> findById(UUID id);

  /**
   * Loads the latest job row by primary key. Despite the method name, no row-level lock is acquired
   * — backends rely on optimistic version checks at the actual mutation site ({@code
   * findOneAndUpdate} on Mongo, {@code WHERE version = ?} on SQL). Callers MUST use a
   * version-checked update path; this method is read-only.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  Optional<JobEntity> findByIdLatest(UUID id);

  /** Deletes a job row by primary key. Transaction attribute: {@code REQUIRED}. */
  void delete(UUID id);

  /**
   * Returns the current persisted status for a job.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @apiNote This method intentionally returns a bare reference rather than {@link Optional} so the
   *     hot status-check path avoids one allocation per call. Callers must null-check the result;
   *     the {@link Nullable} annotation reflects this contract for static analysers.
   * @param id job id to inspect
   * @return current status, or {@code null} when no job exists for {@code id}
   */
  @Nullable JobStatus getJobStatus(UUID id);

  /**
   * Batch-loads jobs by primary key for hot-path recovery and draining flows.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  List<JobEntity> findByIds(List<UUID> ids);

  /**
   * Finds the active job currently associated with a business key, if any.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  Optional<JobEntity> findActiveByBusinessKey(String businessKey);

  /** Finds a job by idempotency key. Transaction attribute: {@code SUPPORTS}. */
  Optional<JobEntity> findByIdempotencyKey(String idempotencyKey);

  /**
   * Returns a page of direct dependant jobs whose {@code dependsOn} points at the supplied parent.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset);

  /** Counts pending jobs. Transaction attribute: {@code SUPPORTS}. */
  long countPendingJobs();

  /** Counts jobs at the supplied status. Transaction attribute: {@code SUPPORTS}. */
  long countJobsByStatus(JobStatus status);

  /**
   * Counts jobs grouped by status.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * <p><b>Performance note:</b> the default implementation issues one {@link
   * #countJobsByStatus(JobStatus)} call per {@link JobStatus} constant (currently 8+). Production
   * store implementations MUST override this with a single grouped query (e.g. {@code SELECT
   * status, COUNT(*) FROM scheduler_job GROUP BY status}) to avoid N database round-trips on every
   * metrics collection cycle.
   */
  default Map<JobStatus, Long> countJobsByStatuses() {
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    for (JobStatus status : JobStatus.values()) {
      long count = countJobsByStatus(status);
      if (count > 0) {
        counts.put(status, count);
      }
    }
    return counts;
  }

  /** Counts active jobs of the supplied type. Transaction attribute: {@code SUPPORTS}. */
  long countActiveJobs(JobExecutionType jobType);

  /** Counts currently registered scheduler nodes. Transaction attribute: {@code SUPPORTS}. */
  long countActiveNodes();

  /**
   * Counts jobs ready to execute at or before the supplied instant. Transaction attribute: {@code
   * SUPPORTS}.
   */
  long countReadyJobs(Instant now);

  /**
   * Counts running jobs whose pickup timestamp is older than the supplied threshold.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  long countStuckJobs(Instant stuckThreshold);

  /**
   * Counts running jobs whose execution start time is older than the supplied threshold.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  long countLongRunningJobs(Instant threshold);

  /** Counts pending batch-child jobs. Transaction attribute: {@code SUPPORTS}. */
  long countPendingBatchChildren();

  /** Counts pending jobs at the supplied priority. Transaction attribute: {@code SUPPORTS}. */
  long countPendingJobsByPriority(JobPriority priority);

  /**
   * Counts pending jobs grouped by priority.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * <p><b>Performance note:</b> the default implementation issues one {@link
   * #countPendingJobsByPriority(JobPriority)} call per {@link JobPriority} constant. Production
   * store implementations MUST override this with a single grouped query to avoid N database
   * round-trips on every metrics collection cycle.
   */
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

  /** Counts pending jobs of the supplied type. Transaction attribute: {@code SUPPORTS}. */
  long countPendingJobsByType(JobExecutionType jobType);

  /**
   * Counts pending jobs grouped by internal execution type.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * <p><b>Performance note:</b> the default implementation issues one {@link
   * #countPendingJobsByType(JobExecutionType)} call per {@link JobExecutionType} constant.
   * Production store implementations MUST override this with a single grouped query to avoid N
   * database round-trips on every metrics collection cycle.
   */
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

  /**
   * Counts jobs in a status whose last update was at or after the supplied instant.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  long countJobsByStatusSince(JobStatus status, Instant since);

  /**
   * Counts jobs that have recorded at least one retry attempt. Transaction attribute: {@code
   * SUPPORTS}.
   */
  long countJobsWithRetries();

  /**
   * Returns the fraction of recently updated jobs that have retried at least once.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  double getRetryRateStats(Instant since);

  /**
   * Returns average execution duration for jobs included in the store's metric definition.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  double getAverageProcessingTime(Instant since);

  /**
   * Returns the average number of child items for batches updated since the cutoff.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  double getAverageBatchSize(Instant since);

  /**
   * Returns the scheduled time of the oldest pending job. Transaction attribute: {@code SUPPORTS}.
   */
  Optional<Instant> getOldestPendingJobTime();

  /**
   * Returns the queue wait time at the given percentile for succeeded jobs.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @param percentile a fraction in the range [0.0, 1.0], e.g. 0.95 for p95
   * @return queue wait time in milliseconds at the requested percentile, or 0 if no data
   */
  long getQueueWaitTimePercentile(double percentile);
}

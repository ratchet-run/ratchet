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
import java.util.Map;
import java.util.Optional;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Aggregate counts, rate statistics, and percentile metrics over the job tables.
 *
 * <p>These are dashboard- and observability-oriented reads. They are not consulted by the
 * scheduling, claim, or execution hot paths — the basic backpressure counters a running engine
 * needs ({@code countPendingJobs}, {@code countActiveNodes}) stay on the core {@link JobCrudStore}.
 * A store may legitimately omit this capability; the engine disables aggregate reporting rather
 * than assuming it is present.
 */
@Incubating
public interface JobAnalyticsStore {

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
   * <p>The value is computed with <b>discrete nearest-rank</b> semantics (equivalent to SQL {@code
   * PERCENTILE_DISC}): the result is always an actually-observed {@code queueWaitMs} value, never
   * an interpolation between two observations. All stores return the same value for the same data.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   *
   * @param percentile a fraction in the range [0.0, 1.0], e.g. 0.95 for p95
   * @return queue wait time in milliseconds at the requested percentile, or 0 if no data
   * @throws IllegalArgumentException if {@code percentile} is {@code NaN} or outside [0.0, 1.0].
   *     All stores reject an out-of-range percentile rather than clamping or passing it to the
   *     backend.
   */
  long getQueueWaitTimePercentile(double percentile);

  /**
   * Counts jobs for one tag by status. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>This is a low-cardinality aggregation; implementations should group in the store.
   *
   * @param tag tag name to count against; never {@code null} or blank
   * @return per-status counts (omitted statuses have zero matches); never {@code null}
   */
  Map<JobStatus, Long> countJobsByStatusForTag(String tag);

  /**
   * Counts jobs for one tag by parameter value. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>Callers should use this for bounded diagnostic cardinalities, not arbitrary high-cardinality
   * payload fields.
   *
   * @param tag tag name to count against; never {@code null} or blank
   * @param paramKey job-parameter key whose distinct values become the result map keys; never
   *     {@code null} or blank
   * @return distinct-value counts keyed by parameter value (null parameter values map to {@code
   *     null}); never {@code null}
   */
  Map<String, Long> countJobsByParamForTag(String tag, String paramKey);

  /**
   * Counts jobs for one tag by execution node. Transaction attribute: {@code SUPPORTS}.
   *
   * <p>This is bounded by scheduler-node cardinality.
   *
   * @param tag tag name to count against; never {@code null} or blank
   * @return per-node counts keyed by node id; never {@code null}
   */
  Map<String, Long> countJobsByExecutionNodeForTag(String tag);
}

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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobEntity;

/**
 * Bulk operations for jobs.
 *
 * <p><b>SPI contract:</b> Implementations must clear the JPA persistence context ({@code
 * EntityManager.clear()}) after native JDBC bulk write operations to prevent stale entity state.
 */
@Incubating
public interface JobBulkStore {

  /**
   * Inserts jobs in bulk. Transaction attribute: {@code REQUIRED}.
   *
   * <p>As with {@link JobCrudStore#create}, a job created in a terminal state does not reserve its
   * business key. A store whose writes wait on uncommitted keys must write the business-key
   * reservations before the idempotency keys and job rows, as create does. Otherwise a bulk insert
   * and a create that share both keys can deadlock.
   */
  void bulkInsert(List<JobEntity> jobs);

  /** Deletes jobs by id in bulk. Transaction attribute: {@code REQUIRED}. */
  int deleteJobsByIds(List<UUID> ids);

  /** Deletes old DLQ rows. Transaction attribute: {@code REQUIRED}. */
  int deleteDlqOlderThan(Instant cutoff);

  /** Reads completion metadata without decoding payloads. Transaction attribute: REQUIRED. */
  Optional<JobEntity> findOrphanCompletionSnapshot(UUID jobId);

  /** Resets orphans with budget and returns exhausted claims in one REQUIRED transaction. */
  OrphanRecovery resetOrphanJobs(Duration grace, int maxCrashRedeliveries, int exhaustedLimit);

  /** Uses the same exact cutoff for the reset and exhausted selection. */
  OrphanRecovery resetOrphanJobsBefore(
      Instant cutoff, int maxCrashRedeliveries, int exhaustedLimit);

  /**
   * Recovers this node's previous claims at startup. Exhausted claims retain RUNNING, picked_at and
   * claim_seq; their owner is cleared so a later orphan scan can fail them.
   */
  OrphanRecovery resetOrphanJobsForNode(
      String nodeId, int maxCrashRedeliveries, int exhaustedLimit);

  /**
   * Closes up to {@code limit} RUNNING execution rows started before {@code cutoff} whose job is
   * not RUNNING and has not changed since {@code cutoff}, or no longer exists.
   *
   * <p>Sets status to FAILED, ended_at to store now, and error_message to {@code
   * JobExecutionEntity.STALE_EXECUTION_ERROR_MESSAGE}; other columns are unchanged. Leaves rows of
   * RUNNING jobs, jobs changed at or after the cutoff, rows started at or after the cutoff, and
   * rows not RUNNING alone. The default does nothing; stores recording executions should override
   * it. Transaction attribute: REQUIRED.
   *
   * @param cutoff exclusive cutoff for execution start and job change times
   * @param limit maximum rows to close; values at or below zero touch nothing
   * @return number of rows closed
   */
  default int closeStaleExecutions(Instant cutoff, int limit) {
    return 0;
  }
}

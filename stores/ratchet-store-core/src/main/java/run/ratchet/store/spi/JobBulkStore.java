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

  /** Inserts jobs in bulk. Transaction attribute: {@code REQUIRED}. */
  void bulkInsert(List<JobEntity> jobs);

  /** Deletes jobs by id in bulk. Transaction attribute: {@code REQUIRED}. */
  int deleteJobsByIds(List<UUID> ids);

  /** Deletes old DLQ rows. Transaction attribute: {@code REQUIRED}. */
  int deleteDlqOlderThan(Instant cutoff);

  /** Reads completion metadata without decoding payloads. Transaction attribute: REQUIRED. */
  Optional<JobEntity> findOrphanCompletionSnapshot(UUID jobId);

  /**
   * Reads up to {@code limit} RUNNING rows whose picked_at is before {@code cutoff} and whose owner
   * has no heartbeat at or after cutoff (including rows without an owner), oldest picked_at first,
   * with no crash-count filter. Rechecks candidates before changing them and closes every confirmed
   * row's RUNNING executions as FAILED with {@code
   * JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE}.
   *
   * <p>Rows below {@code maxCrashRedeliveries} (missing count means zero) become PENDING, with
   * owner and picked_at cleared, crash count incremented and updated_at set to now. Exhausted rows
   * stay RUNNING with the same owner, crash count and claim_seq; picked_at and updated_at are both
   * set to the current JVM time ({@link Instant#now()}). This defers them for one grace period.
   * Every confirmed row leaves the candidate window for this cutoff. Returns (scanned, reset, 0,
   * exhausted), with exhausted claims in original picked_at order for the engine to fail.
   *
   * <p>One transaction per call, never joined to a caller's transaction. A nonpositive limit
   * touches nothing and returns {@link OrphanRecovery#none()}.
   */
  OrphanRecovery resetOrphanJobsBefore(Instant cutoff, int maxCrashRedeliveries, int limit);

  /**
   * Reads up to {@code limit} RUNNING rows owned by {@code nodeId}, oldest picked_at first, with no
   * time cutoff. Rechecks candidates and closes their RUNNING execution rows as FAILED with {@code
   * JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE}. Resets rows below the maximum to PENDING,
   * clears owner and picked_at, increments crash count (missing means zero), and sets updated_at to
   * now. For rows at or above the maximum, clears only the owner, preserving status, picked_at and
   * claim_seq so a later periodic page can fail them.
   *
   * <p>Returns (scanned, reset, released, empty exhausted list). One transaction per call, never
   * joined to a caller's transaction. A nonpositive limit touches nothing and returns {@link
   * OrphanRecovery#none()}.
   */
  OrphanRecovery resetOrphanJobsForNode(String nodeId, int maxCrashRedeliveries, int limit);

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

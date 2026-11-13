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
   * Reads up to {@code limit} RUNNING candidates with picked_at before {@code cutoff}, no owner
   * heartbeat at or after cutoff (including no owner), and crash count below {@code
   * maxCrashRedeliveries}; a missing count is zero. No ordering is promised.
   *
   * <p>Rechecks each candidate before changing it. Closes its RUNNING execution rows as FAILED with
   * {@code JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE}, resets it to PENDING, clears owner
   * and picked_at, increments crash count, and sets updated_at to now. Rows at or above the maximum
   * are never read or changed. Returns (scanned, reset, 0). One REQUIRED transaction per call. A
   * nonpositive limit touches nothing and returns (0, 0, 0).
   */
  OrphanRecovery resetOrphanJobsBefore(Instant cutoff, int maxCrashRedeliveries, int limit);

  /**
   * Reads up to {@code limit} RUNNING rows owned by {@code nodeId}, with no time cutoff. Rechecks
   * candidates and closes their RUNNING execution rows as FAILED with {@code
   * JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE}. Resets rows below the maximum to PENDING,
   * clears owner and picked_at, increments crash count (missing means zero), and sets updated_at to
   * now. For rows at or above the maximum, clears only the owner, preserving RUNNING, picked_at and
   * claim_seq so a later periodic scan can fail them.
   *
   * <p>Returns (scanned, reset, released). One REQUIRED transaction per call. A nonpositive limit
   * touches nothing and returns (0, 0, 0).
   */
  OrphanRecovery resetOrphanJobsForNode(String nodeId, int maxCrashRedeliveries, int limit);

  /**
   * Reads up to {@code limit} RUNNING rows with picked_at before {@code cutoff}, no owner heartbeat
   * at or after cutoff (including no owner), and crash count at or above {@code
   * maxCrashRedeliveries}; a missing count is zero. Reads oldest picked_at first.
   *
   * <p>Rechecks each candidate, closes its RUNNING execution rows as FAILED with {@code
   * JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE}, and leaves the job RUNNING. Returns one
   * {@link ExhaustedOrphan} with jobId, claimSeq, crashCount and pickedBy per confirmed row, oldest
   * picked_at first. One REQUIRED transaction per call. A nonpositive limit touches nothing and
   * returns an empty list.
   */
  List<ExhaustedOrphan> recoverExhaustedOrphans(
      Instant cutoff, int maxCrashRedeliveries, int limit);

  /**
   * If the row is RUNNING with {@code claimSeq}, sets picked_at and updated_at to the current JVM
   * time ({@link Instant#now()}, bound as a parameter) and returns true. Uses the same time source
   * as batch claims and engine cutoff computation. Otherwise changes nothing and returns false.
   * Moves an exhausted orphan the engine could not fail behind the others. One REQUIRED transaction
   * per call.
   */
  boolean deferExhaustedOrphan(UUID jobId, long claimSeq);

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

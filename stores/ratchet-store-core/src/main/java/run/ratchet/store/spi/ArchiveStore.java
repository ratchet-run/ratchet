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
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

/** Job archiving operations for completed/failed job history. */
@Incubating
public interface ArchiveStore {

  /**
   * Archives one terminal job in the caller's store transaction.
   *
   * <p>Transaction attribute: {@code REQUIRED}. Implementations must insert the archive row in the
   * same transaction as any caller-managed active-job cleanup.
   *
   * @param job terminal job to archive; never {@code null}
   * @param reason free-form audit reason recorded on the archive row; never {@code null}
   * @param archivedBy identifier of the actor or component that triggered the archive (node id,
   *     "admin", etc.); never {@code null}
   * @return persisted archive entity; never {@code null}
   */
  ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy);

  /**
   * Atomically moves a batch of terminal jobs from active storage to archive storage.
   *
   * <p>Transaction attribute: {@code REQUIRED}. Implementations must re-read the supplied job IDs
   * inside the store transaction, copy every current row and its extension data to archive storage,
   * then delete the active rows and any active-store business-key reservations and extension data
   * in that same transaction. Every active-row delete must be guarded by terminal status. If any
   * job is missing, no longer terminal, or cannot be deleted, the whole move must roll back: no
   * archive row, active-row delete, or related-data cleanup may remain for any member of the batch.
   *
   * <p>A non-empty call is all-or-nothing: it either returns {@code jobs.size()} or throws. After a
   * failed call, callers may safely retry with a freshly loaded batch.
   *
   * @param jobs distinct terminal jobs to move; never {@code null}, may be empty (no-op when empty)
   * @param reason free-form audit reason recorded on each archive row; never {@code null}
   * @param archivedBy identifier of the actor or component that triggered the archive; never {@code
   *     null}
   * @return the number of jobs moved, equal to {@code jobs.size()}
   */
  int archiveAndDeleteJobsBatch(List<JobEntity> jobs, String reason, String archivedBy);

  /**
   * Finds active terminal jobs old enough to archive. Transaction attribute: {@code SUPPORTS}.
   *
   * @param olderThan jobs whose last-update timestamp is strictly before this instant are
   *     candidates; never {@code null}
   * @param limit maximum number of candidates to return; must be positive
   * @return candidate jobs, never {@code null}
   */
  List<JobEntity> findJobsForArchiving(Instant olderThan, int limit);

  /**
   * Counts active terminal jobs old enough to archive. Transaction attribute: {@code SUPPORTS}.
   *
   * @param olderThan jobs whose last-update timestamp is strictly before this instant are counted;
   *     never {@code null}
   * @return number of candidate rows
   */
  long countJobsForArchiving(Instant olderThan);

  /**
   * Finds archived jobs matching optional filters.
   *
   * <p>{@code null} filter arguments are ignored: {@code targetClass} omits the target-class
   * predicate, {@code businessKey} omits the business-key predicate, {@code from} omits the lower
   * archived-at bound, and {@code to} omits the upper archived-at bound. Results are returned
   * newest first by archive timestamp and capped at {@code limit}.
   *
   * <p>Transaction attribute: {@code SUPPORTS}.
   */
  List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit);

  /**
   * Purges archived rows older than the cutoff. Transaction attribute: {@code REQUIRED}.
   *
   * @param olderThan archived rows whose archive timestamp is strictly before this instant are
   *     deleted; never {@code null}
   * @return number of archive rows deleted
   */
  int purgeArchivedJobs(Instant olderThan);
}

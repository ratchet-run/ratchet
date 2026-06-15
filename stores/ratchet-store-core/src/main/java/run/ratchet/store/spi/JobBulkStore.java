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

  /**
   * Resets orphaned RUNNING jobs to PENDING in one bulk update, or inside one transaction when the
   * backend has no native bulk form. Transaction attribute: {@code REQUIRED}.
   */
  int resetOrphanJobs(Duration grace);

  /**
   * Resets orphaned RUNNING jobs using a caller-supplied cutoff. Implementations should compare
   * node heartbeat and job claim timestamps to this exact instant. Transaction attribute: {@code
   * REQUIRED}.
   *
   * <p>If {@code cutoff} is in the future (e.g. due to clock skew between nodes), the computed
   * grace duration is negative. In that case this method returns {@code 0} immediately — a future
   * cutoff means no jobs are old enough to be orphaned yet.
   *
   * <p><b>Clock source.</b> The default implementation derives the grace duration from {@link
   * Instant#now()} on the caller's JVM, NOT the database server clock that {@link
   * run.ratchet.store.spi.LockStore} mandates for lease-correctness. Orphan detection is inherently
   * approximate (heartbeats are coalesced, recovery rounds are spaced, late lease-expiry produces
   * the same outcome as early reset), so client-side clock use is intentional here. Implementations
   * that need stricter server-clock semantics should override this default and re-derive the cutoff
   * from {@link NodeStore#getDatabaseTime()}.
   *
   * @param cutoff orphan-detection cutoff; jobs whose claim timestamp is strictly before this
   *     instant are eligible for reset. Never {@code null}.
   * @return number of rows reset to PENDING
   */
  default int resetOrphanJobsBefore(Instant cutoff) {
    Duration grace = Duration.between(cutoff, Instant.now());
    if (grace.isNegative()) {
      return 0;
    }
    return resetOrphanJobs(grace);
  }

  /**
   * Reclaims all RUNNING jobs currently owned by {@code nodeId}, unconditionally of heartbeat age,
   * by resetting them to PENDING and clearing {@code picked_by}/{@code picked_at}. Intended for
   * startup self-recovery: a crashing node that restarts within the normal grace window ({@link
   * #resetOrphanJobs(Duration)}) would otherwise leave its own prior RUNNING rows in place until
   * their heartbeat aged out.
   *
   * @param nodeId the node identity whose own prior claims should be released
   * @return number of rows reset to PENDING
   *     <p>Transaction attribute: {@code REQUIRED}. The reset must be one bulk update, or the
   *     backend's closest single-transaction equivalent.
   */
  int resetOrphanJobsForNode(String nodeId);
}

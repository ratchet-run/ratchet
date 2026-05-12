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

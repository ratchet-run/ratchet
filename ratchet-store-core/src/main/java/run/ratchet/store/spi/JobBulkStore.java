package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobEntity;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Bulk operations for jobs.
 *
 * <p><b>SPI contract:</b> Implementations must clear the JPA persistence context ({@code
 * EntityManager.clear()}) after native JDBC bulk write operations to prevent stale entity state.
 */
@Incubating
public interface JobBulkStore {

  void bulkInsert(List<JobEntity> jobs);

  int deleteJobsByIds(List<Long> ids);

  int deleteDlqOlderThan(Instant cutoff);

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
   */
  int resetOrphanJobsForNode(String nodeId);
}

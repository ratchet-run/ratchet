package run.ratchet.store.spi;

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
public interface JobBulkStore {

  /** Inserts a collection of jobs efficiently as a bulk operation. */
  void bulkInsert(List<JobEntity> jobs);

  /** Deletes the specified jobs and returns the number removed. */
  int deleteJobsByIds(List<Long> ids);

  /** Deletes dead-letter-eligible jobs older than the cutoff and returns the number removed. */
  int deleteDlqOlderThan(Instant cutoff);

  /** Resets jobs abandoned by dead nodes after the supplied grace period. */
  int resetOrphanJobs(Duration grace);
}

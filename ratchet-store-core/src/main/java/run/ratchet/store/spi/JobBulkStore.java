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
}

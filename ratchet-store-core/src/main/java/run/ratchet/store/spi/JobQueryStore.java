package run.ratchet.store.spi;

import java.util.List;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobFilter;
import run.ratchet.store.entity.JobEntity;

/**
 * Read-only search and filter operations for dashboard and admin queries.
 *
 * <p>Implementations MUST NOT modify any job state. All methods are read-only.
 *
 * <p>SQL implementations should build parameterized WHERE clauses from the filter fields; no string
 * concatenation of user-supplied values is permitted.
 */
@Incubating
public interface JobQueryStore {

  /**
   * Returns jobs matching the given filter, ordered by the filter's sort field.
   *
   * @param filter filter criteria; null fields are ignored (no constraint)
   * @param limit maximum number of results
   * @param offset zero-based starting position
   * @return matching jobs, possibly empty; never null
   */
  List<JobEntity> searchJobs(JobFilter filter, int limit, int offset);

  /**
   * Returns the total number of jobs matching the given filter, regardless of limit/offset.
   *
   * @param filter filter criteria; null fields are ignored
   * @return count of matching jobs
   */
  long countJobs(JobFilter filter);
}

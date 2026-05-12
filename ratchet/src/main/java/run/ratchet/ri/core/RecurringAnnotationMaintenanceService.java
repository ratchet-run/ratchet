package run.ratchet.ri.core;

import java.time.Instant;
import java.util.Set;

/**
 * Internal maintenance operations for {@code @Recurring} registration cleanup.
 *
 * <p>Implementations run inside the scheduler's normal registration transaction boundary.
 */
public interface RecurringAnnotationMaintenanceService {

  /**
   * Cancels recurring annotation jobs that were previously registered but were not seen during the
   * current startup scan.
   *
   * @implSpec Transaction attribute: REQUIRED. Implementations run inside the registration
   *     transaction that performs the startup annotation cleanup.
   * @param registeredIds active business keys discovered during startup
   * @param nodeStartTime startup timestamp used as a grace cutoff
   * @return the number of canceled orphaned jobs
   */
  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);
}

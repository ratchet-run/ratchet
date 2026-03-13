package run.ratchet.ri.core;

import java.time.Instant;
import java.util.Set;

/** Internal maintenance operations for {@code @Recurring} registration cleanup. */
public interface RecurringAnnotationMaintenanceService {

  /**
   * Cancels recurring annotation jobs that were previously registered but were not seen during the
   * current startup scan.
   *
   * @param registeredIds active business keys discovered during startup
   * @param nodeStartTime startup timestamp used as a grace cutoff
   * @return the number of canceled orphaned jobs
   */
  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime);
}

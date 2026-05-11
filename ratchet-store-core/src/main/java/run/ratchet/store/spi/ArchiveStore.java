package run.ratchet.store.spi;

import java.time.Instant;
import java.util.List;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

/** Job archiving operations for completed/failed job history. */
@Incubating
public interface ArchiveStore {

  ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy);

  int archiveJobsBatch(List<JobEntity> jobs, String reason, String archivedBy);

  List<JobEntity> findJobsForArchiving(Instant olderThan, int limit);

  long countJobsForArchiving(Instant olderThan);

  /**
   * Finds archived jobs matching optional filters.
   *
   * <p>{@code null} filter arguments are ignored: {@code targetClass} omits the target-class
   * predicate, {@code businessKey} omits the business-key predicate, {@code from} omits the lower
   * archived-at bound, and {@code to} omits the upper archived-at bound. Results are returned
   * newest first by archive timestamp and capped at {@code limit}.
   */
  List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit);

  int purgeArchivedJobs(Instant olderThan);
}

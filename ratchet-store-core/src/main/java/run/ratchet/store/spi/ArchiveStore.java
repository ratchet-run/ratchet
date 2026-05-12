package run.ratchet.store.spi;

import java.time.Instant;
import java.util.List;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

/** Job archiving operations for completed/failed job history. */
@Incubating
public interface ArchiveStore {

  /** Archives one terminal job. Transaction attribute: {@code REQUIRED}. */
  ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy);

  /** Archives terminal jobs as one batch. Transaction attribute: {@code REQUIRED}. */
  int archiveJobsBatch(List<JobEntity> jobs, String reason, String archivedBy);

  /** Finds active terminal jobs old enough to archive. Transaction attribute: {@code SUPPORTS}. */
  List<JobEntity> findJobsForArchiving(Instant olderThan, int limit);

  /** Counts active terminal jobs old enough to archive. Transaction attribute: {@code SUPPORTS}. */
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

  /** Purges archived rows older than the cutoff. Transaction attribute: {@code REQUIRED}. */
  int purgeArchivedJobs(Instant olderThan);
}

package run.ratchet.store.spi;

import java.time.Instant;
import java.util.List;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

/** Job archiving operations for completed/failed job history. */
@Incubating
public interface ArchiveStore {

  /**
   * Archives one terminal job in the caller's store transaction.
   *
   * <p>Transaction attribute: {@code REQUIRED}. Implementations must insert the archive row in the
   * same transaction as any caller-managed active-job cleanup.
   *
   * @param job terminal job to archive; never {@code null}
   * @param reason free-form audit reason recorded on the archive row; never {@code null}
   * @param archivedBy identifier of the actor or component that triggered the archive (node id,
   *     "admin", etc.); never {@code null}
   * @return persisted archive entity; never {@code null}
   */
  ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy);

  /**
   * Archives terminal jobs as one batch in the caller's store transaction.
   *
   * <p>Transaction attribute: {@code REQUIRED}. Implementations must insert all archive rows in the
   * same transaction as any caller-managed active-job cleanup.
   *
   * @param jobs terminal jobs to archive; never {@code null}, may be empty (no-op when empty)
   * @param reason free-form audit reason recorded on each archive row; never {@code null}
   * @param archivedBy identifier of the actor or component that triggered the archive; never {@code
   *     null}
   * @return number of archive rows written
   */
  int archiveJobsBatch(List<JobEntity> jobs, String reason, String archivedBy);

  /**
   * Finds active terminal jobs old enough to archive. Transaction attribute: {@code SUPPORTS}.
   *
   * @param olderThan jobs whose last-update timestamp is strictly before this instant are
   *     candidates; never {@code null}
   * @param limit maximum number of candidates to return; must be positive
   * @return candidate jobs, never {@code null}
   */
  List<JobEntity> findJobsForArchiving(Instant olderThan, int limit);

  /**
   * Counts active terminal jobs old enough to archive. Transaction attribute: {@code SUPPORTS}.
   *
   * @param olderThan jobs whose last-update timestamp is strictly before this instant are counted;
   *     never {@code null}
   * @return number of candidate rows
   */
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

  /**
   * Purges archived rows older than the cutoff. Transaction attribute: {@code REQUIRED}.
   *
   * @param olderThan archived rows whose archive timestamp is strictly before this instant are
   *     deleted; never {@code null}
   * @return number of archive rows deleted
   */
  int purgeArchivedJobs(Instant olderThan);
}

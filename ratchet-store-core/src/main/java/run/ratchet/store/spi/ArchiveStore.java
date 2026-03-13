package run.ratchet.store.spi;

import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import java.time.Instant;
import java.util.List;

/** Job archiving operations for completed/failed job history. */
public interface ArchiveStore {

  /** Persists an archive record for a single terminal job and returns the stored snapshot. */
  ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy);

  /** Archives multiple jobs in one store interaction and returns the number of archived rows. */
  int archiveJobsBatch(List<JobEntity> jobs, String reason, String archivedBy);

  /** Finds active jobs old enough to be moved into the archive table. */
  List<JobEntity> findJobsForArchiving(Instant olderThan, int limit);

  /** Counts how many active jobs are currently eligible for archiving. */
  long countJobsForArchiving(Instant olderThan);

  /** Searches archived jobs using optional target-class, business-key, and time-range filters. */
  List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit);

  /** Deletes archived jobs older than the supplied cutoff and returns the number removed. */
  int purgeArchivedJobs(Instant olderThan);
}

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

  List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit);

  int purgeArchivedJobs(Instant olderThan);
}

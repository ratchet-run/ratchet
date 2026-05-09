package run.ratchet.store.util;

import java.time.Instant;
import java.util.Objects;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;

/** Shared utilities for archiving jobs across JPA-based store implementations. */
public final class ArchiveHelper {

  /** JPQL for finding terminal jobs older than a cutoff, used by both JPA store implementations. */
  // language=JPAQL
  public static final String FIND_JOBS_FOR_ARCHIVING_JPQL =
      """
      SELECT DISTINCT j FROM JobEntity j LEFT JOIN FETCH j.tags
      WHERE j.status IN (
        run.ratchet.api.JobStatus.SUCCEEDED,
        run.ratchet.api.JobStatus.FAILED,
        run.ratchet.api.JobStatus.CANCELED)
        AND j.updatedAt < :cutoff
      ORDER BY j.updatedAt ASC
      """;

  private ArchiveHelper() {}

  /** Populates an {@link ArchivedJobEntity} from the given job, reason, and archivedBy fields. */
  public static ArchivedJobEntity buildArchive(JobEntity job, String reason, String archivedBy) {
    Objects.requireNonNull(job, "job");
    ArchivedJobEntity a = new ArchivedJobEntity();
    a.setOriginalJobId(job.getId());
    a.setFinalStatus(job.getStatus());
    a.setJobType(job.getJobType());
    a.setPriority(job.getPriority());
    a.setTotalAttempts(job.getAttempts());
    a.setMaxRetries(job.getMaxRetries());
    a.setBackoffPolicy(job.getBackoffPolicy());
    a.setBackoffParamMs(job.getBackoffParamMs());
    a.setTimeoutSec(job.getTimeoutSec());
    a.setTargetClass(job.getTargetClass());
    a.setMethodName(job.getMethodName());
    a.setBusinessKey(job.getBusinessKey());
    a.setCronExpr(job.getCronExpr());
    a.setZoneId(job.getZoneId());
    a.setOriginalScheduledTime(job.getScheduledTime());
    a.setOriginalCreatedAt(job.getCreatedAt());
    a.setFirstExecutionTime(job.getExecutionStartTime());
    a.setCompletionTime(job.getExecutionEndTime());
    a.setTotalExecutionTimeMs(job.getExecutionDurationMs());
    a.setQueueWaitMs(job.getQueueWaitMs());
    a.setArchivedAt(Instant.now());
    a.setArchivedBy(archivedBy);
    a.setArchiveReason(reason);
    a.setJobResult(job.getJobResult());
    a.setResultType(job.getResultType());
    a.setFinalError(job.getLastError());
    if (job.getPayload() != null) {
      a.setPayloadSummary(job.getPayload().target() + "#" + job.getPayload().method());
    }
    a.setDependedOn(job.getDependsOn());
    a.setSupersededBy(job.getSupersededBy());
    if (job.getTags() != null && !job.getTags().isEmpty()) {
      a.setTags(String.join(",", job.getTags()));
    }
    return a;
  }
}

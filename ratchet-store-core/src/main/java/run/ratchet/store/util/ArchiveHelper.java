package run.ratchet.store.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import java.time.Instant;
import org.jboss.logging.Logger;

/** Shared utilities for archiving jobs across JPA-based store implementations. */
public final class ArchiveHelper {

  /** JPQL for finding terminal jobs older than a cutoff, used by both JPA store implementations. */
  public static final String FIND_JOBS_FOR_ARCHIVING_JPQL =
      "SELECT DISTINCT j FROM JobEntity j LEFT JOIN FETCH j.tags WHERE j.status IN ("
          + "run.ratchet.store.entity.JobStatus.SUCCEEDED, "
          + "run.ratchet.store.entity.JobStatus.FAILED, "
          + "run.ratchet.store.entity.JobStatus.CANCELED) "
          + "AND j.updatedAt < :cutoff "
          + "ORDER BY j.updatedAt ASC";

  private static final Logger log = Logger.getLogger(ArchiveHelper.class);

  private ArchiveHelper() {}

  /** Populates an {@link ArchivedJobEntity} from the given job, reason, and archivedBy fields. */
  public static ArchivedJobEntity buildArchive(JobEntity job, String reason, String archivedBy) {
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

  /**
   * Serializes a job's payload to JSON. Returns "{}" on failure so archival is never blocked by a
   * serialization error.
   */
  public static String payloadToJson(JobEntity job, ObjectMapper mapper) {
    if (job.getPayload() == null) {
      return "{}";
    }
    try {
      return mapper.writeValueAsString(job.getPayload());
    } catch (Exception e) {
      log.warn("Failed to serialize payload", e);
      return "{}";
    }
  }

  /**
   * Serializes a job's params map to JSON. Returns {@code null} when params are absent. Returns
   * {@code null} on failure so archival is never blocked by a serialization error.
   */
  public static String paramsToJson(JobEntity job, ObjectMapper mapper) {
    if (job.getParams() == null) {
      return null;
    }
    try {
      return mapper.writeValueAsString(job.getParams());
    } catch (Exception e) {
      log.warn("Failed to serialize params", e);
      return null;
    }
  }

  /**
   * Serializes a callback payload to JSON. Returns {@code null} when the payload is absent. Returns
   * {@code null} on failure so archival is never blocked by a serialization error.
   */
  public static String callbackPayloadToJson(JobPayload payload, ObjectMapper mapper) {
    if (payload == null) {
      return null;
    }
    try {
      return mapper.writeValueAsString(payload);
    } catch (Exception e) {
      log.warn("Failed to serialize callback payload", e);
      return null;
    }
  }
}

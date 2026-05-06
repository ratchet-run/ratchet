package run.ratchet.store.dto;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobExecutionType;

/**
 * Lightweight DTO for job claiming operations.
 *
 * <p>Contains only the metadata fields needed during the claim phase. Large fields (payload,
 * params, jobResult, lastError) are NOT included to reduce data transfer during polling.
 */
public record JobClaimDto(
    UUID id,
    JobStatus status,
    JobExecutionType jobType,
    JobPriority priority,
    Instant scheduledTime,
    Integer version,
    int timeoutSec,
    String pickedBy,
    Instant pickedAt,
    String businessKey,
    int attempts,
    int maxRetries)
    implements Serializable {

  /** Returns true if id, status, and jobType are all non-null. */
  public boolean isValid() {
    return id != null && status != null && jobType != null;
  }

  public JobType publicJobType() {
    return jobType.toPublicType();
  }

  /** Returns true if attempts &lt; maxRetries. */
  public boolean hasRetriesRemaining() {
    return attempts < maxRetries;
  }
}

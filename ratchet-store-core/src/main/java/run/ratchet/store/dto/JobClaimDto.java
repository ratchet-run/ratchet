package run.ratchet.store.dto;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import java.io.Serializable;
import java.time.Instant;

/**
 * Lightweight DTO for job claiming operations.
 *
 * <p>Contains only the metadata fields needed during the claim phase. Large fields (payload,
 * params, jobResult, lastError) are NOT included to reduce data transfer during polling.
 */
public record JobClaimDto(
    Long id,
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

  /**
   * Checks if this claim represents a valid job ready for execution.
   *
   * @return true if the claim has all required fields populated
   */
  public boolean isValid() {
    return id != null && status != null && jobType != null;
  }

  public JobType publicJobType() {
    return jobType.toPublicType();
  }

  /**
   * Checks if the job has remaining retry attempts available.
   *
   * @return true if the job can be retried
   */
  public boolean hasRetriesRemaining() {
    return attempts < maxRetries;
  }
}

package run.ratchet.store.dto;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobStatus;
import java.io.Serializable;
import java.time.Instant;

/**
 * Lightweight DTO for job claiming operations.
 *
 * <p>Contains only the metadata fields needed during the claim phase. Large fields (payload,
 * params, jobResult, lastError) are NOT included to reduce data transfer during polling.
 *
 * @param id the unique job identifier
 * @param status the current job status (will be RUNNING after claiming)
 * @param jobType the type of job for executor routing
 * @param priority the job priority for ordering
 * @param scheduledTime when the job was scheduled to run
 * @param version the optimistic locking version
 * @param timeoutSec the timeout in seconds for watchdog monitoring
 * @param pickedBy the node ID that claimed this job
 * @param pickedAt when the job was claimed
 * @param businessKey optional business key for idempotency
 * @param attempts the number of execution attempts so far
 * @param maxRetries the maximum number of retry attempts allowed
 */
public record JobClaimDto(
    Long id,
    JobStatus status,
    JobType jobType,
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

  /**
   * Checks if the job has remaining retry attempts available.
   *
   * @return true if the job can be retried
   */
  public boolean hasRetriesRemaining() {
    return attempts < maxRetries;
  }
}

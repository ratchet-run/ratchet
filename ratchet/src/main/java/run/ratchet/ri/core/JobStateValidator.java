package run.ratchet.ri.core;

import run.ratchet.store.entity.JobStatus;
import java.util.Map;
import java.util.Set;

/**
 * Service responsible for validating job state transitions to ensure consistency and prevent
 * invalid state changes within the job scheduler framework. This validator enforces the job
 * lifecycle state machine, ensuring jobs follow predictable and safe transition paths.
 *
 * <p>The JobStateValidator implements a finite state machine that defines:
 *
 * <ul>
 *   <li>Valid transitions between job states
 *   <li>Terminal states that cannot transition further
 *   <li>Idempotent operations (same-state transitions)
 *   <li>Business rules for retries and cancellations
 * </ul>
 *
 * <p>State transition rules:
 *
 * <ul>
 *   <li><b>PENDING:</b> Can transition to RUNNING (execution) or CANCELED (abort)
 *   <li><b>RUNNING:</b> Can transition to SUCCEEDED, FAILED, PENDING (retry), or CANCELED
 *   <li><b>SUCCEEDED:</b> Terminal state - no further transitions allowed
 *   <li><b>FAILED:</b> Terminal state - no further transitions allowed
 *   <li><b>CANCELED:</b> Terminal state - no further transitions allowed
 * </ul>
 *
 * <p>Key design principles:
 *
 * <ul>
 *   <li><b>Idempotency:</b> Same-state transitions are always allowed for safety
 *   <li><b>Fail-safe:</b> Invalid transitions are rejected to maintain data integrity
 *   <li><b>Auditability:</b> All transition attempts can be logged for troubleshooting
 *   <li><b>Thread-safe:</b> Stateless design allows concurrent validation
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * // Before updating job status
 * if (validator.isValidTransition(job.getStatus(), newStatus)) {
 *     job.setStatus(newStatus);
 *     repository.save(job);
 * } else {
 *     throw new IllegalStateException("Invalid transition");
 * }
 * }</pre>
 *
 * @see JobStatus for the complete set of job states
 */
public class JobStateValidator {

  /**
   * Immutable map defining the complete state transition matrix for job lifecycle. Each key
   * represents a source state, and its value is the set of valid target states. Terminal states
   * (SUCCEEDED, FAILED, CANCELED) map to empty sets, preventing further transitions.
   *
   * <p>This design ensures:
   *
   * <ul>
   *   <li>Jobs cannot resurrect from terminal states
   *   <li>Running jobs can be retried by transitioning back to PENDING
   *   <li>Jobs can be canceled from PENDING or RUNNING states
   *   <li>The state machine is deterministic and verifiable
   * </ul>
   */
  private static final Map<JobStatus, Set<JobStatus>> VALID_TRANSITIONS =
      Map.of(
          JobStatus.PENDING,
          Set.of(JobStatus.RUNNING, JobStatus.CANCELED),
          JobStatus.RUNNING,
          Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.PENDING, JobStatus.CANCELED),
          JobStatus.SUCCEEDED,
          Set.of(), // Terminal state - no transitions allowed
          JobStatus.FAILED,
          Set.of(), // Terminal state - no transitions allowed
          JobStatus.CANCELED,
          Set.of() // Terminal state - no transitions allowed
          );

  /**
   * Checks if a job can be canceled based on its current status. Cancellation is only meaningful
   * for jobs that haven't reached a terminal state, allowing users or the system to abort
   * execution.
   *
   * <p>Cancellation rules:
   *
   * <ul>
   *   <li>PENDING jobs can be canceled before execution starts
   *   <li>RUNNING jobs can be canceled to interrupt execution
   *   <li>Terminal states cannot be canceled - the outcome is final
   * </ul>
   *
   * <p>Note: Canceling a RUNNING job may require thread interruption or process termination,
   * depending on the job implementation.
   *
   * @param status the current job status to evaluate
   * @return true if the job can be canceled, false otherwise
   */
  public boolean canCancel(JobStatus status) {
    return status == JobStatus.PENDING || status == JobStatus.RUNNING;
  }

  /**
   * Checks if a job can be retried based on its current status. Retry eligibility is determined by
   * whether the job is in a state that supports transitioning back to PENDING for another execution
   * attempt.
   *
   * <p>Retry logic:
   *
   * <ul>
   *   <li>Only RUNNING jobs can be retried (after a failure occurs)
   *   <li>PENDING jobs don't need retry - they haven't executed yet
   *   <li>Terminal states cannot be retried - they represent final outcomes
   * </ul>
   *
   * @param status the current job status to evaluate
   * @return true if the job can be retried, false otherwise
   */
  public boolean canRetry(JobStatus status) {
    // Only RUNNING jobs can be retried (after they fail)
    return status == JobStatus.RUNNING;
  }

  /**
   * Checks if a job is in a terminal state from which no further transitions are possible. Terminal
   * states represent the final outcome of a job's execution lifecycle and are immutable once
   * reached.
   *
   * <p>Terminal states are:
   *
   * <ul>
   *   <li><b>SUCCEEDED:</b> Job completed successfully
   *   <li><b>FAILED:</b> Job failed permanently after exhausting retries
   *   <li><b>CANCELED:</b> Job was explicitly canceled by user or system
   * </ul>
   *
   * @param status the job status to check (may be null)
   * @return true if the status is terminal, false if non-terminal or null
   */
  public boolean isTerminalState(JobStatus status) {
    return status != null && status.isTerminal();
  }

  /**
   * Checks if a state transition is valid according to the defined transition matrix. This method
   * implements the core validation logic, supporting both state changes and idempotent operations.
   *
   * <p>Validation rules:
   *
   * <ul>
   *   <li>Idempotent transitions (from == to) are always valid
   *   <li>Transitions must be explicitly defined in VALID_TRANSITIONS
   *   <li>Null states are treated as invalid
   *   <li>Terminal states cannot transition to any other state
   * </ul>
   *
   * @param from the current job status (source state)
   * @param to the desired job status (target state)
   * @return true if the transition is valid, false otherwise
   */
  public boolean isValidTransition(JobStatus from, JobStatus to) {
    if (from == to) {
      // Idempotent - same state transitions are always allowed
      return true;
    }

    Set<JobStatus> allowedTransitions = VALID_TRANSITIONS.get(from);
    return allowedTransitions != null && allowedTransitions.contains(to);
  }

  /**
   * Validates a state transition and throws an exception if invalid, providing detailed error
   * information for debugging and audit purposes. This method is designed for use in transactional
   * contexts where invalid transitions should abort the operation.
   *
   * <p>Error reporting includes:
   *
   * <ul>
   *   <li>Job ID for traceability
   *   <li>Source and target states
   *   <li>Clear indication of why the transition is invalid
   * </ul>
   *
   * @param from the current job status (must not be null)
   * @param to the desired job status (must not be null)
   * @param jobId the job ID for error reporting and audit trail
   * @throws IllegalStateException if the transition is not valid, with a descriptive message
   * @throws NullPointerException if from or to status is null
   */
  public void validateTransition(JobStatus from, JobStatus to, Long jobId) {
    if (!isValidTransition(from, to)) {
      throw new IllegalStateException(
          String.format("Invalid job state transition for job %d: %s -> %s", jobId, from, to));
    }
  }
}

package run.ratchet.ri.core;

import run.ratchet.store.entity.JobStatus;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Stateless validator enforcing the job lifecycle state machine. Same-state transitions are
 * idempotent.
 */
public class JobStateValidator {

  private static final Map<JobStatus, Set<JobStatus>> VALID_TRANSITIONS =
      Map.of(
          JobStatus.PENDING,
          Set.of(JobStatus.RUNNING, JobStatus.CANCELED),
          JobStatus.RUNNING,
          Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.PENDING, JobStatus.CANCELED),
          JobStatus.SUCCEEDED,
          Set.of(),
          JobStatus.FAILED,
          Set.of(),
          JobStatus.CANCELED,
          Set.of());

  public boolean canCancel(JobStatus status) {
    return status == JobStatus.PENDING || status == JobStatus.RUNNING;
  }

  public boolean canRetry(JobStatus status) {
    return status == JobStatus.RUNNING;
  }

  public boolean isTerminalState(JobStatus status) {
    return status != null && status.isTerminal();
  }

  public boolean isValidTransition(JobStatus from, JobStatus to) {
    if (from == to) {
      return true;
    }

    Set<JobStatus> allowedTransitions = VALID_TRANSITIONS.get(from);
    return allowedTransitions != null && allowedTransitions.contains(to);
  }

  /**
   * @throws IllegalStateException if the transition is not valid
   */
  public void validateTransition(JobStatus from, JobStatus to, UUID jobId) {
    if (!isValidTransition(from, to)) {
      throw new IllegalStateException(
          String.format("Invalid job state transition for job %s: %s -> %s", jobId, from, to));
    }
  }
}

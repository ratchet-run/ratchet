package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when a workflow branch condition is triggered.
 *
 * <p>{@code branchCondition} is the persisted condition description or expression that matched.
 * {@code nextJobId} identifies the job scheduled for the triggered branch.
 */
public class WorkflowBranchTriggeredEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 1721949020293115008L;

  private final String branchCondition;
  private final UUID nextJobId;

  /**
   * Creates a workflow-branch event with an explicit timestamp.
   *
   * @param branchCondition persisted condition description or expression that matched
   * @param nextJobId job scheduled for the triggered branch
   */
  public WorkflowBranchTriggeredEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String branchCondition,
      UUID nextJobId) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.branchCondition = EventContract.requireNonBlank(branchCondition, "branchCondition");
    this.nextJobId = EventContract.requireNonNull(nextJobId, "nextJobId");
  }

  /**
   * Creates a workflow-branch event using the current system clock instant.
   *
   * @param branchCondition persisted condition description or expression that matched
   * @param nextJobId job scheduled for the triggered branch
   */
  public WorkflowBranchTriggeredEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String branchCondition,
      UUID nextJobId) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.branchCondition = EventContract.requireNonBlank(branchCondition, "branchCondition");
    this.nextJobId = EventContract.requireNonNull(nextJobId, "nextJobId");
  }

  /** Returns the persisted condition description or expression that matched. */
  public String getBranchCondition() {
    return branchCondition;
  }

  /** Returns the job id scheduled for the triggered branch. */
  public UUID getNextJobId() {
    return nextJobId;
  }
}

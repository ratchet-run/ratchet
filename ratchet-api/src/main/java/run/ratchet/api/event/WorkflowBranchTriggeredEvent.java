package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/**
 * Represents an event triggered when a workflow branch condition is met in a job scheduling system.
 *
 * <p>This event captures the condition that caused the branch to be triggered and identifies the
 * next job to execute within the workflow. It extends {@link AbstractJobSchedulerEvent}, inheriting
 * common job metadata such as job ID, business key, job type, priority, node ID, and timestamp.
 */
public class WorkflowBranchTriggeredEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 1721949020293115008L;

  /** The condition expression that triggered the branch. */
  private final String branchCondition;

  /** The ID of the next job to execute in the workflow branch. */
  private final String nextJobId;

  public WorkflowBranchTriggeredEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String branchCondition,
      String nextJobId) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.branchCondition = branchCondition;
    this.nextJobId = nextJobId;
  }

  public WorkflowBranchTriggeredEvent(
      Long jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String branchCondition,
      String nextJobId) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.branchCondition = branchCondition;
    this.nextJobId = nextJobId;
  }

  /**
   * Retrieves the condition expression that triggered the branch in the workflow.
   *
   * @return the condition expression that caused the branch to be triggered.
   */
  public String getBranchCondition() {
    return branchCondition;
  }

  /**
   * Retrieves the ID of the next job to be executed within the workflow branch.
   *
   * @return the next job ID as a string.
   */
  public String getNextJobId() {
    return nextJobId;
  }
}

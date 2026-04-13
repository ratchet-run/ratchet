package run.ratchet.api.event;

import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;
import java.io.Serial;
import java.time.Instant;

/** Fired when a workflow branch condition is triggered. */
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

  public String getBranchCondition() {
    return branchCondition;
  }

  public String getNextJobId() {
    return nextJobId;
  }
}

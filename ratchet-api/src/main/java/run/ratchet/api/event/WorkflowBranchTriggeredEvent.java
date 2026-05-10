package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Fired when a workflow branch condition is triggered. */
public class WorkflowBranchTriggeredEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 1721949020293115008L;

  private final String branchCondition;
  private final UUID nextJobId;

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
    this.branchCondition = branchCondition;
    this.nextJobId = nextJobId;
  }

  public WorkflowBranchTriggeredEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String branchCondition,
      UUID nextJobId) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.branchCondition = branchCondition;
    this.nextJobId = nextJobId;
  }

  public String getBranchCondition() {
    return branchCondition;
  }

  /** Returns the job id scheduled for the triggered branch. */
  public UUID getNextJobId() {
    return nextJobId;
  }
}

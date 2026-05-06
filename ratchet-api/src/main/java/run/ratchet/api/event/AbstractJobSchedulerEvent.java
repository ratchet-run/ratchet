package run.ratchet.api.event;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

public abstract class AbstractJobSchedulerEvent implements Serializable {

  @Serial private static final long serialVersionUID = 6853988277084004625L;

  private final UUID jobId;
  private final String businessKey;
  private final JobType jobType;
  private final JobPriority priority;
  private final String nodeId;
  private final Instant timestamp;

  protected AbstractJobSchedulerEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp) {
    this.jobId = jobId;
    this.businessKey = businessKey;
    this.jobType = jobType;
    this.priority = priority;
    this.nodeId = nodeId;
    this.timestamp = timestamp;
  }

  protected AbstractJobSchedulerEvent(
      UUID jobId, String businessKey, JobType jobType, JobPriority priority, String nodeId) {
    this(jobId, businessKey, jobType, priority, nodeId, Instant.now());
  }

  public UUID getJobId() {
    return jobId;
  }

  public String getBusinessKey() {
    return businessKey;
  }

  public JobType getJobType() {
    return jobType;
  }

  public JobPriority getPriority() {
    return priority;
  }

  public String getNodeId() {
    return nodeId;
  }

  public Instant getTimestamp() {
    return timestamp;
  }
}

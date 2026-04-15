package run.ratchet.loadtest.api;

import java.time.Instant;

public class JobEnqueuedResponse {

  public String runId;
  public long jobId;
  public int sequence;
  public String workload;
  public String acceptedNodeId;
  public Instant acceptedAt;

  public JobEnqueuedResponse() {}

  public JobEnqueuedResponse(
      String runId,
      long jobId,
      int sequence,
      String workload,
      String acceptedNodeId,
      Instant acceptedAt) {
    this.runId = runId;
    this.jobId = jobId;
    this.sequence = sequence;
    this.workload = workload;
    this.acceptedNodeId = acceptedNodeId;
    this.acceptedAt = acceptedAt;
  }
}

package run.ratchet.loadtest.api;

import java.time.Instant;

public class RunStartedResponse {

  public String runId;
  public String workload;
  public int expectedJobs;
  public Instant startedAt;

  public RunStartedResponse() {}

  public RunStartedResponse(String runId, String workload, int expectedJobs, Instant startedAt) {
    this.runId = runId;
    this.workload = workload;
    this.expectedJobs = expectedJobs;
    this.startedAt = startedAt;
  }
}

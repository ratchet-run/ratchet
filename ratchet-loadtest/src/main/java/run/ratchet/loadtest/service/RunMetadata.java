package run.ratchet.loadtest.service;

import java.time.Instant;

public class RunMetadata {

  private final String runId;
  private final String workload;
  private final int expectedJobs;
  private final Instant startedAt;

  public RunMetadata(String runId, String workload, int expectedJobs, Instant startedAt) {
    this.runId = runId;
    this.workload = workload;
    this.expectedJobs = expectedJobs;
    this.startedAt = startedAt;
  }

  public String runId() {
    return runId;
  }

  public String workload() {
    return workload;
  }

  public int expectedJobs() {
    return expectedJobs;
  }

  public Instant startedAt() {
    return startedAt;
  }
}

package run.ratchet.loadtest.workload;

import java.io.Serializable;

public class WorkloadSpec implements Serializable {

  private final String runId;
  private final WorkloadType type;
  private final int sequence;
  private final long sleepMs;
  private final long sleepJitterMs;
  private final double sleepSpikeRate;
  private final long sleepSpikeMs;
  private final double failureRate;
  private final String payload;

  public WorkloadSpec(
      String runId,
      WorkloadType type,
      int sequence,
      long sleepMs,
      long sleepJitterMs,
      double sleepSpikeRate,
      long sleepSpikeMs,
      double failureRate,
      String payload) {
    this.runId = runId;
    this.type = type;
    this.sequence = sequence;
    this.sleepMs = sleepMs;
    this.sleepJitterMs = sleepJitterMs;
    this.sleepSpikeRate = sleepSpikeRate;
    this.sleepSpikeMs = sleepSpikeMs;
    this.failureRate = failureRate;
    this.payload = payload;
  }

  public String runId() {
    return runId;
  }

  public WorkloadType type() {
    return type;
  }

  public int sequence() {
    return sequence;
  }

  public long sleepMs() {
    return sleepMs;
  }

  public long sleepJitterMs() {
    return sleepJitterMs;
  }

  public double sleepSpikeRate() {
    return sleepSpikeRate;
  }

  public long sleepSpikeMs() {
    return sleepSpikeMs;
  }

  public double failureRate() {
    return failureRate;
  }

  public String payload() {
    return payload;
  }
}

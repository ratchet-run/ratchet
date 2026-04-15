package run.ratchet.loadtest.api;

public class EnqueueJobRequest {

  public String runId;
  public int sequence = 0;
  public String workload = "noop";
  public long sleepMs = 5;
  public long sleepJitterMs = 0;
  public double sleepSpikeRate = 0.0;
  public long sleepSpikeMs = 0;
  public double failureRate = 0.0;
  public int payloadBytes = 0;
  public int maxRetries = 0;
  public String priority = "NORMAL";
  public long timeoutSeconds = 60;
}

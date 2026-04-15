package run.ratchet.loadtest.api;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class RunStatusResponse {

  public String runId;
  public String workload;
  public int expectedJobs;
  public int observedJobs;
  public long terminalJobs;
  public boolean complete;
  public Instant startedAt;
  public Instant checkedAt;
  public Map<String, Long> statusCounts = new LinkedHashMap<>();
}

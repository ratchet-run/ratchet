package run.ratchet.loadtest.api;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class ClusterStatusResponse {

  public String nodeId;
  public long activeNodes;
  public long readyJobs;
  public long pendingJobs;
  public Instant checkedAt;
  public Map<String, Long> statusCounts = new LinkedHashMap<>();
}

package run.ratchet.loadtest.api;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class ClusterStatusResponse {

  private String nodeId;
  private long activeNodes;
  private long readyJobs;
  private long pendingJobs;
  private Instant checkedAt;
  private Map<String, Long> statusCounts = new LinkedHashMap<>();

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public long getActiveNodes() {
    return activeNodes;
  }

  public void setActiveNodes(long activeNodes) {
    this.activeNodes = activeNodes;
  }

  public long getReadyJobs() {
    return readyJobs;
  }

  public void setReadyJobs(long readyJobs) {
    this.readyJobs = readyJobs;
  }

  public long getPendingJobs() {
    return pendingJobs;
  }

  public void setPendingJobs(long pendingJobs) {
    this.pendingJobs = pendingJobs;
  }

  public Instant getCheckedAt() {
    return checkedAt;
  }

  public void setCheckedAt(Instant checkedAt) {
    this.checkedAt = checkedAt;
  }

  public Map<String, Long> getStatusCounts() {
    return statusCounts;
  }

  public void setStatusCounts(Map<String, Long> statusCounts) {
    this.statusCounts =
        statusCounts == null ? new LinkedHashMap<>() : new LinkedHashMap<>(statusCounts);
  }
}

package run.ratchet.loadtest.api;

import java.time.Instant;

public class NodeResponse {

  private String nodeId;
  private Instant checkedAt;

  public NodeResponse() {}

  public NodeResponse(String nodeId, Instant checkedAt) {
    this.nodeId = nodeId;
    this.checkedAt = checkedAt;
  }

  public String getNodeId() {
    return nodeId;
  }

  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  public Instant getCheckedAt() {
    return checkedAt;
  }

  public void setCheckedAt(Instant checkedAt) {
    this.checkedAt = checkedAt;
  }
}

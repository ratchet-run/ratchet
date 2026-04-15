package run.ratchet.loadtest.api;

import java.time.Instant;

public class NodeResponse {

  public String nodeId;
  public Instant checkedAt;

  public NodeResponse() {}

  public NodeResponse(String nodeId, Instant checkedAt) {
    this.nodeId = nodeId;
    this.checkedAt = checkedAt;
  }
}

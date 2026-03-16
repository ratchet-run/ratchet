package run.ratchet.store.entity;

import run.ratchet.store.converter.JsonObjectMapConverter;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Entity representing an active scheduler node in the distributed cluster.
 *
 * <p>This entity maintains a registry of all scheduler nodes participating in job processing,
 * enabling health monitoring, failure detection, and workload distribution.
 */
@Entity
@Table(
    name = "scheduler_node",
    indexes = @Index(name = "idx_node_heartbeat", columnList = "heartbeat_ts"))
public class NodeEntity {

  @Id
  @Column(name = "node_id", length = 64)
  private String id;

  @Column(name = "heartbeat_ts", nullable = false)
  private Instant lastHeartbeat;

  @Column(name = "started_at", nullable = false)
  private Instant startedAt;

  @Convert(converter = JsonObjectMapConverter.class)
  @Column(name = "node_info")
  private Map<String, Object> nodeInfo;

  // ── Getters ──────────────────────────────────────────────────────────────

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Instant getLastHeartbeat() {
    return lastHeartbeat;
  }

  public void setLastHeartbeat(Instant lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }

  // ── Setters ──────────────────────────────────────────────────────────────

  public Instant getStartedAt() {
    return startedAt;
  }

  public void setStartedAt(Instant startedAt) {
    this.startedAt = startedAt;
  }

  public Map<String, Object> getNodeInfo() {
    return nodeInfo;
  }

  public void setNodeInfo(Map<String, Object> nodeInfo) {
    this.nodeInfo = nodeInfo;
  }

  // ── Object overrides ────────────────────────────────────────────────────

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeEntity that = (NodeEntity) o;
    return Objects.equals(id, that.id);
  }
}

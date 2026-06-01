/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import run.ratchet.store.converter.JsonObjectMapConverter;

/** Active scheduler node in the distributed cluster. */
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

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

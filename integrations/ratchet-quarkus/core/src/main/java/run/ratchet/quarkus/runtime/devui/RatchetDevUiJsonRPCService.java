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
package run.ratchet.quarkus.runtime.devui;

import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import run.ratchet.api.ClusterQueryService;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobQueryService;

@ApplicationScoped
public class RatchetDevUiJsonRPCService {

  private static final int JOB_LIMIT = 50;
  private static final Duration SNAPSHOT_INTERVAL = Duration.ofSeconds(3);

  @Inject Instance<JobQueryService> jobQueryService;
  @Inject Instance<ClusterQueryService> clusterQueryService;

  public RatchetSnapshot getSnapshot() {
    List<String> status = new ArrayList<>();

    List<JobRow> jobs = findJobs(status);
    QueueHealthRow health = getQueueHealth(status);
    List<NodeRow> nodes = getNodes(status);

    return new RatchetSnapshot(jobs, nodes, health, status(status));
  }

  public Multi<RatchetSnapshot> streamSnapshot() {
    return Multi.createFrom().ticks().every(SNAPSHOT_INTERVAL).map(ignored -> getSnapshot());
  }

  private List<JobRow> findJobs(List<String> status) {
    if (!jobQueryService.isResolvable()) {
      status.add("Job query service is unavailable.");
      return List.of();
    }
    try {
      return jobQueryService.get().findJobs(defaultJobFilter(), JOB_LIMIT, 0).items().stream()
          .map(JobRow::from)
          .toList();
    } catch (RuntimeException e) {
      status.add("Jobs are unavailable: " + e.getClass().getSimpleName() + ".");
      return List.of();
    }
  }

  private QueueHealthRow getQueueHealth(List<String> status) {
    if (!jobQueryService.isResolvable()) {
      status.add("Queue health is unavailable.");
      return QueueHealthRow.empty();
    }
    try {
      return QueueHealthRow.from(jobQueryService.get().getQueueHealth());
    } catch (RuntimeException e) {
      status.add("Queue health is unavailable: " + e.getClass().getSimpleName() + ".");
      return QueueHealthRow.empty();
    }
  }

  private List<NodeRow> getNodes(List<String> status) {
    if (!clusterQueryService.isResolvable()) {
      status.add("Cluster query service is unavailable.");
      return List.of();
    }
    try {
      return clusterQueryService.get().getNodes().items().stream().map(NodeRow::from).toList();
    } catch (RuntimeException e) {
      status.add("Cluster nodes are unavailable: " + e.getClass().getSimpleName() + ".");
      return List.of();
    }
  }

  private JobFilter defaultJobFilter() {
    return JobFilter.builder().skipCount(true).build();
  }

  private String status(List<String> status) {
    return status.isEmpty() ? "Snapshot loaded." : String.join(" ", status);
  }
}

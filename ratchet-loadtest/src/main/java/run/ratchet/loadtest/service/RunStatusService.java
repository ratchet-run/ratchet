package run.ratchet.loadtest.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.loadtest.api.ClusterStatusResponse;
import run.ratchet.loadtest.api.RunStatusResponse;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.TagStore;

@ApplicationScoped
public class RunStatusService {

  private static final int PAGE_SIZE = 500;

  @Inject TagStore tagStore;
  @Inject JobCrudStore jobStore;
  @Inject RunRegistry runRegistry;
  @Inject NodeIdentityProvider nodeIdentityProvider;

  public RunStatusResponse status(String runId) {
    RunMetadata metadata = runRegistry.get(runId).orElse(null);
    RunSummary summary = summarizeRun(Tags.run(runId));
    Map<JobStatus, Long> counts = summary.statusCounts();
    long observedJobs = summary.observedJobs();
    long terminalJobs =
        counts.entrySet().stream()
            .filter(entry -> entry.getKey().isTerminal())
            .mapToLong(Map.Entry::getValue)
            .sum();
    int expectedJobs = metadata == null ? Math.toIntExact(observedJobs) : metadata.expectedJobs();

    RunStatusResponse response = new RunStatusResponse();
    response.runId = runId;
    response.workload = metadata == null ? null : metadata.workload();
    response.expectedJobs = expectedJobs;
    response.observedJobs = Math.toIntExact(observedJobs);
    response.terminalJobs = terminalJobs;
    response.complete = expectedJobs > 0 && terminalJobs >= expectedJobs;
    response.startedAt = metadata == null ? null : metadata.startedAt();
    response.checkedAt = Instant.now();
    for (JobStatus status : JobStatus.values()) {
      response.statusCounts.put(status.name(), counts.getOrDefault(status, 0L));
    }
    response.enqueueNodeCounts.putAll(summary.enqueueNodeCounts());
    response.executionNodeCounts.putAll(summary.executionNodeCounts());
    return response;
  }

  public ClusterStatusResponse cluster() {
    ClusterStatusResponse response = new ClusterStatusResponse();
    response.nodeId = nodeIdentityProvider.getNodeId();
    response.activeNodes = jobStore.countActiveNodes();
    response.readyJobs = jobStore.countReadyJobs(Instant.now());
    response.pendingJobs = jobStore.countPendingJobs();
    response.checkedAt = Instant.now();
    for (JobStatus status : JobStatus.values()) {
      response.statusCounts.put(status.name(), jobStore.countJobsByStatus(status));
    }
    return response;
  }

  public List<UUID> findJobIds(String runId) {
    List<UUID> ids = new ArrayList<>();
    int offset = 0;
    while (true) {
      List<UUID> page = tagStore.findJobIdsByTag(Tags.run(runId), PAGE_SIZE, offset);
      if (page.isEmpty()) {
        return ids;
      }
      ids.addAll(page);
      offset += page.size();
    }
  }

  private RunSummary summarizeRun(String tag) {
    Map<JobStatus, Long> counts = tagStore.countJobsByStatusForTag(tag);
    Map<String, Long> enqueueNodeCounts =
        tagStore.countJobsByParamForTag(tag, Tags.PARAM_ENQUEUE_NODE);
    Map<String, Long> executionNodeCounts = tagStore.countJobsByExecutionNodeForTag(tag);
    long observedJobs = counts.values().stream().mapToLong(Long::longValue).sum();
    return new RunSummary(counts, enqueueNodeCounts, executionNodeCounts, observedJobs);
  }

  private record RunSummary(
      Map<JobStatus, Long> statusCounts,
      Map<String, Long> enqueueNodeCounts,
      Map<String, Long> executionNodeCounts,
      long observedJobs) {}
}

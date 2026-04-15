package run.ratchet.loadtest.service;

import run.ratchet.loadtest.api.ClusterStatusResponse;
import run.ratchet.loadtest.api.RunStatusResponse;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.TagStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class RunStatusService {

  private static final int PAGE_SIZE = 500;

  @Inject TagStore tagStore;
  @Inject JobCrudStore jobStore;
  @Inject RunRegistry runRegistry;
  @Inject NodeIdentityProvider nodeIdentityProvider;

  public RunStatusResponse status(String runId) {
    RunMetadata metadata = runRegistry.get(runId).orElse(null);
    Map<JobStatus, Long> counts = countsForTag(Tags.run(runId));
    long observedJobs = counts.values().stream().mapToLong(Long::longValue).sum();
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

  public List<Long> findJobIds(String runId) {
    List<Long> ids = new ArrayList<>();
    int offset = 0;
    while (true) {
      List<Long> page = tagStore.findJobIdsByTag(Tags.run(runId), PAGE_SIZE, offset);
      if (page.isEmpty()) {
        return ids;
      }
      ids.addAll(page);
      offset += page.size();
    }
  }

  private Map<JobStatus, Long> countsForTag(String tag) {
    Map<JobStatus, Long> counts = new EnumMap<>(JobStatus.class);
    int offset = 0;
    while (true) {
      List<Long> ids = tagStore.findJobIdsByTag(tag, PAGE_SIZE, offset);
      if (ids.isEmpty()) {
        return counts;
      }
      for (JobEntity job : jobStore.findByIds(ids)) {
        counts.merge(job.getStatus(), 1L, Long::sum);
      }
      offset += ids.size();
    }
  }
}

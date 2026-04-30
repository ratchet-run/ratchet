package run.ratchet.loadtest.service;

import run.ratchet.loadtest.api.ResetResponse;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@ApplicationScoped
public class LoadTestResetService {

  @Inject JobCrudStore jobStore;
  @Inject RunRegistry runRegistry;
  @Inject RunStatusService runStatusService;

  public ResetResponse reset(String runId) {
    if (runId != null && !runId.isBlank()) {
      int deleted = deleteRun(runId);
      runRegistry.remove(runId);
      return new ResetResponse(1, deleted);
    }

    Set<String> runIds = new HashSet<>();
    for (RunMetadata metadata : runRegistry.all()) {
      runIds.add(metadata.runId());
    }

    int deleted = 0;
    for (String id : runIds) {
      deleted += deleteRun(id);
    }
    runRegistry.clear();
    return new ResetResponse(runIds.size(), deleted);
  }

  private int deleteRun(String runId) {
    List<UUID> jobIds = runStatusService.findJobIds(runId);
    int deleted = 0;
    for (UUID jobId : jobIds) {
      jobStore.delete(jobId);
      deleted++;
    }
    return deleted;
  }
}

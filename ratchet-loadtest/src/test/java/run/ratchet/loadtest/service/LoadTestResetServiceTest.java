package run.ratchet.loadtest.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import run.ratchet.loadtest.api.ResetResponse;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobBulkStore;

class LoadTestResetServiceTest {

  @Test
  void resetRunDeletesJobsInOneBulkStoreCall() {
    UUID first = UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID second = UUID.fromString("00000000-0000-0000-0000-000000000002");
    RecordingJobBulkStore jobStore = new RecordingJobBulkStore();

    LoadTestResetService service = new LoadTestResetService();
    service.jobStore = jobStore;
    service.runRegistry = new RunRegistry();
    service.runStatusService = new FixedRunStatusService(List.of(first, second));

    ResetResponse response = service.reset("run-a");

    assertEquals(1, response.getRunsReset());
    assertEquals(2, response.getJobsDeleted());
    assertEquals(1, jobStore.deleteCalls);
    assertEquals(List.of(first, second), jobStore.deletedIds);
  }

  private static final class FixedRunStatusService extends RunStatusService {
    private final List<UUID> jobIds;

    private FixedRunStatusService(List<UUID> jobIds) {
      this.jobIds = jobIds;
    }

    @Override
    public List<UUID> findJobIds(String runId) {
      return jobIds;
    }
  }

  private static final class RecordingJobBulkStore implements JobBulkStore {
    private int deleteCalls;
    private List<UUID> deletedIds = List.of();

    @Override
    public void bulkInsert(List<JobEntity> jobs) {}

    @Override
    public int deleteJobsByIds(List<UUID> ids) {
      deleteCalls++;
      deletedIds = new ArrayList<>(ids);
      return ids.size();
    }

    @Override
    public int deleteDlqOlderThan(Instant cutoff) {
      return 0;
    }

    @Override
    public int resetOrphanJobs(Duration grace) {
      return 0;
    }

    @Override
    public int resetOrphanJobsForNode(String nodeId) {
      return 0;
    }
  }
}

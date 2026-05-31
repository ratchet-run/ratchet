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

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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import run.ratchet.loadtest.api.ResetResponse;
import run.ratchet.store.spi.JobBulkStore;

@ApplicationScoped
public class LoadTestResetService {

  @Inject JobBulkStore jobStore;
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
    if (jobIds.isEmpty()) {
      return 0;
    }
    return jobStore.deleteJobsByIds(jobIds);
  }
}

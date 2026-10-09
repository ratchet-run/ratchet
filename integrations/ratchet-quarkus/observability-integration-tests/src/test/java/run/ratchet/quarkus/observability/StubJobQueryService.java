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
package run.ratchet.quarkus.observability;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.ExecutionHistorySummary;
import run.ratchet.api.JobDetail;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPage;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobSummary;
import run.ratchet.api.QueueHealthSnapshot;

/** Supplies a fixed queue-health snapshot; this app has no Ratchet engine or store. */
@ApplicationScoped
public class StubJobQueryService implements JobQueryService {

  @Override
  public QueueHealthSnapshot getQueueHealth() {
    return new QueueHealthSnapshot(
        7L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0.0, 0.0, 0L, null, Map.of(), Map.of());
  }

  @Override
  public JobPage<JobSummary> findJobs(JobFilter filter, int limit, int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<JobDetail> getJobDetail(UUID jobId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public JobPage<ExecutionHistorySummary> getExecutionHistory(UUID jobId, int limit, int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public JobPage<JobSummary> getDependants(UUID jobId, int limit, int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public JobPage<JobSummary> getBatchChildren(UUID batchParentId, int limit, int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public JobPage<JobSummary> getRecurringMasters(int limit, int offset) {
    throw new UnsupportedOperationException();
  }
}

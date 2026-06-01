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
package run.ratchet.spi;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/** Default no-op {@link MetricsCollector} for deployments without a monitoring integration. */
@Incubating
@ApplicationScoped
@Default
public class NoOpMetricsCollector implements MetricsCollector {

  @Override
  public void jobStarted(UUID jobId, JobType type, JobPriority priority) {}

  @Override
  public void jobCompleted(UUID jobId, JobType type, long executionTimeMs) {}

  @Override
  public void jobFailed(UUID jobId, JobType type, Throwable cause, int attempt) {}

  @Override
  public void successFinalizationRetried(UUID jobId, JobType type) {}

  @Override
  public void successFinalizationMinimal(UUID jobId, JobType type) {}

  @Override
  public void successFinalizationStuck(UUID jobId, JobType type) {}

  @Override
  public void claimTransientFailure(String executionType) {}

  @Override
  public void jobsClaimed(String executionType, int claimedCount) {}

  @Override
  public void gateRejected(String executionType, String gateStatus) {}

  @Override
  public void localWakeup(String source) {}

  @Override
  public void clusterWakeupPublished(String transport, String outcome) {}

  @Override
  public void clusterWakeupReceived(String transport, String outcome) {}
}

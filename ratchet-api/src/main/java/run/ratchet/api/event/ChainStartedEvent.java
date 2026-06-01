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
package run.ratchet.api.event;

import java.io.Serial;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when a job chain begins execution.
 *
 * <p>{@code jobId} identifies the first chain step being started. {@code parentJobId} identifies
 * the root job that triggered the chain.
 */
@Incubating
public class ChainStartedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = -4450548507481291423L;

  private final UUID parentJobId;

  public ChainStartedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      UUID parentJobId) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.parentJobId = EventContract.requireNonNull(parentJobId, "parentJobId");
  }

  public ChainStartedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      UUID parentJobId) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.parentJobId = EventContract.requireNonNull(parentJobId, "parentJobId");
  }

  /** Returns the root job that triggered the chain. */
  public UUID getParentJobId() {
    return parentJobId;
  }
}

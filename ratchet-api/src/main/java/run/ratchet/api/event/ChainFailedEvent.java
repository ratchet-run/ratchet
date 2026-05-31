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

/** Fired when a job chain fails. */
@Incubating
public class ChainFailedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 5542623623918947230L;

  private final UUID parentJobId;
  private final String errorMessage;

  /**
   * Creates an event with an explicit timestamp.
   *
   * @param parentJobId root or parent job whose chain failed
   * @param errorMessage sanitized failure message that stopped the chain
   */
  public ChainFailedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      UUID parentJobId,
      String errorMessage) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.parentJobId = EventContract.requireNonNull(parentJobId, "parentJobId");
    this.errorMessage = EventContract.requireNonBlank(errorMessage, "errorMessage");
  }

  /**
   * Creates an event using the current system clock instant.
   *
   * @param parentJobId root or parent job whose chain failed
   * @param errorMessage sanitized failure message that stopped the chain
   */
  public ChainFailedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      UUID parentJobId,
      String errorMessage) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.parentJobId = EventContract.requireNonNull(parentJobId, "parentJobId");
    this.errorMessage = EventContract.requireNonBlank(errorMessage, "errorMessage");
  }

  /** Returns the root or parent job whose chain failed. */
  public UUID getParentJobId() {
    return parentJobId;
  }

  /** Returns the sanitized failure message that stopped the chain. */
  public String getErrorMessage() {
    return errorMessage;
  }
}

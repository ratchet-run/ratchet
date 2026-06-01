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

/** Fired when a job completes successfully. */
@Incubating
public class JobCompletedEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 6928539910648242733L;

  private final Long executionTimeMs;

  /**
   * Creates a completion event with an explicit timestamp.
   *
   * @param executionTimeMs wall-clock execution duration in milliseconds, or {@code null} if not
   *     recorded
   */
  public JobCompletedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.executionTimeMs = EventContract.requireNonNegative(executionTimeMs, "executionTimeMs");
  }

  /**
   * Creates a completion event using the current system clock instant.
   *
   * <p>Use the constructor that accepts an explicit {@link Instant} for tests, replay, or any path
   * that already has a scheduler-provided timestamp.
   *
   * @param executionTimeMs wall-clock execution duration in milliseconds, or {@code null} if not
   *     recorded
   */
  public JobCompletedEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.executionTimeMs = EventContract.requireNonNegative(executionTimeMs, "executionTimeMs");
  }

  /** Returns the wall-clock execution duration in milliseconds, or {@code null} if not recorded. */
  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }
}

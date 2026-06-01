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
 * Fired when a job has been cancelled.
 *
 * <p>Fired after the job record reaches CANCELED state.
 */
@Incubating
public class JobCancelledEvent extends AbstractJobCancellationEvent {

  @Serial private static final long serialVersionUID = -3714116971496582534L;

  public JobCancelledEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String previousStatus,
      Long executionTimeMs) {
    super(
        jobId, businessKey, jobType, priority, nodeId, timestamp, previousStatus, executionTimeMs);
  }

  public JobCancelledEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String previousStatus,
      Long executionTimeMs) {
    super(jobId, businessKey, jobType, priority, nodeId, previousStatus, executionTimeMs);
  }
}

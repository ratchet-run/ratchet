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
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobType;

/**
 * Fired when a job has been created in WAITING state, blocked on a named signal.
 *
 * <p>{@code signalTimeout} is the maximum time the job may wait before timing out. A {@code null}
 * timeout means the job waits until a matching signal is delivered or the job is canceled.
 */
@Incubating
public class JobSignalWaitingEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 7412309856012374810L;

  private final String signalKey;
  private final Duration signalTimeout;

  public JobSignalWaitingEvent(
      UUID jobId,
      String businessKey,
      UUID recurringMasterId,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String signalKey,
      Duration signalTimeout) {
    super(jobId, businessKey, recurringMasterId, jobType, priority, nodeId, timestamp);
    this.signalKey = EventContract.requireNonBlank(signalKey, "signalKey");
    this.signalTimeout = signalTimeout;
  }

  public JobSignalWaitingEvent(
      UUID jobId,
      String businessKey,
      UUID recurringMasterId,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String signalKey,
      Duration signalTimeout) {
    this(
        jobId,
        businessKey,
        recurringMasterId,
        jobType,
        priority,
        nodeId,
        Instant.now(),
        signalKey,
        signalTimeout);
  }

  /** Returns the signal key the job is waiting on. */
  public String getSignalKey() {
    return signalKey;
  }

  /**
   * Returns the maximum time the job may wait for its signal.
   *
   * <p>A {@code null} value means the job has no signal timeout and waits until a matching signal
   * is delivered or the job is canceled.
   */
  public Duration getSignalTimeout() {
    return signalTimeout;
  }
}

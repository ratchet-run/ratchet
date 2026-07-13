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
 * Fired after a running job exceeds its configured execution timeout and Ratchet applies the
 * resulting retry or terminal-failure transition.
 *
 * <p>This event identifies the timeout as the cause of the transition. A retry also publishes a
 * {@link JobRetryingEvent}; a terminal timeout also publishes {@link JobFailedEvent} and follows
 * the normal dead-letter path. Crossing the soft warning threshold does not publish this event.
 */
@Incubating
public class JobExecutionTimedOutEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 1L;

  private final Duration executionTimeout;
  private final Duration elapsedTime;
  private final int retryAttempt;

  /**
   * Creates an execution-timeout event with an explicit timestamp.
   *
   * @param executionTimeout configured maximum execution duration that was exceeded
   * @param elapsedTime observed execution duration when the hard-timeout watchdog fired
   * @param retryAttempt 1-based failed-attempt count recorded by the timeout transition
   */
  public JobExecutionTimedOutEvent(
      UUID jobId,
      String businessKey,
      UUID recurringMasterId,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      Duration executionTimeout,
      Duration elapsedTime,
      int retryAttempt) {
    super(jobId, businessKey, recurringMasterId, jobType, priority, nodeId, timestamp);
    this.executionTimeout = EventContract.requirePositive(executionTimeout, "executionTimeout");
    this.elapsedTime = EventContract.requireNonNegative(elapsedTime, "elapsedTime");
    this.retryAttempt = EventContract.requirePositive(retryAttempt, "retryAttempt");
  }

  /**
   * Creates an execution-timeout event using the current system clock instant.
   *
   * @param executionTimeout configured maximum execution duration that was exceeded
   * @param elapsedTime observed execution duration when the hard-timeout watchdog fired
   * @param retryAttempt 1-based failed-attempt count recorded by the timeout transition
   */
  public JobExecutionTimedOutEvent(
      UUID jobId,
      String businessKey,
      UUID recurringMasterId,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Duration executionTimeout,
      Duration elapsedTime,
      int retryAttempt) {
    this(
        jobId,
        businessKey,
        recurringMasterId,
        jobType,
        priority,
        nodeId,
        Instant.now(),
        executionTimeout,
        elapsedTime,
        retryAttempt);
  }

  /** Returns the configured maximum execution duration that was exceeded. */
  public Duration getExecutionTimeout() {
    return executionTimeout;
  }

  /** Returns the observed execution duration when the hard-timeout watchdog fired. */
  public Duration getElapsedTime() {
    return elapsedTime;
  }

  /** Returns the 1-based failed-attempt count recorded by the timeout transition. */
  public int getRetryAttempt() {
    return retryAttempt;
  }
}

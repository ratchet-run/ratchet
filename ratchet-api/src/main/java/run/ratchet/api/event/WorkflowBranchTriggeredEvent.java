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
 * Fired when a workflow branch condition is triggered.
 *
 * <p>{@code branchCondition} is the persisted condition description or expression that matched.
 * {@code nextJobId} identifies the job scheduled for the triggered branch.
 */
@Incubating
public class WorkflowBranchTriggeredEvent extends AbstractJobSchedulerEvent {

  @Serial private static final long serialVersionUID = 1721949020293115008L;

  private final String branchCondition;
  private final UUID nextJobId;

  /**
   * Creates a workflow-branch event with an explicit timestamp.
   *
   * @param branchCondition persisted condition description or expression that matched
   * @param nextJobId job scheduled for the triggered branch
   */
  public WorkflowBranchTriggeredEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      Instant timestamp,
      String branchCondition,
      UUID nextJobId) {
    super(jobId, businessKey, jobType, priority, nodeId, timestamp);
    this.branchCondition = EventContract.requireNonBlank(branchCondition, "branchCondition");
    this.nextJobId = EventContract.requireNonNull(nextJobId, "nextJobId");
  }

  /**
   * Creates a workflow-branch event using the current system clock instant.
   *
   * @param branchCondition persisted condition description or expression that matched
   * @param nextJobId job scheduled for the triggered branch
   */
  public WorkflowBranchTriggeredEvent(
      UUID jobId,
      String businessKey,
      JobType jobType,
      JobPriority priority,
      String nodeId,
      String branchCondition,
      UUID nextJobId) {
    super(jobId, businessKey, jobType, priority, nodeId);
    this.branchCondition = EventContract.requireNonBlank(branchCondition, "branchCondition");
    this.nextJobId = EventContract.requireNonNull(nextJobId, "nextJobId");
  }

  /** Returns the persisted condition description or expression that matched. */
  public String getBranchCondition() {
    return branchCondition;
  }

  /** Returns the job id scheduled for the triggered branch. */
  public UUID getNextJobId() {
    return nextJobId;
  }
}

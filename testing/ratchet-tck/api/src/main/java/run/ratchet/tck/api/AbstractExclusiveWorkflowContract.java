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
package run.ratchet.tck.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobSummary;

/** Public API contract for exclusive, first-match-wins workflow routing. */
public abstract class AbstractExclusiveWorkflowContract {

  @AfterEach
  void clearAfterEach() {
    runtime().clear();
    TckJobs.resetAll();
  }

  @Test
  void overlappingConditions_scheduleOnlyFirstEligibleBranchAndCancelSibling()
      throws InterruptedException {
    JobHandle parent =
        runtime()
            .scheduler()
            .enqueue(TckJobs::noop)
            .<Void>when(TckJobs::workflowConditionMatches, TckJobs::recordSiblingWorkflowBranch, 20)
            .<Void>when(
                TckJobs::workflowConditionMatches, TckJobs::recordPreferredWorkflowBranch, 10)
            .submit();
    runtime().probe().track(parent);

    assertTrue(
        runtime().probe().awaitCompleted(parent, defaultTimeout()),
        "workflow parent should complete");

    List<JobSummary> branches = awaitTerminalBranches(parent.id());
    assertEquals(2, branches.size(), "both workflow branches should be persisted");
    assertEquals(
        JobStatus.SUCCEEDED,
        statusOf(branches, "recordPreferredWorkflowBranch"),
        "the first matching branch should run");
    assertEquals(
        JobStatus.CANCELED,
        statusOf(branches, "recordSiblingWorkflowBranch"),
        "the remaining matching sibling should be canceled");
    assertEquals(
        List.of("preferred"),
        TckJobs.workflowBranchEvents(),
        "the canceled sibling task body must never run");
  }

  @Test
  void equalPriorityConditions_scheduleFirstRegisteredBranchAndCancelSibling()
      throws InterruptedException {
    JobHandle parent =
        runtime()
            .scheduler()
            .enqueue(TckJobs::noop)
            .<Void>when(
                TckJobs::workflowConditionMatches, TckJobs::recordPreferredWorkflowBranch, 10)
            .<Void>when(TckJobs::workflowConditionMatches, TckJobs::recordSiblingWorkflowBranch, 10)
            .submit();
    runtime().probe().track(parent);

    assertTrue(
        runtime().probe().awaitCompleted(parent, defaultTimeout()),
        "workflow parent should complete");

    List<JobSummary> branches = awaitTerminalBranches(parent.id());
    assertEquals(2, branches.size(), "both workflow branches should be persisted");
    assertEquals(
        JobStatus.SUCCEEDED,
        statusOf(branches, "recordPreferredWorkflowBranch"),
        "the first registered equal-priority branch should run");
    assertEquals(
        JobStatus.CANCELED,
        statusOf(branches, "recordSiblingWorkflowBranch"),
        "the later equal-priority sibling should be canceled");
    assertEquals(
        List.of("preferred"),
        TckJobs.workflowBranchEvents(),
        "the canceled equal-priority sibling task body must never run");
  }

  protected abstract RatchetTckRuntime runtime();

  protected abstract JobQueryService queryService();

  protected Duration defaultTimeout() {
    return Duration.ofSeconds(30);
  }

  private List<JobSummary> awaitTerminalBranches(UUID parentId) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + defaultTimeout().toNanos();
    List<JobSummary> branches = List.of();
    while (System.nanoTime() < deadlineNanos) {
      branches = queryService().getDependants(parentId);
      if (branches.size() == 2
          && branches.stream()
              .allMatch(
                  branch ->
                      branch.status() == JobStatus.SUCCEEDED
                          || branch.status() == JobStatus.CANCELED)) {
        return branches;
      }
      Thread.sleep(200L);
    }
    return branches;
  }

  private static JobStatus statusOf(List<JobSummary> branches, String methodName) {
    return branches.stream()
        .filter(branch -> methodName.equals(branch.methodName()))
        .map(JobSummary::status)
        .findFirst()
        .orElseThrow(() -> new AssertionError("missing workflow branch " + methodName));
  }
}

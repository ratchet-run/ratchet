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
package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.JobResult;

/**
 * Tracks which workflow branch fired for integration tests.
 *
 * <p>Used with {@code thenOnSuccess} / {@code thenOnFailure} to verify that the correct branch
 * executes based on the parent job's outcome.
 */
public class WorkflowBranchTracker {

  private WorkflowBranchTracker() {}

  private static final AtomicBoolean SUCCESS_BRANCH_FIRED = new AtomicBoolean(false);
  private static final AtomicBoolean FAILURE_BRANCH_FIRED = new AtomicBoolean(false);
  private static final AtomicBoolean SUCCESS_SCENARIO_SUCCESS_BRANCH_FIRED =
      new AtomicBoolean(false);
  private static final AtomicBoolean SUCCESS_SCENARIO_FAILURE_BRANCH_FIRED =
      new AtomicBoolean(false);
  private static final AtomicBoolean FAILURE_SCENARIO_SUCCESS_BRANCH_FIRED =
      new AtomicBoolean(false);
  private static final AtomicBoolean FAILURE_SCENARIO_FAILURE_BRANCH_FIRED =
      new AtomicBoolean(false);
  private static final AtomicBoolean CONDITIONAL_BRANCH_FIRED = new AtomicBoolean(false);
  private static final AtomicInteger CONDITIONAL_BRANCH_EXECUTIONS = new AtomicInteger();

  public static void onSuccess() {
    SUCCESS_BRANCH_FIRED.set(true);
  }

  public static void onFailure() {
    FAILURE_BRANCH_FIRED.set(true);
  }

  public static void onConditional() {
    CONDITIONAL_BRANCH_FIRED.set(true);
    CONDITIONAL_BRANCH_EXECUTIONS.incrementAndGet();
  }

  public static boolean throwingCondition(JobResult<Void> result) {
    throw new IllegalStateException("conditional predicate failed");
  }

  public static void throwingConditional() {
    throw new IllegalStateException("conditional branch failed");
  }

  public static void onSuccessScenarioSuccess() {
    SUCCESS_SCENARIO_SUCCESS_BRANCH_FIRED.set(true);
  }

  public static void onSuccessScenarioFailure() {
    SUCCESS_SCENARIO_FAILURE_BRANCH_FIRED.set(true);
  }

  public static void onFailureScenarioSuccess() {
    FAILURE_SCENARIO_SUCCESS_BRANCH_FIRED.set(true);
  }

  public static void onFailureScenarioFailure() {
    FAILURE_SCENARIO_FAILURE_BRANCH_FIRED.set(true);
  }

  public static boolean successBranchFired() {
    return SUCCESS_BRANCH_FIRED.get();
  }

  public static boolean failureBranchFired() {
    return FAILURE_BRANCH_FIRED.get();
  }

  public static boolean conditionalBranchFired() {
    return CONDITIONAL_BRANCH_FIRED.get();
  }

  public static int conditionalBranchExecutionCount() {
    return CONDITIONAL_BRANCH_EXECUTIONS.get();
  }

  public static boolean successScenarioSuccessBranchFired() {
    return SUCCESS_SCENARIO_SUCCESS_BRANCH_FIRED.get();
  }

  public static boolean successScenarioFailureBranchFired() {
    return SUCCESS_SCENARIO_FAILURE_BRANCH_FIRED.get();
  }

  public static boolean failureScenarioSuccessBranchFired() {
    return FAILURE_SCENARIO_SUCCESS_BRANCH_FIRED.get();
  }

  public static boolean failureScenarioFailureBranchFired() {
    return FAILURE_SCENARIO_FAILURE_BRANCH_FIRED.get();
  }

  public static void reset() {
    SUCCESS_BRANCH_FIRED.set(false);
    FAILURE_BRANCH_FIRED.set(false);
    SUCCESS_SCENARIO_SUCCESS_BRANCH_FIRED.set(false);
    SUCCESS_SCENARIO_FAILURE_BRANCH_FIRED.set(false);
    FAILURE_SCENARIO_SUCCESS_BRANCH_FIRED.set(false);
    FAILURE_SCENARIO_FAILURE_BRANCH_FIRED.set(false);
    CONDITIONAL_BRANCH_FIRED.set(false);
    CONDITIONAL_BRANCH_EXECUTIONS.set(0);
  }
}

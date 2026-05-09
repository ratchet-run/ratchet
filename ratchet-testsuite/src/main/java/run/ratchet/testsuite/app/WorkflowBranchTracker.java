package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

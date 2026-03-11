package run.ratchet.testsuite.app;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tracks execution order for chain/sequential job tests.
 *
 * <p>Each step method appends its identifier to a shared list, allowing tests to verify that chain
 * steps execute in the correct order.
 */
public class ChainStepTracker {

  private static final CopyOnWriteArrayList<String> EXECUTION_ORDER = new CopyOnWriteArrayList<>();

  public static void stepA() {
    EXECUTION_ORDER.add("A");
  }

  public static void stepB() {
    EXECUTION_ORDER.add("B");
  }

  public static void stepC() {
    EXECUTION_ORDER.add("C");
  }

  public static List<String> executionOrder() {
    return List.copyOf(EXECUTION_ORDER);
  }

  public static void reset() {
    EXECUTION_ORDER.clear();
  }
}

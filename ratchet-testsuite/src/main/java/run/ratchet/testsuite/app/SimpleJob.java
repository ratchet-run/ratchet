package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal job implementation for basic lifecycle tests.
 *
 * <p>Tracks invocation count via a static atomic counter that tests can read to verify execution.
 */
public class SimpleJob {

  private static final AtomicInteger INVOCATION_COUNT = new AtomicInteger(0);

  public static void execute() {
    INVOCATION_COUNT.incrementAndGet();
  }

  public static int getInvocationCount() {
    return INVOCATION_COUNT.get();
  }

  public static void resetCount() {
    INVOCATION_COUNT.set(0);
  }
}

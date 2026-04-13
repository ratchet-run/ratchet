package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * No-op job for performance testing. Isolates scheduler overhead from job logic by doing zero work
 * in the execute method.
 */
public class TimingJob {

  private static final AtomicInteger INVOCATION_COUNT = new AtomicInteger(0);

  public static void execute() {
    INVOCATION_COUNT.incrementAndGet();
  }

  public static void processBatchItem(String item) {
    INVOCATION_COUNT.incrementAndGet();
  }

  public static int getInvocationCount() {
    return INVOCATION_COUNT.get();
  }

  public static void resetCount() {
    INVOCATION_COUNT.set(0);
  }
}

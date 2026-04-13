package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicInteger;

/** Always throws; tracks attempts for retry tests. */
public class FailingJob {

  private static final AtomicInteger ATTEMPT_COUNT = new AtomicInteger(0);

  public static void execute() {
    ATTEMPT_COUNT.incrementAndGet();
    throw new RuntimeException("Intentional test failure");
  }

  public static int getAttemptCount() {
    return ATTEMPT_COUNT.get();
  }

  public static void resetCount() {
    ATTEMPT_COUNT.set(0);
  }
}

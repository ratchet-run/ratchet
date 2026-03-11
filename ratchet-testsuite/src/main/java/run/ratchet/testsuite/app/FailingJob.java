package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Job that always throws an exception. Used for retry and failure-handling tests.
 *
 * <p>Tracks attempt count so tests can verify retry behavior.
 */
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

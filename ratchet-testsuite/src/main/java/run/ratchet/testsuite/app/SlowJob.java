package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Job with configurable sleep duration. Used for timeout and cancellation tests.
 *
 * <p>Default sleep is 60 seconds. Override via {@link #setSleepMs(long)} before submitting.
 */
public class SlowJob {

  private static final AtomicBoolean STARTED = new AtomicBoolean(false);
  private static final AtomicBoolean COMPLETED = new AtomicBoolean(false);
  private static volatile long sleepMs = 60_000;

  public static void execute() throws InterruptedException {
    STARTED.set(true);
    Thread.sleep(sleepMs);
    COMPLETED.set(true);
  }

  public static void setSleepMs(long ms) {
    sleepMs = ms;
  }

  public static boolean hasStarted() {
    return STARTED.get();
  }

  public static boolean hasCompleted() {
    return COMPLETED.get();
  }

  public static void reset() {
    sleepMs = 60_000;
    STARTED.set(false);
    COMPLETED.set(false);
  }
}

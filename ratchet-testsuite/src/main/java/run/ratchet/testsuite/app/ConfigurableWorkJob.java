package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Job with configurable sleep duration for performance testing. Simulates real workloads with
 * controllable execution time.
 */
public class ConfigurableWorkJob {

  private static volatile long sleepMs = 5;
  private static final AtomicInteger INVOCATION_COUNT = new AtomicInteger(0);

  public static void execute() throws InterruptedException {
    Thread.sleep(sleepMs);
    INVOCATION_COUNT.incrementAndGet();
  }

  public static void setSleepMs(long ms) {
    sleepMs = ms;
  }

  public static int getInvocationCount() {
    return INVOCATION_COUNT.get();
  }

  public static void reset() {
    sleepMs = 5;
    INVOCATION_COUNT.set(0);
  }
}

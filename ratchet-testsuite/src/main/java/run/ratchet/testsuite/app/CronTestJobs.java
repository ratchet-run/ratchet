package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple job for recurring/cron schedule tests.
 *
 * <p>Tracks tick count so tests can verify that the cron schedule fires at the expected rate.
 */
public class CronTestJobs {

  private static final AtomicInteger TICK_COUNT = new AtomicInteger(0);

  public static void tick() {
    TICK_COUNT.incrementAndGet();
  }

  public static int tickCount() {
    return TICK_COUNT.get();
  }

  public static void reset() {
    TICK_COUNT.set(0);
  }
}

package run.ratchet.testsuite.app;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.Recurring;

/**
 * CDI bean with {@code @Recurring} annotated methods for testing recurring job discovery and
 * scheduling.
 */
@ApplicationScoped
public class TestRecurringJobs {

  private static final AtomicInteger EVERY_MINUTE_COUNT = new AtomicInteger(0);

  public static int getEveryMinuteCount() {
    return EVERY_MINUTE_COUNT.get();
  }

  public static void resetCounts() {
    EVERY_MINUTE_COUNT.set(0);
  }

  @Recurring(cron = "*/5 * * * * ?", id = "test-every-5-seconds")
  public void everyMinute() {
    EVERY_MINUTE_COUNT.incrementAndGet();
  }
}

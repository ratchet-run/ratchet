package run.ratchet.testsuite.app;

import java.util.concurrent.atomic.AtomicInteger;

public class ResourceTestJob {

  private static final AtomicInteger CONCURRENT = new AtomicInteger();
  private static final AtomicInteger MAX_CONCURRENT = new AtomicInteger();
  private static final AtomicInteger COMPLETED = new AtomicInteger();

  public static void execute() throws InterruptedException {
    int current = CONCURRENT.incrementAndGet();
    MAX_CONCURRENT.accumulateAndGet(current, Math::max);
    Thread.sleep(500);
    CONCURRENT.decrementAndGet();
    COMPLETED.incrementAndGet();
  }

  public static int getMaxConcurrentSeen() {
    return MAX_CONCURRENT.get();
  }

  public static int getCompletedCount() {
    return COMPLETED.get();
  }

  public static void reset() {
    CONCURRENT.set(0);
    MAX_CONCURRENT.set(0);
    COMPLETED.set(0);
  }
}

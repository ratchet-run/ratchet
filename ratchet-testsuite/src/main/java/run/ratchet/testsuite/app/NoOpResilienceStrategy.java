package run.ratchet.testsuite.app;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.spi.ResilienceStrategy;

@Alternative
@Priority(1000)
@ApplicationScoped
public class NoOpResilienceStrategy implements ResilienceStrategy {

  private static final AtomicInteger EXECUTE_COUNT = new AtomicInteger(0);
  private static final AtomicInteger AVAILABILITY_CHECK_COUNT = new AtomicInteger(0);
  private static final ConcurrentLinkedQueue<String> EXECUTED_SERVICES =
      new ConcurrentLinkedQueue<>();
  private static final ConcurrentLinkedQueue<String> CHECKED_SERVICES =
      new ConcurrentLinkedQueue<>();

  public static int getExecuteCount() {
    return EXECUTE_COUNT.get();
  }

  public static int getAvailabilityCheckCount() {
    return AVAILABILITY_CHECK_COUNT.get();
  }

  public static List<String> executedServices() {
    return List.copyOf(EXECUTED_SERVICES);
  }

  public static List<String> checkedServices() {
    return List.copyOf(CHECKED_SERVICES);
  }

  public static void resetCounts() {
    EXECUTE_COUNT.set(0);
    AVAILABILITY_CHECK_COUNT.set(0);
    EXECUTED_SERVICES.clear();
    CHECKED_SERVICES.clear();
  }

  @Override
  public <T> T execute(String serviceName, Callable<T> task) throws Exception {
    EXECUTE_COUNT.incrementAndGet();
    EXECUTED_SERVICES.add(serviceName);
    return task.call();
  }

  @Override
  public boolean isServiceAvailable(String serviceName) {
    AVAILABILITY_CHECK_COUNT.incrementAndGet();
    CHECKED_SERVICES.add(serviceName);
    return true;
  }
}

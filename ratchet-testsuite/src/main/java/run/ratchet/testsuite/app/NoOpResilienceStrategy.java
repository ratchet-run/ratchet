package run.ratchet.testsuite.app;

import run.ratchet.spi.ResilienceStrategy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@Alternative
@Priority(1000)
@ApplicationScoped
public class NoOpResilienceStrategy implements ResilienceStrategy {

  private static final AtomicInteger EXECUTE_COUNT = new AtomicInteger(0);

  @Override
  public <T> T execute(String serviceName, Callable<T> task) throws Exception {
    EXECUTE_COUNT.incrementAndGet();
    return task.call();
  }

  @Override
  public boolean isServiceAvailable(String serviceName) {
    return true;
  }

  public static int getExecuteCount() {
    return EXECUTE_COUNT.get();
  }

  public static void resetCounts() {
    EXECUTE_COUNT.set(0);
  }
}

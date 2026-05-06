package run.ratchet.testsuite.app;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;

/** FAST profile: 3 min calls, 10s wait, 2 half-open. */
@ApplicationScoped
public class CircuitBreakerTestService {

  private static final AtomicInteger CALL_COUNT = new AtomicInteger(0);
  private static volatile boolean shouldFail = false;

  public static void setShouldFail(boolean fail) {
    shouldFail = fail;
  }

  public static int getCallCount() {
    return CALL_COUNT.get();
  }

  public static void reset() {
    CALL_COUNT.set(0);
    shouldFail = false;
  }

  @CircuitBreakerProtected(service = "test-service", profile = CircuitBreakerProfile.FAST)
  public String callService() {
    CALL_COUNT.incrementAndGet();
    if (shouldFail) {
      throw new RuntimeException("Simulated service failure");
    }
    return "success";
  }
}

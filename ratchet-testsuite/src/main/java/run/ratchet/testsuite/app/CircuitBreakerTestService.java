package run.ratchet.testsuite.app;

import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CDI bean with circuit breaker-protected methods for integration testing.
 *
 * <p>Uses the FAST profile for quicker test feedback (3 minimum calls, 10s wait, 2 half-open
 * calls).
 */
@ApplicationScoped
public class CircuitBreakerTestService {

  private static final AtomicInteger CALL_COUNT = new AtomicInteger(0);
  private static volatile boolean shouldFail = false;

  @CircuitBreakerProtected(service = "test-service", profile = CircuitBreakerProfile.FAST)
  public String callService() {
    CALL_COUNT.incrementAndGet();
    if (shouldFail) {
      throw new RuntimeException("Simulated service failure");
    }
    return "success";
  }

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
}

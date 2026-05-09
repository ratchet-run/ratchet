package run.ratchet.testsuite.app;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;

/** FAST profile: 3 min calls, 10s wait, 2 half-open. */
@ApplicationScoped
public class CircuitBreakerTestService {

  private static final AtomicInteger CALL_COUNT = new AtomicInteger(0);
  private static volatile boolean shouldFail = false;
  private static volatile CountDownLatch blockStarted;
  private static volatile CountDownLatch blockRelease;

  public static void setShouldFail(boolean fail) {
    shouldFail = fail;
  }

  public static int getCallCount() {
    return CALL_COUNT.get();
  }

  public static void blockCalls(CountDownLatch started, CountDownLatch release) {
    blockStarted = started;
    blockRelease = release;
  }

  public static void reset() {
    CALL_COUNT.set(0);
    shouldFail = false;
    blockStarted = null;
    blockRelease = null;
  }

  @CircuitBreakerProtected(service = "test-service", profile = CircuitBreakerProfile.FAST)
  public String callService() {
    CALL_COUNT.incrementAndGet();
    CountDownLatch started = blockStarted;
    CountDownLatch release = blockRelease;
    if (started != null && release != null) {
      started.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while blocking circuit breaker test call", e);
      }
    }
    if (shouldFail) {
      throw new RuntimeException("Simulated service failure");
    }
    return "success";
  }

  @ApplicationScoped
  public static class TestCircuitBreakerConfigProvider implements CircuitBreakerConfigProvider {

    private static final CircuitBreakerConfig TEST_FAST_CONFIG =
        new CircuitBreakerConfig(50.0f, 20, 100L, 2, 3);

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
      return TEST_FAST_CONFIG;
    }
  }
}

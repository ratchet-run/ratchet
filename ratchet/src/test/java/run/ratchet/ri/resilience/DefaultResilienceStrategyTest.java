package run.ratchet.ri.resilience;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultResilienceStrategyTest {

  private DefaultResilienceStrategy strategy;

  @BeforeEach
  void setUp() {
    CircuitBreakerRegistry registry = new CircuitBreakerRegistry();
    strategy = new DefaultResilienceStrategy(registry);
  }

  @Test
  void executePassesThroughWhenClosed() throws Exception {
    String result = strategy.execute("my-service", () -> "hello");
    assertEquals("hello", result);
  }

  @Test
  void isServiceAvailableReturnsTrueForUnknownService() {
    assertTrue(strategy.isServiceAvailable("unknown-service"));
  }

  @Test
  void executeThrowsWhenCircuitOpen() throws Exception {
    for (int i = 0; i < 10; i++) {
      try {
        strategy.execute(
            "failing-service",
            () -> {
              throw new RuntimeException("fail");
            });
      } catch (RuntimeException ignored) {
        // Expected
      }
    }

    assertThrows(
        ServiceUnavailableException.class,
        () -> strategy.execute("failing-service", () -> "should not run"));
    assertFalse(strategy.isServiceAvailable("failing-service"));
  }

  @Test
  void executePropagatesToTaskExceptions() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                strategy.execute(
                    "test-service",
                    () -> {
                      throw new IllegalStateException("task error");
                    }));
    assertEquals("task error", thrown.getMessage());
  }
}

package run.ratchet.ri.resilience;

import static org.junit.jupiter.api.Assertions.*;

import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.ri.config.DefaultRatchetConfig;
import run.ratchet.ri.config.EnvironmentRatchetConfigSource;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultResilienceStrategyTest {

  private DefaultResilienceStrategy strategy;

  @BeforeEach
  void setUp() {
    CircuitBreakerRegistry registry = new CircuitBreakerRegistry();
    strategy = new DefaultResilienceStrategy(registry);
  }

  @AfterEach
  void clearProperties() {
    System.clearProperty("RATCHET_CB_DEFAULT_FAILURE_RATE");
    System.clearProperty("RATCHET_CB_DEFAULT_WINDOW_SIZE");
    System.clearProperty("RATCHET_CB_DEFAULT_WAIT_MS");
    System.clearProperty("RATCHET_CB_DEFAULT_SLOW_CALL_MS");
    System.clearProperty("RATCHET_CB_DEFAULT_HALF_OPEN_CALLS");
    System.clearProperty("RATCHET_CB_DEFAULT_MIN_CALLS");
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

  @Test
  void disabledCircuitBreakerPassesThroughEvenWhenBreakerIsOpen() throws Exception {
    TestCircuitBreakerConfigProvider provider = new TestCircuitBreakerConfigProvider(false);
    CircuitBreakerRegistry registry = new CircuitBreakerRegistry(provider);
    registry.getBreaker("disabled-service").transitionToOpen();

    DefaultResilienceStrategy disabledStrategy = new DefaultResilienceStrategy(registry, provider);

    assertTrue(disabledStrategy.isServiceAvailable("disabled-service"));
    assertEquals("ok", disabledStrategy.execute("disabled-service", () -> "ok"));
  }

  @Test
  void registryUsesInjectedCircuitBreakerConfiguration() {
    TestCircuitBreakerConfigProvider provider = new TestCircuitBreakerConfigProvider(true);
    provider.config = new CircuitBreakerConfig(75.0f, 7, 1234L, 4321L, 2, 3);

    CircuitBreakerRegistry registry = new CircuitBreakerRegistry(provider);

    assertEquals(1234L, registry.getBreaker("custom-service").getWaitDurationMs());
  }

  @Test
  void defaultCircuitBreakerConfigRejectsOutOfRangeValues() {
    System.setProperty("RATCHET_CB_DEFAULT_FAILURE_RATE", "150");
    System.setProperty("RATCHET_CB_DEFAULT_WINDOW_SIZE", "0");
    System.setProperty("RATCHET_CB_DEFAULT_WAIT_MS", "-1");
    System.setProperty("RATCHET_CB_DEFAULT_SLOW_CALL_MS", "-1");
    System.setProperty("RATCHET_CB_DEFAULT_HALF_OPEN_CALLS", "0");
    System.setProperty("RATCHET_CB_DEFAULT_MIN_CALLS", "0");

    DefaultCircuitBreakerConfigProvider provider =
        new DefaultCircuitBreakerConfigProvider(
            new DefaultRatchetConfig(new EnvironmentRatchetConfigSource()));
    CircuitBreakerConfig config = provider.configFor(CircuitBreakerProfile.DEFAULT);

    assertEquals(
        CircuitBreakerConfiguration.DEFAULT.failureRateThreshold(),
        config.failureRateThreshold());
    assertEquals(
        CircuitBreakerConfiguration.DEFAULT.slidingWindowSize(), config.slidingWindowSize());
    assertEquals(CircuitBreakerConfiguration.DEFAULT.waitDurationMs(), config.waitDurationMs());
    assertEquals(
        CircuitBreakerConfiguration.DEFAULT.slowCallThresholdMs(),
        config.slowCallThresholdMs());
    assertEquals(
        CircuitBreakerConfiguration.DEFAULT.permittedCallsInHalfOpen(),
        config.permittedCallsInHalfOpen());
    assertEquals(CircuitBreakerConfiguration.DEFAULT.minimumCalls(), config.minimumCalls());
  }

  private static final class TestCircuitBreakerConfigProvider
      implements CircuitBreakerConfigProvider {

    private final boolean enabled;
    private CircuitBreakerConfig config = new CircuitBreakerConfig(50.0f, 4, 100L, 5000L, 2, 2);

    private TestCircuitBreakerConfigProvider(boolean enabled) {
      this.enabled = enabled;
    }

    @Override
    public boolean isEnabled() {
      return enabled;
    }

    @Override
    public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
      return config;
    }
  }
}

package run.ratchet.ri.resilience;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;

class DefaultResilienceStrategyTest {

  private CircuitBreakerRegistry registry;
  private DefaultResilienceStrategy strategy;

  @BeforeEach
  void setUp() {
    registry = defaultRegistry();
    strategy = new DefaultResilienceStrategy(registry);
  }

  @Test
  void executePassesThroughWhenClosed() throws Exception {
    String result = strategy.execute("my-service", () -> "hello");
    assertEquals("hello", result);
    assertTrue(strategy.isServiceAvailable("my-service"));
    assertEquals(Duration.ZERO, strategy.getRetryDelay("my-service"));
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
        CircuitBreakerOpenException.class,
        () -> strategy.execute("failing-service", () -> "should not run"));
    assertFalse(strategy.isServiceAvailable("failing-service"));
  }

  @Test
  void getRetryDelayReturnsBreakerWaitDurationAfterCircuitOpens() {
    for (int i = 0; i < 10; i++) {
      try {
        strategy.execute(
            "retry-delay-service",
            () -> {
              throw new RuntimeException("fail");
            });
      } catch (Exception ignored) {
        // Drive the breaker open.
      }
    }

    Duration retryDelay = strategy.getRetryDelay("retry-delay-service");
    assertFalse(retryDelay.isNegative());
    assertTrue(
        retryDelay.compareTo(
                Duration.ofMillis(registry.getBreaker("retry-delay-service").getWaitDurationMs()))
            <= 0);
    assertFalse(strategy.isServiceAvailable("retry-delay-service"));
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
    provider.config = new CircuitBreakerConfig(75.0f, 7, 1234L, 2, 3);

    CircuitBreakerRegistry registry = new CircuitBreakerRegistry(provider);

    assertEquals(1234L, registry.getBreaker("custom-service").getWaitDurationMs());
  }

  @Test
  void defaultCircuitBreakerConfigUsesRatchetOptions() {
    RatchetOptions options =
        RatchetOptions.builder()
            .circuitBreaker(
                circuitBreaker ->
                    circuitBreaker.profile(
                        CircuitBreakerProfile.DEFAULT,
                        profile ->
                            profile
                                .failureRateThreshold(75.0f)
                                .slidingWindowSize(7)
                                .waitDurationMs(1234L)
                                .permittedCallsInHalfOpen(2)
                                .minimumCalls(3)))
            .build();
    DefaultCircuitBreakerConfigProvider provider = new DefaultCircuitBreakerConfigProvider(options);
    CircuitBreakerConfig config = provider.configFor(CircuitBreakerProfile.DEFAULT);

    assertEquals(75.0f, config.failureRateThreshold());
    assertEquals(7, config.slidingWindowSize());
    assertEquals(1234L, config.waitDurationMs());
    assertEquals(2, config.permittedCallsInHalfOpen());
    assertEquals(3, config.minimumCalls());
  }

  @Test
  void
      defaultCircuitBreakerConfigProviderProtectedConstructorFailsClearlyWhenUsedWithoutInjection() {
    DefaultCircuitBreakerConfigProvider provider = new DefaultCircuitBreakerConfigProvider();

    IllegalStateException thrown = assertThrows(IllegalStateException.class, provider::isEnabled);

    assertEquals("RatchetOptions were not injected", thrown.getMessage());
  }

  private static final class TestCircuitBreakerConfigProvider
      implements CircuitBreakerConfigProvider {

    private final boolean enabled;
    private CircuitBreakerConfig config = new CircuitBreakerConfig(50.0f, 4, 100L, 2, 2);

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

  private static CircuitBreakerRegistry defaultRegistry() {
    return new CircuitBreakerRegistry(
        new DefaultCircuitBreakerConfigProvider(RatchetOptions.defaults()));
  }
}

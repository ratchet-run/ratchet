package run.ratchet.ri.resilience;

import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.spi.CircuitBreakerConfig;

/**
 * Configuration for a circuit breaker instance.
 *
 * @param failureRateThreshold percentage (0-100) of failures that triggers OPEN state
 * @param slidingWindowSize number of recent calls tracked for failure rate calculation
 * @param waitDurationMs time in milliseconds to stay in OPEN state before transitioning to
 *     HALF_OPEN
 * @param permittedCallsInHalfOpen number of trial calls allowed in HALF_OPEN state
 * @param minimumCalls minimum calls before evaluating failure rate
 */
public record CircuitBreakerConfiguration(
    float failureRateThreshold,
    int slidingWindowSize,
    long waitDurationMs,
    int permittedCallsInHalfOpen,
    int minimumCalls) {

  public CircuitBreakerConfiguration {
    if (failureRateThreshold < 0.0f || failureRateThreshold > 100.0f) {
      throw new IllegalArgumentException("failureRateThreshold must be between 0 and 100");
    }
    if (slidingWindowSize <= 0) {
      throw new IllegalArgumentException("slidingWindowSize must be greater than zero");
    }
    if (waitDurationMs < 0) {
      throw new IllegalArgumentException("waitDurationMs must not be negative");
    }
    if (permittedCallsInHalfOpen <= 0) {
      throw new IllegalArgumentException("permittedCallsInHalfOpen must be greater than zero");
    }
    if (minimumCalls <= 0) {
      throw new IllegalArgumentException("minimumCalls must be greater than zero");
    }
  }

  public static final CircuitBreakerConfiguration DEFAULT =
      new CircuitBreakerConfiguration(50.0f, 100, 30_000L, 3, 5);

  public static final CircuitBreakerConfiguration FAST =
      new CircuitBreakerConfiguration(50.0f, 20, 10_000L, 2, 3);

  public static final CircuitBreakerConfiguration CRITICAL =
      new CircuitBreakerConfiguration(75.0f, 200, 60_000L, 5, 10);

  public static final CircuitBreakerConfiguration EXTERNAL_API =
      new CircuitBreakerConfiguration(60.0f, 50, 60_000L, 3, 5);

  public static final CircuitBreakerConfiguration CLAIM_PATH =
      new CircuitBreakerConfiguration(50.0f, 20, 5_000L, 1, 5);

  public static CircuitBreakerConfiguration forProfile(CircuitBreakerProfile profile) {
    return switch (profile) {
      case FAST -> FAST;
      case CRITICAL -> CRITICAL;
      case EXTERNAL_API -> EXTERNAL_API;
      case CLAIM_PATH -> CLAIM_PATH;
      default -> DEFAULT;
    };
  }

  public static CircuitBreakerConfiguration fromSpi(CircuitBreakerConfig config) {
    return new CircuitBreakerConfiguration(
        config.failureRateThreshold(),
        config.slidingWindowSize(),
        config.waitDurationMs(),
        config.permittedCallsInHalfOpen(),
        config.minimumCalls());
  }
}

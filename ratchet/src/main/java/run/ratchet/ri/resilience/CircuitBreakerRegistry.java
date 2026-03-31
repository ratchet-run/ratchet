package run.ratchet.ri.resilience;

import run.ratchet.api.CircuitBreakerProfile;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Registry for managing circuit breaker instances by service name.
 *
 * <p>Circuit breakers are created lazily on first request and cached. Service names must come from
 * a bounded, static vocabulary (e.g., class names, annotation values) — using dynamic names like
 * tenant IDs will cause unbounded memory growth.
 */
@ApplicationScoped
public class CircuitBreakerRegistry {

  private static final Logger log = Logger.getLogger(CircuitBreakerRegistry.class.getName());

  private final Map<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();
  private final Map<String, CircuitBreakerConfiguration> configs = new ConcurrentHashMap<>();

  public CircuitBreakerRegistry() {
    registerDefaultConfigs();
  }

  /**
   * Gets or creates a circuit breaker with the default profile.
   *
   * <p>Delegates to {@link #getBreaker(String, CircuitBreakerProfile)} with {@link
   * CircuitBreakerProfile#DEFAULT} so that callers using the no-profile overload share the same
   * breaker instance as CDI interceptors that explicitly specify the default profile.
   */
  public CircuitBreaker getBreaker(String serviceName) {
    return getBreaker(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  /** Gets or creates a circuit breaker for a specific profile. */
  public CircuitBreaker getBreaker(String serviceName, CircuitBreakerProfile profile) {
    String key = serviceName + ":" + profile.name();
    return breakers.computeIfAbsent(
        key,
        k -> {
          String configKey = profile.name().toLowerCase().replace('_', '-');
          CircuitBreakerConfiguration config =
              configs.getOrDefault(configKey, CircuitBreakerConfiguration.forProfile(profile));
          return createBreaker(serviceName, config);
        });
  }

  /** Gets the current state of a circuit breaker, or null if not created. */
  public CircuitBreaker.State getBreakerState(String serviceName) {
    return getBreakerState(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  /** Gets the current state of a circuit breaker for a specific profile, or null if not created. */
  public CircuitBreaker.State getBreakerState(String serviceName, CircuitBreakerProfile profile) {
    CircuitBreaker breaker = breakers.get(serviceName + ":" + profile.name());
    return breaker != null ? breaker.getState() : null;
  }

  /** Manually opens a circuit breaker (default profile). */
  public void openBreaker(String serviceName) {
    openBreaker(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  /** Manually opens a circuit breaker for a specific profile. */
  public void openBreaker(String serviceName, CircuitBreakerProfile profile) {
    CircuitBreaker breaker = breakers.get(serviceName + ":" + profile.name());
    if (breaker != null) {
      breaker.transitionToOpen();
      log.warning("Manually opened circuit breaker for service: " + serviceName);
    }
  }

  /** Resets a circuit breaker to CLOSED (default profile). */
  public void resetBreaker(String serviceName) {
    resetBreaker(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  /** Resets a circuit breaker to CLOSED for a specific profile. */
  public void resetBreaker(String serviceName, CircuitBreakerProfile profile) {
    CircuitBreaker breaker = breakers.get(serviceName + ":" + profile.name());
    if (breaker != null) {
      breaker.reset();
      log.info("Reset circuit breaker for service: " + serviceName);
    }
  }

  /** Registers a named configuration for future breaker creation. */
  public void registerConfig(String name, CircuitBreakerConfiguration config) {
    configs.put(name, config);
    log.fine("Registered circuit breaker config: " + name);
  }

  private CircuitBreaker createBreaker(String serviceName, CircuitBreakerConfiguration config) {
    CircuitBreaker breaker = new CircuitBreaker(serviceName, config);
    log.fine("Created circuit breaker for service: " + serviceName);
    // Metrics for circuit breaker creation tracked via JUL logging
    // MetricsCollector SPI is job-oriented; circuit breaker metrics are internal
    return breaker;
  }

  private void registerDefaultConfigs() {
    for (CircuitBreakerProfile profile : CircuitBreakerProfile.values()) {
      configs.put(
          profile.name().toLowerCase().replace('_', '-'),
          CircuitBreakerConfiguration.forProfile(profile));
    }
  }
}

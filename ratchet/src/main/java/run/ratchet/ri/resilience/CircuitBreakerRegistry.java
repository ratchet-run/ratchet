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

  /** Gets or creates a circuit breaker with default configuration. */
  public CircuitBreaker getBreaker(String serviceName) {
    return breakers.computeIfAbsent(serviceName, this::createBreaker);
  }

  /** Gets or creates a circuit breaker for a specific profile. */
  public CircuitBreaker getBreaker(String serviceName, CircuitBreakerProfile profile) {
    String key = serviceName + ":" + profile.name();
    return breakers.computeIfAbsent(
        key, k -> createBreaker(serviceName, CircuitBreakerConfiguration.forProfile(profile)));
  }

  /** Gets the current state of a circuit breaker, or null if not created. */
  public CircuitBreaker.State getBreakerState(String serviceName) {
    CircuitBreaker breaker = breakers.get(serviceName);
    return breaker != null ? breaker.getState() : null;
  }

  /** Manually opens a circuit breaker. */
  public void openBreaker(String serviceName) {
    CircuitBreaker breaker = breakers.get(serviceName);
    if (breaker != null) {
      breaker.transitionToOpen();
      log.warning("Manually opened circuit breaker for service: " + serviceName);
    }
  }

  /** Resets a circuit breaker to CLOSED. */
  public void resetBreaker(String serviceName) {
    CircuitBreaker breaker = breakers.get(serviceName);
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

  private CircuitBreaker createBreaker(String serviceName) {
    CircuitBreakerConfiguration config =
        configs.getOrDefault(serviceName, CircuitBreakerConfiguration.DEFAULT);
    return createBreaker(serviceName, config);
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

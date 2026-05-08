package run.ratchet.ri.resilience;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.CircuitBreakerConfigProvider;

/**
 * Registry for managing circuit breaker instances by service name.
 *
 * <p>Circuit breakers are created lazily on first request and cached. Service names must come from
 * a bounded, static vocabulary (e.g., class names, annotation values) — using dynamic names like
 * tenant IDs will cause unbounded memory growth.
 */
@ApplicationScoped
public class CircuitBreakerRegistry {

  private static final Logger log = Logger.getLogger(CircuitBreakerRegistry.class);

  private final Map<CircuitBreakerKey, CircuitBreaker> breakers = new ConcurrentHashMap<>();
  private final Map<String, CircuitBreakerConfiguration> configs = new ConcurrentHashMap<>();
  private final CircuitBreakerConfigProvider configProvider;

  public CircuitBreakerRegistry() {
    this(new DefaultCircuitBreakerConfigProvider(RatchetOptions.defaults()));
  }

  @Inject
  public CircuitBreakerRegistry(CircuitBreakerConfigProvider configProvider) {
    this.configProvider = configProvider;
    registerDefaultConfigs();
  }

  public CircuitBreaker getBreaker(String serviceName) {
    return getBreaker(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  public CircuitBreaker getBreaker(String serviceName, CircuitBreakerProfile profile) {
    CircuitBreakerKey key = new CircuitBreakerKey(serviceName, profile);
    return breakers.computeIfAbsent(
        key,
        k -> {
          String configKey = profile.name().toLowerCase().replace('_', '-');
          CircuitBreakerConfiguration config =
              configs.getOrDefault(
                  configKey,
                  CircuitBreakerConfiguration.fromSpi(configProvider.configFor(profile)));
          return createBreaker(serviceName, config);
        });
  }

  public CircuitBreaker.State getBreakerState(String serviceName) {
    return getBreakerState(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  public CircuitBreaker.State getBreakerState(String serviceName, CircuitBreakerProfile profile) {
    CircuitBreaker breaker = breakers.get(new CircuitBreakerKey(serviceName, profile));
    return breaker != null ? breaker.getState() : null;
  }

  public void openBreaker(String serviceName) {
    openBreaker(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  public void openBreaker(String serviceName, CircuitBreakerProfile profile) {
    CircuitBreaker breaker = breakers.get(new CircuitBreakerKey(serviceName, profile));
    if (breaker != null) {
      breaker.transitionToOpen();
      log.warnf("Manually opened circuit breaker for service: %s", serviceName);
    }
  }

  public void resetBreaker(String serviceName) {
    resetBreaker(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  public void resetBreaker(String serviceName, CircuitBreakerProfile profile) {
    CircuitBreaker breaker = breakers.get(new CircuitBreakerKey(serviceName, profile));
    if (breaker != null) {
      breaker.reset();
      log.infof("Reset circuit breaker for service: %s", serviceName);
    }
  }

  public void registerConfig(String name, CircuitBreakerConfiguration config) {
    configs.put(name, config);
    log.debugf("Registered circuit breaker config: %s", name);
  }

  private CircuitBreaker createBreaker(String serviceName, CircuitBreakerConfiguration config) {
    CircuitBreaker breaker = new CircuitBreaker(serviceName, config);
    log.debugf("Created circuit breaker for service: %s", serviceName);
    return breaker;
  }

  private void registerDefaultConfigs() {
    for (CircuitBreakerProfile profile : CircuitBreakerProfile.values()) {
      configs.put(
          profile.name().toLowerCase().replace('_', '-'),
          CircuitBreakerConfiguration.fromSpi(configProvider.configFor(profile)));
    }
  }

  private record CircuitBreakerKey(String serviceName, CircuitBreakerProfile profile) {}
}

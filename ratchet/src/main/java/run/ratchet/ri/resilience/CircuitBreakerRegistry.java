/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.resilience;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.NoOpMetricsCollector;

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
  private final MetricsCollector metricsCollector;

  protected CircuitBreakerRegistry() {
    this.configProvider = null;
    this.metricsCollector = new NoOpMetricsCollector();
  }

  public CircuitBreakerRegistry(CircuitBreakerConfigProvider configProvider) {
    this(configProvider, new NoOpMetricsCollector());
  }

  @Inject
  public CircuitBreakerRegistry(
      CircuitBreakerConfigProvider configProvider, MetricsCollector metricsCollector) {
    this.configProvider = configProvider;
    this.metricsCollector = metricsCollector;
    registerDefaultConfigs();
  }

  public CircuitBreaker getBreaker(String serviceName) {
    return getBreaker(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  public CircuitBreaker getBreaker(String serviceName, CircuitBreakerProfile profile) {
    CircuitBreakerKey key = new CircuitBreakerKey(serviceName, profile);
    String configKey = profile.name().toLowerCase().replace('_', '-');
    CircuitBreakerConfiguration config =
        configs.getOrDefault(
            configKey, CircuitBreakerConfiguration.fromSpi(configProvider.configFor(profile)));
    return breakers.computeIfAbsent(key, k -> createBreaker(k.serviceName(), k.profile(), config));
  }

  public CircuitBreaker.State getBreakerState(String serviceName) {
    return getBreakerState(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  public CircuitBreaker.State getBreakerState(String serviceName, CircuitBreakerProfile profile) {
    return getBreaker(serviceName, profile).getState();
  }

  public void openBreaker(String serviceName) {
    openBreaker(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  public void openBreaker(String serviceName, CircuitBreakerProfile profile) {
    CircuitBreaker breaker = getBreaker(serviceName, profile);
    breaker.transitionToOpen();
    log.warnf("Manually opened circuit breaker for service: %s", serviceName);
  }

  public void resetBreaker(String serviceName) {
    resetBreaker(serviceName, CircuitBreakerProfile.DEFAULT);
  }

  /**
   * Resets the breaker for {@code serviceName} and {@code profile}, if it exists.
   *
   * <p>This is an administrative reset. It forces the breaker directly to CLOSED and clears the
   * sliding window, skipping HALF_OPEN trial calls.
   */
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

  private CircuitBreaker createBreaker(
      String serviceName, CircuitBreakerProfile profile, CircuitBreakerConfiguration config) {
    CircuitBreaker breaker =
        new CircuitBreaker(serviceName, config, state -> reportState(serviceName, profile, state));
    reportState(serviceName, profile, CircuitBreaker.State.CLOSED);
    log.debugf("Created circuit breaker for service: %s", serviceName);
    return breaker;
  }

  private void reportState(
      String serviceName, CircuitBreakerProfile profile, CircuitBreaker.State state) {
    try {
      metricsCollector.circuitBreakerState(serviceName, profile.name(), state.name());
    } catch (RuntimeException e) {
      log.debugf(
          e, "Metrics collector rejected circuit breaker state for service: %s", serviceName);
    }
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

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

import java.time.Duration;
import java.util.concurrent.Callable;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.CircuitBreakerManager;
import run.ratchet.spi.ResilienceStrategy;

/**
 * Default {@link ResilienceStrategy} using the built-in {@link CircuitBreaker}; produced by {@code
 * RatchetProducer} (no CDI annotations here so it can be overridden).
 */
public class DefaultResilienceStrategy implements ResilienceStrategy {

  private static final Logger log = Logger.getLogger(DefaultResilienceStrategy.class);

  private final CircuitBreakerManager registry;
  private final CircuitBreakerConfigProvider configProvider;

  public DefaultResilienceStrategy(CircuitBreakerRegistry registry) {
    this(registry, new DefaultCircuitBreakerConfigProvider(RatchetOptions.defaults()));
  }

  public DefaultResilienceStrategy(
      CircuitBreakerRegistry registry, CircuitBreakerConfigProvider configProvider) {
    this((CircuitBreakerManager) registry, configProvider);
  }

  public DefaultResilienceStrategy(
      CircuitBreakerManager registry, CircuitBreakerConfigProvider configProvider) {
    this.registry = registry;
    this.configProvider = configProvider;
  }

  @Override
  public <T> T execute(String serviceName, Callable<T> task) throws Exception {
    if (!configProvider.isEnabled()) {
      return task.call();
    }
    try {
      return registry.getBreaker(serviceName).execute(task);
    } catch (CircuitBreakerOpenException e) {
      log.warnv("Circuit breaker OPEN for service: {0}", serviceName);
      throw e;
    }
  }

  @Override
  public boolean isServiceAvailable(String serviceName) {
    if (!configProvider.isEnabled()) {
      return true;
    }
    return !CircuitBreaker.State.OPEN.name().equals(registry.getBreaker(serviceName).stateName());
  }

  @Override
  public Duration getRetryDelay(String serviceName) {
    if (!configProvider.isEnabled()) {
      return Duration.ZERO;
    }
    return Duration.ofMillis(registry.getBreaker(serviceName).getRemainingWaitDurationMs());
  }
}

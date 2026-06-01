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
import java.util.Objects;
import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;

/** Reads built-in circuit breaker configuration from CDI-provided Ratchet options. */
@ApplicationScoped
public class DefaultCircuitBreakerConfigProvider implements CircuitBreakerConfigProvider {

  private final RatchetOptions options;

  protected DefaultCircuitBreakerConfigProvider() {
    this.options = null;
  }

  @Inject
  public DefaultCircuitBreakerConfigProvider(RatchetOptions options) {
    this.options = Objects.requireNonNull(options, "options must not be null");
  }

  @Override
  public boolean isEnabled() {
    return options().circuitBreaker().enabled();
  }

  @Override
  public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
    CircuitBreakerConfiguration defaults = CircuitBreakerConfiguration.forProfile(profile);
    RatchetOptions.CircuitBreakerProfileOptions profileOptions =
        options().circuitBreaker().profile(profile);
    if (profileOptions == null) {
      return new CircuitBreakerConfig(
          defaults.failureRateThreshold(),
          defaults.slidingWindowSize(),
          defaults.waitDurationMs(),
          defaults.permittedCallsInHalfOpen(),
          defaults.minimumCalls());
    }

    return new CircuitBreakerConfig(
        profileOptions.failureRateThreshold(),
        profileOptions.slidingWindowSize(),
        profileOptions.waitDurationMs(),
        profileOptions.permittedCallsInHalfOpen(),
        profileOptions.minimumCalls());
  }

  private RatchetOptions options() {
    if (options == null) {
      throw new IllegalStateException("RatchetOptions were not injected");
    }
    return options;
  }
}

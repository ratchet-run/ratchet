package run.ratchet.spi;

import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.Incubating;

/** Supplies circuit breaker settings for the built-in resilience implementation and interceptor. */
@Incubating
public interface CircuitBreakerConfigProvider {

  boolean isEnabled();

  CircuitBreakerConfig configFor(CircuitBreakerProfile profile);
}

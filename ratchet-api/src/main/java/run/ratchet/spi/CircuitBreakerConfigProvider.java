package run.ratchet.spi;

import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.Incubating;

/** Supplies circuit breaker settings for the built-in resilience implementation and interceptor. */
@Incubating
public interface CircuitBreakerConfigProvider {

  /**
   * Returns whether the built-in circuit breaker integration is enabled.
   *
   * <p>When this returns {@code false}, callers should bypass {@link
   * #configFor(CircuitBreakerProfile)} and execute without circuit breaker protection.
   *
   * @return {@code true} when circuit breaker protection should be applied
   */
  boolean isEnabled();

  /**
   * Returns the configuration for a circuit breaker profile.
   *
   * @param profile profile requested by the scheduled job; never {@code null}
   * @return non-null circuit breaker configuration for {@code profile}
   * @throws NullPointerException if {@code profile} is {@code null}
   * @throws IllegalStateException if the provider cannot supply a configuration for the profile
   */
  CircuitBreakerConfig configFor(CircuitBreakerProfile profile);
}

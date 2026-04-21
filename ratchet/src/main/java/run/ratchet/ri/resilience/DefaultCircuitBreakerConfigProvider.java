package run.ratchet.ri.resilience;

import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Reads built-in circuit breaker configuration from CDI-provided Ratchet options. */
@ApplicationScoped
public class DefaultCircuitBreakerConfigProvider implements CircuitBreakerConfigProvider {

  private final RatchetOptions options;

  protected DefaultCircuitBreakerConfigProvider() {
    this.options = null;
  }

  @Inject
  public DefaultCircuitBreakerConfigProvider(RatchetOptions options) {
    this.options = options;
  }

  @Override
  public boolean isEnabled() {
    return options.circuitBreaker().enabled();
  }

  @Override
  public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
    CircuitBreakerConfiguration defaults = CircuitBreakerConfiguration.forProfile(profile);
    RatchetOptions.CircuitBreakerProfileOptions profileOptions =
        options.circuitBreaker().profile(profile);
    if (profileOptions == null) {
      return new CircuitBreakerConfig(
          defaults.failureRateThreshold(),
          defaults.slidingWindowSize(),
          defaults.waitDurationMs(),
          defaults.slowCallThresholdMs(),
          defaults.permittedCallsInHalfOpen(),
          defaults.minimumCalls());
    }

    return new CircuitBreakerConfig(
        profileOptions.failureRateThreshold(),
        profileOptions.slidingWindowSize(),
        profileOptions.waitDurationMs(),
        profileOptions.slowCallThresholdMs(),
        profileOptions.permittedCallsInHalfOpen(),
        profileOptions.minimumCalls());
  }
}

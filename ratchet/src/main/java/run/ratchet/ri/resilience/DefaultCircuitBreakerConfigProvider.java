package run.ratchet.ri.resilience;

import run.ratchet.api.CircuitBreakerProfile;
import run.ratchet.spi.CircuitBreakerConfig;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.RatchetConfig;
import run.ratchet.spi.RatchetConfigKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Reads built-in circuit breaker configuration through the typed Ratchet config facade. */
@ApplicationScoped
public class DefaultCircuitBreakerConfigProvider implements CircuitBreakerConfigProvider {

  private static final RatchetConfigKey<Boolean> ENABLED =
      RatchetConfigKey.bool(
          "ratchet.circuit-breaker.enabled",
          "RATCHET_CIRCUIT_BREAKER_ENABLED",
          "scheduler.circuit-breaker.enabled",
          "SCHEDULER_CIRCUIT_BREAKER_ENABLED",
          true);

  private final RatchetConfig config;

  protected DefaultCircuitBreakerConfigProvider() {
    this.config = null;
  }

  @Inject
  public DefaultCircuitBreakerConfigProvider(RatchetConfig config) {
    this.config = config;
  }

  @Override
  public boolean isEnabled() {
    return config.get(ENABLED);
  }

  @Override
  public CircuitBreakerConfig configFor(CircuitBreakerProfile profile) {
    CircuitBreakerConfiguration defaults = CircuitBreakerConfiguration.forProfile(profile);
    String suffix = profile.name();
    String environmentSuffix = profile == CircuitBreakerProfile.EXTERNAL_API ? "EXTERNAL" : suffix;
    String propertySuffix = profile.name().toLowerCase().replace('_', '-');

    float failureRate =
        config.get(
            RatchetConfigKey.floatingRange(
                "ratchet.circuit-breaker." + propertySuffix + ".failure-rate",
                "RATCHET_CB_" + environmentSuffix + "_FAILURE_RATE",
                "scheduler.circuit-breaker." + propertySuffix + ".failure-rate",
                "SCHEDULER_CB_" + environmentSuffix + "_FAILURE_RATE",
                defaults.failureRateThreshold(),
                0.0f,
                100.0f));
    int windowSize =
        config.get(
            RatchetConfigKey.integerAtLeast(
                "ratchet.circuit-breaker." + propertySuffix + ".window-size",
                "RATCHET_CB_" + environmentSuffix + "_WINDOW_SIZE",
                "scheduler.circuit-breaker." + propertySuffix + ".window-size",
                "SCHEDULER_CB_" + environmentSuffix + "_WINDOW_SIZE",
                defaults.slidingWindowSize(),
                1));
    RatchetConfigKey<Long> waitMsKey =
        RatchetConfigKey.longAtLeast(
            "ratchet.circuit-breaker." + propertySuffix + ".wait-ms",
            "RATCHET_CB_" + environmentSuffix + "_WAIT_MS",
            "scheduler.circuit-breaker." + propertySuffix + ".wait-ms",
            "SCHEDULER_CB_" + environmentSuffix + "_WAIT_MS",
            defaults.waitDurationMs(),
            0L);
    RatchetConfigKey<Long> waitSecondsKey =
        RatchetConfigKey.longAtLeast(
            "ratchet.circuit-breaker." + propertySuffix + ".wait-seconds",
            "RATCHET_CB_" + environmentSuffix + "_WAIT_SECONDS",
            "scheduler.circuit-breaker." + propertySuffix + ".wait-seconds",
            "SCHEDULER_CB_" + environmentSuffix + "_WAIT_SECONDS",
            defaults.waitDurationMs() / 1000L,
            0L);
    long waitDurationMs =
        config.raw(waitSecondsKey).isPresent()
            ? config.get(waitSecondsKey) * 1000L
            : config.get(waitMsKey);
    long slowCallMs =
        config.get(
            RatchetConfigKey.longAtLeast(
                "ratchet.circuit-breaker." + propertySuffix + ".slow-call-ms",
                "RATCHET_CB_" + environmentSuffix + "_SLOW_CALL_MS",
                "scheduler.circuit-breaker." + propertySuffix + ".slow-call-ms",
                "SCHEDULER_CB_" + environmentSuffix + "_SLOW_CALL_MS",
                defaults.slowCallThresholdMs(),
                0L));
    int halfOpenCalls =
        config.get(
            RatchetConfigKey.integerAtLeast(
                "ratchet.circuit-breaker." + propertySuffix + ".half-open-calls",
                "RATCHET_CB_" + environmentSuffix + "_HALF_OPEN_CALLS",
                "scheduler.circuit-breaker." + propertySuffix + ".half-open-calls",
                "SCHEDULER_CB_" + environmentSuffix + "_HALF_OPEN_CALLS",
                defaults.permittedCallsInHalfOpen(),
                1));
    int minimumCalls =
        config.get(
            RatchetConfigKey.integerAtLeast(
                "ratchet.circuit-breaker." + propertySuffix + ".minimum-calls",
                "RATCHET_CB_" + environmentSuffix + "_MIN_CALLS",
                "scheduler.circuit-breaker." + propertySuffix + ".minimum-calls",
                "SCHEDULER_CB_" + environmentSuffix + "_MIN_CALLS",
                defaults.minimumCalls(),
                1));

    return new CircuitBreakerConfig(
        failureRate, windowSize, waitDurationMs, slowCallMs, halfOpenCalls, minimumCalls);
  }
}

package run.ratchet.ri.resilience;

import java.time.Duration;
import java.util.concurrent.Callable;
import org.jboss.logging.Logger;
import run.ratchet.api.RatchetOptions;
import run.ratchet.spi.CircuitBreakerConfigProvider;
import run.ratchet.spi.ResilienceStrategy;

/**
 * Default {@link ResilienceStrategy} using the built-in {@link CircuitBreaker}; produced by {@code
 * RatchetProducer} (no CDI annotations here so it can be overridden).
 */
public class DefaultResilienceStrategy implements ResilienceStrategy {

  private static final Logger log = Logger.getLogger(DefaultResilienceStrategy.class);

  private final CircuitBreakerRegistry registry;
  private final CircuitBreakerConfigProvider configProvider;

  public DefaultResilienceStrategy(CircuitBreakerRegistry registry) {
    this(registry, new DefaultCircuitBreakerConfigProvider(RatchetOptions.defaults()));
  }

  public DefaultResilienceStrategy(
      CircuitBreakerRegistry registry, CircuitBreakerConfigProvider configProvider) {
    this.registry = registry;
    this.configProvider = configProvider;
  }

  @Override
  public <T> T execute(String serviceName, Callable<T> task) throws Exception {
    if (!configProvider.isEnabled()) {
      return task.call();
    }
    CircuitBreaker breaker = registry.getBreaker(serviceName);
    try {
      return breaker.execute(task);
    } catch (ServiceUnavailableException e) {
      log.warnv("Circuit breaker OPEN for service: {0}", serviceName);
      throw e;
    }
  }

  @Override
  public boolean isServiceAvailable(String serviceName) {
    if (!configProvider.isEnabled()) {
      return true;
    }
    CircuitBreaker.State state = registry.getBreakerState(serviceName);
    return state != CircuitBreaker.State.OPEN;
  }

  @Override
  public Duration getRetryDelay(String serviceName) {
    if (!configProvider.isEnabled()) {
      return Duration.ZERO;
    }
    return Duration.ofMillis(registry.getBreaker(serviceName).getWaitDurationMs());
  }
}

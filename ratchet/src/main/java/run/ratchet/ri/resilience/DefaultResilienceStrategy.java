package run.ratchet.ri.resilience;

import run.ratchet.spi.ResilienceStrategy;
import java.time.Duration;
import java.util.concurrent.Callable;
import org.jboss.logging.Logger;

/**
 * Default {@link ResilienceStrategy} using the built-in {@link CircuitBreaker}; produced by {@code
 * RatchetProducer} (no CDI annotations here so it can be overridden).
 */
public class DefaultResilienceStrategy implements ResilienceStrategy {

  private static final Logger log = Logger.getLogger(DefaultResilienceStrategy.class);

  private final CircuitBreakerRegistry registry;

  public DefaultResilienceStrategy(CircuitBreakerRegistry registry) {
    this.registry = registry;
  }

  @Override
  public <T> T execute(String serviceName, Callable<T> task) throws Exception {
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
    CircuitBreaker.State state = registry.getBreakerState(serviceName);
    return state != CircuitBreaker.State.OPEN;
  }

  @Override
  public Duration getRetryDelay(String serviceName) {
    return Duration.ofMillis(registry.getBreaker(serviceName).getWaitDurationMs());
  }
}

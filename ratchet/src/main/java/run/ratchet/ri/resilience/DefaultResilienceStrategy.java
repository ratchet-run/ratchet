package run.ratchet.ri.resilience;

import run.ratchet.spi.ResilienceStrategy;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default {@link ResilienceStrategy} implementation using the built-in {@link CircuitBreaker}.
 *
 * <p>Delegates to the {@link CircuitBreakerRegistry} for circuit breaker management. If the circuit
 * is OPEN, throws {@link ServiceUnavailableException} without executing the task.
 */
public class DefaultResilienceStrategy implements ResilienceStrategy {

  private static final Logger log = Logger.getLogger(DefaultResilienceStrategy.class.getName());

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
      log.log(Level.WARNING, "Circuit breaker OPEN for service: {0}", serviceName);
      throw e;
    }
  }

  @Override
  public boolean isServiceAvailable(String serviceName) {
    CircuitBreaker.State state = registry.getBreakerState(serviceName);
    return state == null || state != CircuitBreaker.State.OPEN;
  }
}

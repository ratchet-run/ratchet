package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.util.concurrent.Callable;

/**
 * Strategy for wrapping job execution with resilience patterns such as circuit breakers.
 *
 * <p>Implementations may provide circuit breaker protection, bulkhead isolation, or other
 * resilience mechanisms. The default RI implementation provides a lightweight count-based circuit
 * breaker. Users may plug in Resilience4j or MicroProfile Fault Tolerance implementations via this
 * SPI.
 */
@Incubating
public interface ResilienceStrategy {

  /**
   * Executes the given task with resilience protection.
   *
   * @param serviceName identifies the service being protected (must be from a bounded vocabulary)
   * @param task the callable to execute
   * @param <T> the return type
   * @return the result of the task
   * @throws Exception if the task fails or the service is unavailable
   */
  <T> T execute(String serviceName, Callable<T> task) throws Exception;

  /**
   * Checks whether a service is currently available (circuit not open).
   *
   * @param serviceName the service to check
   * @return true if calls are permitted, false if the circuit is open
   */
  boolean isServiceAvailable(String serviceName);
}

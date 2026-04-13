package run.ratchet.spi;

import run.ratchet.api.Incubating;
import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * Strategy for wrapping job execution with resilience patterns such as circuit breakers. The
 * default RI implementation provides a lightweight count-based circuit breaker; Resilience4j or
 * MicroProfile Fault Tolerance implementations may be plugged in via this SPI.
 */
@Incubating
public interface ResilienceStrategy {

  /**
   * Executes the given task with resilience protection.
   *
   * @param serviceName identifies the service being protected (must be from a bounded vocabulary)
   * @param <T> the return type
   * @throws Exception if the task fails or the service is unavailable
   */
  <T> T execute(String serviceName, Callable<T> task) throws Exception;

  /**
   * Checks whether a service is currently available (circuit not open).
   *
   * @return true if calls are permitted, false if the circuit is open
   */
  boolean isServiceAvailable(String serviceName);

  /**
   * Returns the recommended delay before retrying work that was rejected because the protected
   * service is unavailable.
   *
   * <p>The default implementation preserves the existing RI behavior of 30 seconds so existing SPI
   * implementations remain source- and binary-compatible.
   *
   * @return the recommended retry delay
   */
  default Duration getRetryDelay(String serviceName) {
    return Duration.ofSeconds(30);
  }
}

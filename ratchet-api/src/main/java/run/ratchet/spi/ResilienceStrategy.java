package run.ratchet.spi;

import java.time.Duration;
import java.util.concurrent.Callable;
import run.ratchet.api.Incubating;
import run.ratchet.api.exception.CircuitBreakerOpenException;

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
   * @throws CircuitBreakerOpenException if the call is rejected because the circuit is open. This
   *     rejection is distinct from task failure; scheduler implementations should delay/reschedule
   *     the work without consuming a retry attempt.
   * @throws Exception if the task itself fails
   */
  <T> T execute(String serviceName, Callable<T> task) throws Exception;

  /**
   * Checks whether a service is currently available (circuit not open).
   *
   * <p>This method is an advisory pre-check only. Availability can change immediately after it
   * returns, so callers must still handle {@link CircuitBreakerOpenException} from {@link
   * #execute(String, Callable)}.
   *
   * @return true if calls are permitted, false if the circuit is open
   */
  boolean isServiceAvailable(String serviceName);

  /**
   * Returns the recommended delay before retrying work that was rejected with {@link
   * CircuitBreakerOpenException}.
   *
   * <p>Implementations must not return {@code null} or a negative duration. Return {@link
   * Duration#ZERO} when an immediate retry is appropriate. For open circuits, implementations
   * should return the remaining open-window duration when known, or a conservative retry delay when
   * it is not known.
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

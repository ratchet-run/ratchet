package run.ratchet.api.exception;

import java.io.Serial;

/**
 * Thrown when a resilience strategy rejects a protected call because the circuit is open.
 *
 * <p>This exception identifies scheduler back-pressure caused by an unavailable protected service.
 * It is distinct from task failure: callers that catch it should reschedule or delay the work
 * without consuming a job retry attempt.
 */
public class CircuitBreakerOpenException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public CircuitBreakerOpenException(String message) {
    super(message);
  }

  public CircuitBreakerOpenException(String message, Throwable cause) {
    super(message, cause);
  }
}

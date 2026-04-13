package run.ratchet.ri.resilience;

import java.io.Serial;

/** Thrown when a circuit breaker is OPEN and calls are not permitted. */
public class ServiceUnavailableException extends RuntimeException {

  @Serial private static final long serialVersionUID = -2077185964269635004L;

  public ServiceUnavailableException(String message) {
    super(message);
  }

  public ServiceUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}

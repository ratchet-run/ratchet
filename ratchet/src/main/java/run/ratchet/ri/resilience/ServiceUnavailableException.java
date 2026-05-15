package run.ratchet.ri.resilience;

import java.io.Serial;
import run.ratchet.api.exception.CircuitBreakerOpenException;

/**
 * RI compatibility alias for open-circuit rejections.
 *
 * @deprecated use {@link CircuitBreakerOpenException}
 */
@Deprecated(since = "1.0", forRemoval = false)
public class ServiceUnavailableException extends CircuitBreakerOpenException {

  @Serial private static final long serialVersionUID = -2077185964269635004L;

  public ServiceUnavailableException(String message) {
    super(message);
  }

  public ServiceUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}

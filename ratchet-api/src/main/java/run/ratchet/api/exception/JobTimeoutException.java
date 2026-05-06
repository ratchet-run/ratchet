package run.ratchet.api.exception;

import java.io.Serial;

/**
 * Thrown when a job exceeds its configured execution timeout.
 *
 * <p>Distinct from {@link InterruptedException} so that timeout-vs-interrupt failures can be
 * disambiguated by metrics, logs, and {@code shouldNotRetry} policies. Previously the runtime
 * inferred timeouts from {@code InterruptedException}, which conflated genuine cancellation,
 * cooperative shutdown, and watchdog timeouts.
 */
public class JobTimeoutException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public JobTimeoutException(String message) {
    super(message);
  }

  public JobTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}

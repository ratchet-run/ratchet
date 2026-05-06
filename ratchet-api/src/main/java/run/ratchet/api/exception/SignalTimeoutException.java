package run.ratchet.api.exception;

import java.io.Serial;

/** Thrown when a signal-waiting job exceeds its configured signal wait timeout. */
public class SignalTimeoutException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public SignalTimeoutException(String message) {
    super(message);
  }

  public SignalTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}

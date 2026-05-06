package run.ratchet.api.exception;

/** Thrown when a signal-waiting job exceeds its configured signal wait timeout. */
public class SignalTimeoutException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public SignalTimeoutException(String message) {
    super(message);
  }

  public SignalTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}

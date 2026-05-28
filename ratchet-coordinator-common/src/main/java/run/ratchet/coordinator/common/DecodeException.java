package run.ratchet.coordinator.common;

/** Thrown when a coordinator wakeup payload cannot be decoded. */
public final class DecodeException extends RuntimeException {

  public DecodeException(String message) {
    super(message);
  }

  public DecodeException(String message, Throwable cause) {
    super(message, cause);
  }
}

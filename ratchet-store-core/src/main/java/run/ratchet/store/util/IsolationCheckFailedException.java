package run.ratchet.store.util;

/** Thrown when SQL isolation validation is configured to fail and the isolation level is wrong. */
public class IsolationCheckFailedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public IsolationCheckFailedException(String message) {
    super(message);
  }
}

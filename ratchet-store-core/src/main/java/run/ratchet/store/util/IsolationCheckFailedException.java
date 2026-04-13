package run.ratchet.store.util;

/** Thrown when ratchet.isolation-check=fail and the DB isolation level is wrong. */
public class IsolationCheckFailedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public IsolationCheckFailedException(String message) {
    super(message);
  }
}

package run.ratchet.store.util;

import java.io.Serial;

/** Thrown when SQL isolation validation is configured to fail and the isolation level is wrong. */
public class IsolationCheckFailedException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public IsolationCheckFailedException(String message) {
    super(message);
  }
}

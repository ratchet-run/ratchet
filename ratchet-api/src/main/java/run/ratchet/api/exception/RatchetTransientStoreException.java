package run.ratchet.api.exception;

import java.io.Serial;

/**
 * Thrown when a transient store-level concurrency conflict occurs and the caller should retry the
 * persistence operation without treating the job itself as failed.
 */
public class RatchetTransientStoreException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public RatchetTransientStoreException(String message) {
    super(message);
  }

  public RatchetTransientStoreException(String message, Throwable cause) {
    super(message, cause);
  }
}

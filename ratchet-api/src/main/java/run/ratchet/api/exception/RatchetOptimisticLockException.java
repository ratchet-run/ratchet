package run.ratchet.api.exception;

import java.io.Serial;

/** Thrown on concurrent version mismatch. Not a PersistenceException to avoid JTA auto-rollback. */
public class RatchetOptimisticLockException extends RuntimeException {

  @Serial private static final long serialVersionUID = 1L;

  public RatchetOptimisticLockException(String message) {
    super(message);
  }

  public RatchetOptimisticLockException(String message, Throwable cause) {
    super(message, cause);
  }
}

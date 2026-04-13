package run.ratchet.api.exception;

/** Thrown on concurrent version mismatch. Not a PersistenceException to avoid JTA auto-rollback. */
public class RatchetOptimisticLockException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public RatchetOptimisticLockException(String message) {
    super(message);
  }

  public RatchetOptimisticLockException(String message, Throwable cause) {
    super(message, cause);
  }
}

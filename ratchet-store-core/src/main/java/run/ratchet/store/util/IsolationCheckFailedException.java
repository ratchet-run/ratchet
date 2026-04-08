package run.ratchet.store.util;

/**
 * Thrown by {@link IsolationCheck#verifyReadCommitted} when the database session isolation level
 * does not match the value Ratchet requires AND the {@code ratchet.isolation-check} system property
 * is set to {@code fail}.
 *
 * <p>Deliberately a plain {@link RuntimeException} (not {@code
 * jakarta.persistence.PersistenceException} or {@code DeploymentException}) so it does not trigger
 * JTA transaction rollback of any enclosing user transaction. The check runs at
 * {@code @PostConstruct} time, before any user transaction has begun, so rollback semantics should
 * not matter — but a defensive choice keeps the exception safe even if a future refactor moves the
 * check inside a transactional method.
 */
public class IsolationCheckFailedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public IsolationCheckFailedException(String message) {
    super(message);
  }
}

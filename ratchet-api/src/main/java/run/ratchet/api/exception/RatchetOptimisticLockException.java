package run.ratchet.api.exception;

/**
 * Thrown when a {@code JobCrudStore.save()} call detects a concurrent version mismatch on the
 * target row — i.e. another transaction mutated the job between the time the caller reloaded it and
 * the time the caller tried to write it back.
 *
 * <p>Deliberately a plain {@link RuntimeException} (NOT a {@code
 * jakarta.persistence.OptimisticLockException}) so that propagation through user code does not
 * extend the JPA rollback hierarchy. Callers on a JPA container can catch this type without their
 * enclosing {@code @Transactional} method being marked rollback-only by the container's exception
 * dispatch.
 *
 * <p><b>Rollback semantics differ between stores.</b> The MongoDB store throws this exception
 * without marking any enclosing JTA transaction for rollback. The MySQL and PostgreSQL stores
 * detect the conflict during {@code em.flush()} inside a JTA-managed {@code EntityManager}, and
 * Hibernate's JTA integration calls {@code Transaction.setRollbackOnly()} BEFORE the exception is
 * translated to this type. The translated exception type is consistent across stores, but the
 * rollback behavior is not. This is a JPA+JTA limitation, not a Ratchet defect. Workarounds for
 * code that needs JTA-isolated job submission include wrapping {@code JobScheduler.submit(...)} in
 * a {@code @Transactional(REQUIRES_NEW)} boundary, or moving to the Mongo store.
 *
 * <p><b>Not stable for serialization across versions.</b> Future minor versions may add fields
 * (e.g. {@code jobId} for observability). Do not persist instances of this exception across version
 * boundaries via Java serialization.
 */
public class RatchetOptimisticLockException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public RatchetOptimisticLockException(String message) {
    super(message);
  }

  public RatchetOptimisticLockException(String message, Throwable cause) {
    super(message, cause);
  }
}

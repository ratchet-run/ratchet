package run.ratchet.store;

/**
 * Detects database constraint violations in a vendor-neutral way.
 *
 * <p>Implementations parse database-specific error messages or codes to identify the type of
 * constraint violation that occurred. This enables the scheduler to handle constraint violations
 * (such as duplicate keys from idempotency checks) without coupling to a specific database vendor.
 */
public interface ConstraintDetector {

  /**
   * Returns the violated constraint name, or null if the exception is not a constraint violation.
   *
   * @param e the exception to inspect
   * @return the constraint name, or null
   */
  String constraintName(Exception e);

  /**
   * Returns true if the exception represents a duplicate key constraint violation.
   *
   * @param e the exception to inspect
   * @return true if duplicate key
   */
  boolean isDuplicateKey(Exception e);

  /**
   * Returns true if the exception represents a deadlock.
   *
   * @param e the exception to inspect
   * @return true if deadlock
   */
  boolean isDeadlock(Exception e);
}

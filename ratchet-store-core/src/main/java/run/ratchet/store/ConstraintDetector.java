package run.ratchet.store;

/**
 * Detects database constraint violations in a vendor-neutral way.
 *
 * <p>Implementations parse database-specific error messages or codes to identify the type of
 * constraint violation that occurred. This enables the scheduler to handle constraint violations
 * (such as duplicate keys from idempotency checks) without coupling to a specific database vendor.
 */
public interface ConstraintDetector {

  String constraintName(Exception e);

  boolean isDuplicateKey(Exception e);

  boolean isDeadlock(Exception e);
}

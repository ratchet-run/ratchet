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

  /**
   * Returns true if the exception was raised by a unique-constraint violation on the business key
   * reservation table.
   *
   * <p>SQL stores use {@code scheduler_business_key_reservation} as the authoritative owner table
   * for active business keys. The vendor-specific detectors still provide duplicate-key and
   * constraint-name parsing because those signals differ by database, but this composed
   * business-key check is shared.
   */
  default boolean isDuplicateBusinessKey(Exception e) {
    if (!isDuplicateKey(e)) {
      return false;
    }
    String name = constraintName(e);
    return name != null && name.contains("scheduler_business_key_reservation");
  }

  boolean isDeadlock(Exception e);

  boolean isTransientConnectionFailure(Exception e);
}

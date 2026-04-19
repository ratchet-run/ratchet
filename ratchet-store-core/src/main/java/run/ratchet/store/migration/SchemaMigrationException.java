package run.ratchet.store.migration;

/** Thrown when schema migration discovery or validation fails before SQL execution can continue. */
public class SchemaMigrationException extends RuntimeException {

  public SchemaMigrationException(String message) {
    super(message);
  }

  public SchemaMigrationException(String message, Throwable cause) {
    super(message, cause);
  }
}

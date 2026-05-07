package run.ratchet.store.migration;

/**
 * Thrown when schema initialization at scheduler startup cannot continue safely.
 *
 * <p>Distinct from {@link SchemaMigrationException}: this type signals a fatal startup-time
 * configuration problem (no {@code DataSource} bound, unsupported database product, legacy schema
 * detected without baseline marker) that {@code RatchetLifecycle} re-throws to halt deployment
 * rather than swallowing as a hook warning.
 */
public class SchemaInitializationException extends RuntimeException {

  public SchemaInitializationException(String message) {
    super(message);
  }

  public SchemaInitializationException(String message, Throwable cause) {
    super(message, cause);
  }
}

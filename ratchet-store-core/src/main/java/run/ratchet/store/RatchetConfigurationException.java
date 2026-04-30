package run.ratchet.store;

/**
 * Thrown at startup when a Ratchet configuration is structurally incompatible with the store
 * implementation — for example, a host-supplied {@code MongoClient} configured with a non-STANDARD
 * {@code UuidRepresentation} that would silently corrupt UUIDv7 IDs.
 */
public class RatchetConfigurationException extends RuntimeException {

  public RatchetConfigurationException(String message) {
    super(message);
  }

  public RatchetConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
}

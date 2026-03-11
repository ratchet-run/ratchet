package run.ratchet.spi;

/**
 * Marker interface for job persistence operations.
 *
 * <p>The full composed interface with all method signatures is {@link
 * run.ratchet.store.spi.JobStore} in the {@code ratchet-store-core} module. This marker
 * exists in {@code ratchet-api} so that higher-level API types can reference the store concept
 * without depending on JPA entity types.
 *
 * <p>Implementations must be thread-safe.
 */
public interface JobStore {}

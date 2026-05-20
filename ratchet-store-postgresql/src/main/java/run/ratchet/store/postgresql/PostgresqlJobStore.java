package run.ratchet.store.postgresql;

import run.ratchet.store.spi.JobStore;

/**
 * Public PostgreSQL-specific store type.
 *
 * <p>The CDI implementation is package-private; consumers can inject this interface without
 * exposing the concrete store as an extension point.
 */
public interface PostgresqlJobStore extends JobStore {}

package run.ratchet.store.mysql;

import run.ratchet.store.spi.JobStore;

/**
 * Public MySQL-specific store type.
 *
 * <p>The CDI implementation is package-private; consumers can inject this interface without
 * exposing the concrete store as an extension point.
 */
public interface MysqlJobStore extends JobStore {}

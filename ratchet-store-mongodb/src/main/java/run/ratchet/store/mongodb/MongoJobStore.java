package run.ratchet.store.mongodb;

import run.ratchet.store.spi.JobStore;

/**
 * Public MongoDB-specific store type.
 *
 * <p>The CDI implementation is package-private; consumers can inject this interface without
 * exposing the concrete store as an extension point.
 */
public interface MongoJobStore extends JobStore {}

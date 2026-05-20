package run.ratchet.store.mongodb;

import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.RecurringJobStore;

/**
 * Public MongoDB-specific store type.
 *
 * <p>The CDI implementation is package-private; consumers can inject this interface without
 * exposing the concrete store as an extension point.
 *
 * <p>Composes {@link RecurringJobStore} directly until the CP2 cleanup commit folds it into {@link
 * JobStore}.
 */
public interface MongoJobStore extends JobStore, RecurringJobStore {}

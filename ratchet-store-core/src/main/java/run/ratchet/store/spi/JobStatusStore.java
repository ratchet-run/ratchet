package run.ratchet.store.spi;

import run.ratchet.api.Incubating;

/**
 * Historical super-interface for all job status operations. Decomposed into {@link
 * JobTerminalStore}, {@link JobRetryStore}, {@link JobPauseStore}, and {@link JobBatchStatusStore}.
 *
 * <p>This marker composes the four new sub-interfaces for one release so existing implementations
 * keep compiling. New callers should depend on the narrowest of the four that they actually need,
 * which tightens dependency graphs and makes mocking simpler. Scheduled for removal in the next
 * minor release.
 */
@Deprecated(since = "alpha")
@Incubating
public interface JobStatusStore
    extends JobTerminalStore, JobRetryStore, JobPauseStore, JobBatchStatusStore {
  // Composed marker — all methods inherited from the four decomposed sub-interfaces above.
}

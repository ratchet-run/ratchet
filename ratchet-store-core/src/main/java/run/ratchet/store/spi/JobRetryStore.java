package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import java.time.Instant;

/**
 * Retry and backoff-scheduling operations for jobs.
 *
 * <p>Separated from {@link JobStatusStore} during the status-SPI decomposition.
 */
@Incubating
public interface JobRetryStore {

  int incrementRetryAttempt(long id);

  boolean scheduleJobRetry(long id, String error, Instant newScheduledTime, int attempts);

  /**
   * Atomically resets FAILED to PENDING including retry metadata in one operation to avoid TOCTOU
   * gaps.
   */
  boolean resetFailedToPending(long id);
}

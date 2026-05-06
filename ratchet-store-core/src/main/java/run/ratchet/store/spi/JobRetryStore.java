package run.ratchet.store.spi;

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;

/**
 * Retry and backoff-scheduling operations for jobs.
 *
 * <p>Separated from {@link JobStatusStore} during the status-SPI decomposition.
 */
@Incubating
public interface JobRetryStore {

  int incrementRetryAttempt(UUID id);

  boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts);

  /**
   * Atomically resets FAILED to PENDING including retry metadata in one operation to avoid TOCTOU
   * gaps.
   */
  boolean resetFailedToPending(UUID id);
}

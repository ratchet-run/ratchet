package run.ratchet.store.spi;

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;

/** Retry and backoff-scheduling operations for jobs. */
@Incubating
public interface JobRetryStore {

  /**
   * Increments the retry attempt count.
   *
   * @return the new attempt count, or {@code -1} when no retryable job row matched the id
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  int incrementRetryAttempt(UUID id);

  /**
   * Schedules the next retry attempt for a RUNNING or WAITING job. Implementations return {@code
   * false} for missing rows and for jobs in every other status, including FAILED terminal rows.
   * Transaction attribute: {@code REQUIRED}.
   */
  boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts);

  /**
   * Atomically resets FAILED to PENDING including retry metadata in one operation to avoid TOCTOU
   * gaps. Transaction attribute: {@code REQUIRED}.
   */
  boolean resetFailedToPending(UUID id);
}

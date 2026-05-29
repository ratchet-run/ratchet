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
   * @param id job id whose attempt counter should be incremented; never {@code null}
   * @return the new attempt count, or {@code -1} when no retryable job row matched the id
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  int incrementRetryAttempt(UUID id);

  /**
   * Schedules the next retry attempt for a RUNNING or WAITING job. Implementations return {@code
   * false} for missing rows and for jobs in every other status, including FAILED terminal rows.
   * Transaction attribute: {@code REQUIRED}.
   *
   * @param id job id to reschedule; never {@code null}
   * @param error last-error message to record alongside the new schedule; may be {@code null}
   * @param newScheduledTime instant at which the job becomes eligible for re-pickup; never {@code
   *     null}
   * @param attempts attempt counter to persist on the row (typically the post-increment value
   *     returned by {@link #incrementRetryAttempt(UUID)})
   * @return {@code true} when the row was RUNNING or WAITING and was rescheduled, {@code false}
   *     otherwise
   */
  boolean scheduleJobRetry(UUID id, String error, Instant newScheduledTime, int attempts);

  /**
   * Atomically resets FAILED to PENDING including retry metadata in one operation to avoid TOCTOU
   * gaps. Transaction attribute: {@code REQUIRED}.
   *
   * @param id job id to reset; never {@code null}
   * @return {@code true} when the row was FAILED and was reset to PENDING, {@code false} otherwise
   */
  boolean resetFailedToPending(UUID id);
}

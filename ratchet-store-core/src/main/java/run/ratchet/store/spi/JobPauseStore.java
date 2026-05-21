package run.ratchet.store.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobStatus;

/**
 * Pause / resume transitions for executable jobs.
 *
 * <p>Recurring-master pause/resume lives on {@link RecurringJobStore}.
 */
@Incubating
public interface JobPauseStore {

  /**
   * Atomically transitions to PAUSED, recording the original status for later resume in the same
   * operation to avoid TOCTOU gaps. Returns {@code false} when {@code expected} is WAITING or
   * terminal. Throws {@link IllegalArgumentException} when {@code expected} is PAUSED. Transaction
   * attribute: {@code REQUIRED}.
   */
  boolean transitionToPaused(UUID id, JobStatus expected);

  /**
   * Atomically transitions from PAUSED to the target status, clearing the stored paused-from
   * status. The target must be a non-PAUSED, non-WAITING live status. Transaction attribute: {@code
   * REQUIRED}.
   */
  boolean transitionFromPaused(UUID id, JobStatus target);

  /**
   * Atomically transitions from PAUSED to the stored paused-from status, reading the target from
   * the database row in the same operation to avoid TOCTOU races.
   *
   * @param id job id to resume
   * @return the status restored from the paused row, or {@code null} when no row is currently
   *     PAUSED. Callers should treat {@code null} as a lost race or missing job and re-read before
   *     deciding whether to retry.
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  JobStatus transitionFromPausedAtomic(UUID id);
}

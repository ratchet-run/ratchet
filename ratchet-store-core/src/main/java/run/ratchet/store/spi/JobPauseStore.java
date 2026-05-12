package run.ratchet.store.spi;

import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.JobStatus;

/**
 * Pause / resume transitions for executable jobs and recurring masters.
 *
 * <p>{@code pauseRecurring} / {@code resumeRecurring} ONLY operate on recurring masters; executable
 * pauses use the {@code transition*} methods.
 */
@Incubating
public interface JobPauseStore {

  /**
   * Atomically transitions to PAUSED, recording the original status for later resume in the same
   * operation to avoid TOCTOU gaps. Transaction attribute: {@code REQUIRED}.
   */
  boolean transitionToPaused(UUID id, JobStatus expected);

  /**
   * Atomically transitions from PAUSED to the target status, clearing the stored paused-from
   * status. Transaction attribute: {@code REQUIRED}.
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

  /**
   * Pauses a recurring master. ONLY operates on recurring masters. Post hot/cold-split, recurring
   * masters live in cold with the rec_status shim ('P' PENDING, 'A' PAUSED) and have no hot row.
   * Single-table store implementations may treat this as a status flip on the live row. Returns
   * true iff the master transitioned from PENDING to PAUSED. Transaction attribute: {@code
   * REQUIRED}.
   */
  boolean pauseRecurring(UUID id);

  /**
   * Resumes a recurring master. ONLY operates on recurring masters. Returns true iff the master
   * transitioned from PAUSED to PENDING. Transaction attribute: {@code REQUIRED}.
   */
  boolean resumeRecurring(UUID id);
}

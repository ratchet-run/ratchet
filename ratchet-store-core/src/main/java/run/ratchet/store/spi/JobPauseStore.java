package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobStatus;

/**
 * Pause / resume transitions for executable jobs and recurring masters.
 *
 * <p>Separated from {@link JobStatusStore} during the status-SPI decomposition. {@code
 * pauseRecurring} / {@code resumeRecurring} ONLY operate on recurring masters; executable pauses
 * use the {@code transition*} methods.
 */
@Incubating
public interface JobPauseStore {

  /**
   * Atomically transitions to PAUSED, recording the original status for later resume in the same
   * operation to avoid TOCTOU gaps.
   */
  boolean transitionToPaused(long id, JobStatus expected);

  /**
   * Atomically transitions from PAUSED to the target status, clearing the stored paused-from
   * status.
   */
  boolean transitionFromPaused(long id, JobStatus target);

  /**
   * Atomically transitions from PAUSED to the stored paused-from status, reading the target from
   * the database row in the same operation to avoid TOCTOU races.
   */
  JobStatus transitionFromPausedAtomic(long id);

  /**
   * Pauses a recurring master. ONLY operates on recurring masters. Post hot/cold-split, recurring
   * masters live in cold with the rec_status shim ('P' PENDING, 'A' PAUSED) and have no hot row.
   * Single-table store implementations may treat this as a status flip on the live row. Returns
   * true iff the master transitioned from PENDING to PAUSED.
   */
  boolean pauseRecurring(long id);

  /**
   * Resumes a recurring master. ONLY operates on recurring masters. Returns true iff the master
   * transitioned from PAUSED to PENDING.
   */
  boolean resumeRecurring(long id);
}

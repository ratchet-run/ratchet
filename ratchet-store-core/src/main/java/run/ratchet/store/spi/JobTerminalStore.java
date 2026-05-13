package run.ratchet.store.spi;

import java.time.Instant;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.api.exception.RatchetTransientStoreException;

/**
 * Terminal status transitions for jobs: success / failure / cancel.
 *
 * <p>Implementations are expected to flip a live job to its terminal form atomically (for the
 * hot/cold MySQL store, this means hot DELETE + cold UPDATE + bkres DELETE in a single
 * transaction).
 *
 * <p>A {@code false} return means the requested transition did not match a row. Store failures
 * propagate as {@link RatchetTransientStoreException}; implementations must not convert them to
 * {@code false}.
 */
@Incubating
public interface JobTerminalStore {

  /**
   * Marks a job as succeeded with a stored result.
   *
   * @throws RatchetTransientStoreException if the backing store cannot complete the transition
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean markJobSucceeded(
      UUID id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs);

  /**
   * Marks a job as succeeded without a stored result.
   *
   * @throws RatchetTransientStoreException if the backing store cannot complete the transition
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean markJobSucceededMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs);

  /**
   * Marks a batch child as succeeded and advances the parent batch counters atomically.
   *
   * <p>The child terminal transition and parent counter update must happen in the same transaction;
   * implementations must not commit the counter in an inner {@code REQUIRES_NEW} transaction.
   *
   * @throws RatchetTransientStoreException if either update cannot complete
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean markJobSucceededAndUpdateBatch(
      UUID jobId,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs,
      UUID batchId);

  /**
   * Atomically transitions a RUNNING job to terminal FAILED state. Captures total attempts and
   * terminal error in a single store call. Replaces the older {@code setStatus(FAILED)+save}
   * pattern that is incompatible with the hot/cold split (hot DELETE + cold UPDATE + bkres DELETE
   * in one tx).
   *
   * <p><b>Caller contract:</b> the target job must be in {@code RUNNING} status when this is
   * invoked. Implementations may match the hot row on {@code status = 'RUNNING'} and return {@code
   * false} (silent no-op) for any other status, including {@code WAITING}. To fail a WAITING job
   * (e.g. on signal timeout), use {@code compareAndSwapStatus} with an expected status of {@code
   * WAITING}, which dispatches to the matching {@code WAITING}-aware path internally.
   *
   * @throws RatchetTransientStoreException if the backing store cannot complete the transition
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts);

  /**
   * Cancels a job by id. Dispatches by job_type internally: executable jobs DELETE the live queue
   * row + UPDATE cold to terminal CANCELED; recurring masters clear the recurring shim and set cold
   * terminal CANCELED. Single-table store implementations may treat this as an UPDATE to CANCELED.
   * Returns true iff the job transitioned to CANCELED.
   *
   * @throws RatchetTransientStoreException if the backing store cannot complete the transition
   *     <p>Transaction attribute: {@code REQUIRED}.
   */
  boolean cancelJob(UUID id);
}

package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import java.time.Instant;
import java.util.UUID;

/**
 * Terminal status transitions for jobs: success / failure / cancel.
 *
 * <p>Separated from {@link JobStatusStore} during the status-SPI decomposition. Implementations are
 * expected to flip a live job to its terminal form atomically (for the hot/cold MySQL store, this
 * means hot DELETE + cold UPDATE + bkres DELETE in a single transaction).
 */
@Incubating
public interface JobTerminalStore {

  boolean markJobSucceeded(
      UUID id,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      Long durationMs,
      Long queueWaitMs);

  boolean markJobSucceededMinimal(
      UUID id, Instant start, Instant end, Long durationMs, Long queueWaitMs);

  /** Marks a batch child as succeeded and advances the parent batch counters atomically. */
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
   * pattern that is incompatible with the hot/cold split (hot DELETE + cold UPDATE + bkres
   * DELETE in one tx).
   */
  boolean markJobFailedTerminal(UUID id, String terminalError, int totalAttempts);

  /**
   * Cancels a job by id. Dispatches by job_type internally: executable jobs DELETE the live queue
   * row + UPDATE cold to terminal CANCELED; recurring masters clear the recurring shim and set cold
   * terminal CANCELED. Single-table store implementations may treat this as an UPDATE to CANCELED.
   * Returns true iff the job transitioned to CANCELED.
   */
  boolean cancelJob(UUID id);
}

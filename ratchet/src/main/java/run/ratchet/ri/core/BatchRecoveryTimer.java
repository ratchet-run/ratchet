package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Timer that delegates periodic batch recovery to {@link BatchService}.
 *
 * <p>Recovers stuck batches every 15 minutes — batches where all children have completed but the
 * completion flag was never set (due to crash, network partition, or transaction rollback).
 *
 * @see BatchService#recoverStuckBatches()
 */
@ApplicationScoped
public class BatchRecoveryTimer {

  private static final Logger log = Logger.getLogger(BatchRecoveryTimer.class.getName());

  /** The batch service that contains the actual recovery logic. */
  private final BatchService batchService;

  /** Handle to the scheduled task for cancellation during shutdown. */
  private volatile ScheduledFuture<?> handle;

  // Required by CDI proxy
  protected BatchRecoveryTimer() {
    this.batchService = null;
  }

  @Inject
  public BatchRecoveryTimer(BatchService batchService) {
    this.batchService = batchService;
  }

  /**
   * Starts the batch recovery timer on the provided executor.
   *
   * <p>Schedules batch recovery to run every 15 minutes.
   *
   * @param executor the scheduled executor to use for periodic execution
   */
  public void start(ScheduledExecutorService executor) {
    handle = executor.scheduleAtFixedRate(this::recoverBatches, 15, 15, TimeUnit.MINUTES);
    log.info("Initialized batch recovery timer — checking for stuck batches every 15min");
  }

  /** Stops the batch recovery timer. */
  public void stop() {
    if (handle != null) {
      handle.cancel(false);
      handle = null;
    }
  }

  /** Recovers stuck batches. Called periodically by the scheduled executor. */
  void recoverBatches() {
    int recovered = batchService.recoverStuckBatches();
    if (recovered > 0) {
      log.info("Batch recovery timer recovered " + recovered + " stuck batch(es)");
    }
  }
}

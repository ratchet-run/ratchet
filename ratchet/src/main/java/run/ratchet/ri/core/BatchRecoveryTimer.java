package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

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

  private static final Logger log = Logger.getLogger(BatchRecoveryTimer.class);

  private final BatchService batchService;

  private volatile ScheduledFuture<?> handle;

  // Required by CDI proxy
  protected BatchRecoveryTimer() {
    this.batchService = null;
  }

  @Inject
  public BatchRecoveryTimer(BatchService batchService) {
    this.batchService = batchService;
  }

  public void start(ScheduledExecutorService executor) {
    handle = executor.scheduleAtFixedRate(this::recoverBatches, 15, 15, TimeUnit.MINUTES);
    log.info("Initialized batch recovery timer — checking for stuck batches every 15min");
  }

  public void stop() {
    if (handle != null) {
      handle.cancel(false);
      handle = null;
    }
  }

  void recoverBatches() {
    int recovered = batchService.recoverStuckBatches();
    if (recovered > 0) {
      log.infof("Batch recovery timer recovered %s stuck batch(es)", recovered);
    }
  }
}

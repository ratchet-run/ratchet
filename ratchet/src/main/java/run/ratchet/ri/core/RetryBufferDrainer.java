package run.ratchet.ri.core;

import run.ratchet.ri.core.RetryBufferManager.BufferedClaim;
import run.ratchet.ri.util.RatchetConfiguration;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.JobExecutionType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/**
 * Periodically drains retry buffers by resubmitting jobs when executor capacity becomes available.
 * Runs on a fixed 1-second interval, respecting priority ordering and drain mode.
 */
@ApplicationScoped
public class RetryBufferDrainer {

  private static final Logger log = Logger.getLogger(RetryBufferDrainer.class);

  private final AtomicBoolean started = new AtomicBoolean();

  private final ExecutorProvider executorProvider;
  private final RetryBufferManager retryBufferManager;
  private final JobSubmissionService jobSubmissionService;
  private final ThreadPoolManager threadPoolManager;
  private final DrainController drainController;
  private final long drainIntervalMs;

  @SuppressWarnings("java:S3077")
  private volatile ScheduledFuture<?> drainerTask;

  protected RetryBufferDrainer() {
    this.executorProvider = null;
    this.retryBufferManager = null;
    this.jobSubmissionService = null;
    this.threadPoolManager = null;
    this.drainController = null;
    this.drainIntervalMs = 1000L;
  }

  @Inject
  public RetryBufferDrainer(
      ExecutorProvider executorProvider,
      RetryBufferManager retryBufferManager,
      JobSubmissionService jobSubmissionService,
      ThreadPoolManager threadPoolManager,
      DrainController drainController,
      RatchetConfiguration config) {
    this.executorProvider = executorProvider;
    this.retryBufferManager = retryBufferManager;
    this.jobSubmissionService = jobSubmissionService;
    this.threadPoolManager = threadPoolManager;
    this.drainController = drainController;
    this.drainIntervalMs = Math.max(50L, config.getRetryBufferDrainIntervalMs());
  }

  void start() {
    if (!started.compareAndSet(false, true)) {
      return;
    }

    drainerTask =
        executorProvider
            .getScheduledExecutor()
            .scheduleAtFixedRate(
                this::drainRetryBuffers, drainIntervalMs, drainIntervalMs, TimeUnit.MILLISECONDS);
  }

  void shutdown() {
    started.set(false);
    if (drainerTask != null && !drainerTask.isCancelled()) {
      drainerTask.cancel(false);
      log.info("RetryBufferDrainer shutdown complete");
    }
  }

  private void drainRetryBuffers() {
    if (drainController.isDraining()) {
      return;
    }

    for (JobExecutionType jobType : JobExecutionType.values()) {
      while (!drainController.isDraining()) {
        int capacity = threadPoolManager.getAvailableCapacity(jobType);
        if (capacity <= 0) {
          break;
        }

        List<BufferedClaim> bufferedJobs =
            retryBufferManager.pollBatchFromBuffer(jobType, capacity);
        if (bufferedJobs.isEmpty()) {
          break;
        }

        for (BufferedClaim buffered : bufferedJobs) {
          if (drainController.isDraining() || !threadPoolManager.canAcceptWork(jobType)) {
            retryBufferManager.forceOffer(buffered.toClaimDto());
            continue;
          }
          jobSubmissionService.submitBuffered(buffered.toClaimDto());
        }
      }
    }
  }
}

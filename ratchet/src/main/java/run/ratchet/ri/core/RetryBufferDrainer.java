package run.ratchet.ri.core;

import run.ratchet.ri.core.RetryBufferManager.BufferedJob;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobCrudStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Periodically drains retry buffers by resubmitting jobs when executor capacity becomes available.
 *
 * <p>When jobs cannot be immediately executed due to thread pool capacity constraints, they are
 * placed in priority-ordered retry buffers managed by {@link RetryBufferManager}. This drainer runs
 * on a fixed 1-second interval to check buffer status and resubmit jobs when resources free up.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li><b>Buffer Monitoring:</b> Reports buffer sizes per job type and total via {@link
 *       MetricsCollector}, enabling dashboard visibility into backpressure
 *   <li><b>Priority Preservation:</b> Buffers maintain priority ordering, ensuring high-priority
 *       jobs are drained first when capacity becomes available
 *   <li><b>Capacity-Aware Draining:</b> Only submits jobs when the corresponding thread pool can
 *       accept work, preventing submission failures
 *   <li><b>Drain Mode Respect:</b> Stops draining during graceful shutdown to allow in-flight jobs
 *       to complete without new work being added
 * </ul>
 *
 * <p>Backpressure flow:
 *
 * <pre>
 * JobExecutionCoordinator.submit() -> Thread pool full -> RetryBufferManager.offer()
 *                                                              |
 * RetryBufferDrainer (1s interval) -> Check capacity -> JobSubmissionService.submitBuffered()
 * </pre>
 *
 * @see RetryBufferManager for the buffer storage implementation
 * @see JobSubmissionService for the submission mechanism
 * @see ThreadPoolManager for capacity checking
 */
@ApplicationScoped
public class RetryBufferDrainer {

  private static final Logger log = Logger.getLogger(RetryBufferDrainer.class.getName());

  /** Thread-safe flag ensuring the drainer is started exactly once. */
  private final AtomicBoolean started = new AtomicBoolean();

  /** Executor provider for scheduling the periodic drain task. */
  private final ExecutorProvider executorProvider;

  /** Manages the priority-ordered retry buffers for each job type. */
  private final RetryBufferManager retryBufferManager;

  /** Service for submitting buffered jobs to thread pools. */
  private final JobSubmissionService jobSubmissionService;

  /** Provides thread pool capacity information for drain decisions. */
  private final ThreadPoolManager threadPoolManager;

  /** Controls drain mode during graceful shutdown. */
  private final DrainController drainController;

  /** Store for loading full job entities from buffered DTOs when draining. */
  private final JobCrudStore jobCrudStore;

  /** Handle to the scheduled drainer task for cancellation during shutdown. */
  private ScheduledFuture<?> drainerTask;

  // Required by CDI proxy
  protected RetryBufferDrainer() {
    this.executorProvider = null;
    this.retryBufferManager = null;
    this.jobSubmissionService = null;
    this.threadPoolManager = null;
    this.drainController = null;
    this.jobCrudStore = null;
  }

  @Inject
  public RetryBufferDrainer(
      ExecutorProvider executorProvider,
      RetryBufferManager retryBufferManager,
      JobSubmissionService jobSubmissionService,
      ThreadPoolManager threadPoolManager,
      DrainController drainController,
      JobCrudStore jobCrudStore) {
    this.executorProvider = executorProvider;
    this.retryBufferManager = retryBufferManager;
    this.jobSubmissionService = jobSubmissionService;
    this.threadPoolManager = threadPoolManager;
    this.drainController = drainController;
    this.jobCrudStore = jobCrudStore;
  }

  /**
   * Starts the RetryBufferDrainer, scheduling the periodic task for draining retry buffers.
   *
   * <p>This method ensures that the drainer is started only once, using a thread-safe mechanism. It
   * schedules the periodic execution of the {@code drainRetryBuffers} method, which attempts to
   * process and resubmit jobs from the retry buffers.
   *
   * <p>The buffers hold the jobs sorted by priority, ensuring that the highest-priority jobs are
   * processed first when resources become available.
   *
   * <p>Preconditions: This method should only be invoked once during the startup phase of the
   * application. The required dependencies must be properly provided.
   */
  void start() {
    if (!started.compareAndSet(false, true)) {
      return;
    }

    drainerTask =
        executorProvider
            .getScheduledExecutor()
            .scheduleAtFixedRate(this::drainRetryBuffers, 1, 1, TimeUnit.SECONDS);
  }

  /**
   * Stops the retry buffer drainer during application shutdown.
   *
   * <p>Cancels the scheduled drainer task to prevent it from attempting to drain buffers during
   * shutdown, which could interfere with graceful termination.
   */
  void shutdown() {
    started.set(false);
    if (drainerTask != null && !drainerTask.isCancelled()) {
      drainerTask.cancel(false);
      log.info("RetryBufferDrainer shutdown complete");
    }
  }

  /**
   * Drains retry buffers associated with specific {@code JobExecutionType}s by attempting to
   * resubmit jobs under the current system constraints.
   *
   * <p>This method processes jobs stored in retry buffers managed by {@code retryBufferManager},
   * which holds jobs that failed on initial attempts and are awaiting resource availability for
   * retry. It iterates through all {@link JobExecutionType} categories, polling jobs from their
   * respective buffers.
   *
   * <p>The draining process is governed by the {@code drainController}. If the drain mode is
   * activated (via {@code drainController.isDraining()}), the method exits immediately to avoid
   * unnecessary processing. Similarly, it checks resource availability for each {@code
   * JobExecutionType} via the {@code threadPoolManager}. If no resources are available, the
   * processing for that {@code JobExecutionType} is paused.
   *
   * <p>The method submits retry jobs to the {@code jobSubmissionService} for further execution,
   * ensuring that each job is adequately polled from the retry buffer before submission.
   *
   * <p>Key considerations: The method ensures that resources are not overwhelmed by conditionally
   * evaluating system capacity. It operates in a loop that is interrupted if the drain mode becomes
   * active or if no jobs exist for processing. The retry job buffers are drained in a FIFO manner
   * to preserve job submission order.
   */
  private void drainRetryBuffers() {
    if (drainController.isDraining()) {
      return;
    }

    for (JobExecutionType jobType : JobExecutionType.values()) {
      while (!retryBufferManager.isBufferEmpty(jobType) && !drainController.isDraining()) {
        int capacity = threadPoolManager.getAvailableCapacity(jobType);
        if (capacity <= 0) {
          break;
        }

        List<BufferedJob> bufferedJobs = retryBufferManager.pollBatchFromBuffer(jobType, capacity);
        if (bufferedJobs.isEmpty()) {
          break;
        }

        Map<Long, JobEntity> jobsById = new LinkedHashMap<>();
        for (var job :
            jobCrudStore.findByIds(bufferedJobs.stream().map(BufferedJob::jobId).toList())) {
          jobsById.put(job.getId(), job);
        }

        for (BufferedJob buffered : bufferedJobs) {
          JobEntity job = jobsById.get(buffered.jobId());
          if (drainController.isDraining() || !threadPoolManager.canAcceptWork(jobType)) {
            if (job != null) {
              retryBufferManager.forceOffer(job);
            }
            continue;
          }

          if (job == null) {
            log.warning(
                "Buffered job " + buffered.jobId() + " no longer exists in database, skipping");
            continue;
          }
          jobSubmissionService.submitBuffered(job);
        }
      }
    }
  }
}

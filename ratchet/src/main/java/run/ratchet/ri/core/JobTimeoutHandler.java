package run.ratchet.ri.core;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobStatusStore;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service responsible for monitoring and enforcing job execution timeouts to prevent runaway jobs
 * from consuming system resources indefinitely. Implements a two-tier timeout strategy with soft
 * warnings and hard cancellations.
 *
 * <p>Timeout enforcement strategy:
 *
 * <ol>
 *   <li><b>Soft Timeout:</b> At configurable percentage (default 80%) of timeout - logs warning
 *   <li><b>Hard Timeout:</b> At 100% of configured timeout - forcefully cancels job execution
 * </ol>
 *
 * @see JobTask for timeout handler integration
 */
public class JobTimeoutHandler {

  private static final Logger log = Logger.getLogger(JobTimeoutHandler.class.getName());

  private final JobCrudStore jobCrudStore;
  private final JobStatusStore jobStatusStore;
  private final PostExecutionHandler lifecycleFacade;
  private final int softTimeoutPercent;
  private final long defaultTimeoutSeconds;

  // Required by CDI proxy
  protected JobTimeoutHandler() {
    this.jobCrudStore = null;
    this.jobStatusStore = null;
    this.lifecycleFacade = null;
    this.softTimeoutPercent = 0;
    this.defaultTimeoutSeconds = 0;
  }

  /**
   * Creates a new JobTimeoutHandler.
   *
   * @param jobCrudStore store for loading job entities
   * @param jobStatusStore store for marking jobs as failed
   * @param lifecycleFacade handler for batch/workflow failure propagation
   * @param softTimeoutPercent percentage of timeout at which soft warning fires (e.g. 80)
   * @param defaultTimeoutSeconds default timeout if job-specific not set
   */
  public JobTimeoutHandler(
      JobCrudStore jobCrudStore,
      JobStatusStore jobStatusStore,
      PostExecutionHandler lifecycleFacade,
      int softTimeoutPercent,
      long defaultTimeoutSeconds) {
    this.jobCrudStore = jobCrudStore;
    this.jobStatusStore = jobStatusStore;
    this.lifecycleFacade = lifecycleFacade;
    this.softTimeoutPercent = softTimeoutPercent;
    this.defaultTimeoutSeconds = defaultTimeoutSeconds;
  }

  /**
   * Schedules timeout monitoring for a job execution.
   *
   * @param job the job entity to monitor for timeout
   * @param future the Future representing the job's asynchronous execution
   * @param scheduler the scheduled executor service for timeout callbacks
   * @param executionStartTime the instant when job execution began
   */
  public void scheduleTimeoutMonitoring(
      JobEntity job,
      Future<?> future,
      ScheduledExecutorService scheduler,
      Instant executionStartTime) {
    scheduleTimeoutMonitoring(
        job.getId(), job.getTimeoutSec(), future, scheduler, executionStartTime);
  }

  /**
   * Schedules timeout monitoring for a job using only job ID and timeout value.
   *
   * @param jobId the job ID to monitor for timeout
   * @param jobTimeoutSec the job's configured timeout (0 or negative uses system default)
   * @param future the Future representing the job's asynchronous execution
   * @param scheduler the scheduled executor service for timeout callbacks
   * @param executionStartTime the instant when job execution began
   */
  public void scheduleTimeoutMonitoring(
      Long jobId,
      int jobTimeoutSec,
      Future<?> future,
      ScheduledExecutorService scheduler,
      Instant executionStartTime) {
    long timeoutSec = jobTimeoutSec;
    if (timeoutSec <= 0) {
      timeoutSec = defaultTimeoutSeconds;
    }

    final long finalTimeoutSec = timeoutSec;
    final AtomicBoolean softTimeoutSent = new AtomicBoolean(false);

    long softTimeoutSec = (timeoutSec * softTimeoutPercent) / 100;

    scheduler.schedule(
        () ->
            handleSoftTimeoutById(
                jobId, future, softTimeoutSent, executionStartTime, finalTimeoutSec),
        softTimeoutSec,
        TimeUnit.SECONDS);

    scheduler.schedule(
        () -> handleHardTimeoutById(jobId, future, executionStartTime, finalTimeoutSec),
        timeoutSec,
        TimeUnit.SECONDS);
  }

  private String formatDuration(Duration duration) {
    long hours = duration.toHours();
    long minutes = duration.toMinutesPart();
    long seconds = duration.toSecondsPart();

    if (hours > 0) {
      return String.format("%dh %dm %ds", hours, minutes, seconds);
    } else if (minutes > 0) {
      return String.format("%dm %ds", minutes, seconds);
    } else {
      return String.format("%ds", seconds);
    }
  }

  private void handleSoftTimeoutById(
      Long jobId,
      Future<?> future,
      AtomicBoolean softTimeoutSent,
      Instant executionStartTime,
      long timeoutSec) {
    if (!future.isDone() && softTimeoutSent.compareAndSet(false, true)) {
      Duration elapsed = Duration.between(executionStartTime, Instant.now());
      log.warning(
          String.format(
              "Job %s approaching timeout - %d%% threshold reached. Elapsed: %s, Timeout: %ds",
              jobId, softTimeoutPercent, formatDuration(elapsed), timeoutSec));
    }
  }

  private void handleHardTimeoutById(
      Long jobId, Future<?> future, Instant executionStartTime, long timeoutSec) {
    if (!future.isDone()) {
      Duration elapsed = Duration.between(executionStartTime, Instant.now());
      log.severe(
          String.format(
              "Job %s exceeded timeout of %ds. Cancelling execution. Elapsed: %s",
              jobId, timeoutSec, formatDuration(elapsed)));

      future.cancel(true);

      try {
        boolean marked =
            jobStatusStore.compareAndSwapStatus(
                jobId,
                JobStatus.RUNNING,
                JobStatus.FAILED,
                "Hard timeout exceeded (" + timeoutSec + "s)");
        if (marked) {
          log.info("Job " + jobId + " marked as FAILED due to hard timeout");

          jobCrudStore
              .findById(jobId)
              .ifPresent(
                  job ->
                      lifecycleFacade.handlePermanentFailure(
                          job,
                          new java.util.concurrent.TimeoutException(
                              "Hard timeout exceeded (" + timeoutSec + "s)")));
        } else {
          log.info("Job " + jobId + " already in terminal state when timeout handler ran");
        }
      } catch (Exception e) {
        log.log(Level.SEVERE, "Failed to mark timed-out job as FAILED: " + jobId, e);
      }
    }
  }
}

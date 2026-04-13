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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;

/**
 * Enforces job execution timeouts with a two-tier strategy: a soft warning at a configurable
 * percentage of the limit (default 80%), then a hard cancel + DLQ escalation at 100%.
 */
public class JobTimeoutHandler {

  private static final Logger log = Logger.getLogger(JobTimeoutHandler.class);

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

  public void scheduleTimeoutMonitoring(
      JobEntity job,
      Future<?> future,
      ScheduledExecutorService scheduler,
      Instant executionStartTime) {
    scheduleTimeoutMonitoring(
        job.getId(), job.getTimeoutSec(), future, scheduler, executionStartTime);
  }

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
      log.warn(
          String.format(
              "Job %s approaching timeout - %d%% threshold reached. Elapsed: %s, Timeout: %ds",
              jobId, softTimeoutPercent, formatDuration(elapsed), timeoutSec));
    }
  }

  private void handleHardTimeoutById(
      Long jobId, Future<?> future, Instant executionStartTime, long timeoutSec) {
    if (future.isDone()) {
      return;
    }
    Duration elapsed = Duration.between(executionStartTime, Instant.now());
    log.error(
        String.format(
            "Job %s exceeded timeout of %ds. Cancelling execution. Elapsed: %s",
            jobId, timeoutSec, formatDuration(elapsed)));

    future.cancel(true);

    try {
      processHardTimeout(jobId, timeoutSec);
    } catch (Exception e) {
      log.errorf(e, "Failed to mark timed-out job as FAILED: %s", jobId);
    }
  }

  /**
   * Applies the hard-timeout routing decision: retry-or-fail. Package-private for testability.
   *
   * <p>Operation order is intentional:
   *
   * <ol>
   *   <li>Load the job to see {@code maxRetries} and confirm it still exists.
   *   <li>Increment {@code attempts} while status is still {@code RUNNING}. {@code
   *       incrementRetryAttempt} has {@code WHERE status = 'RUNNING'} — calling it AFTER a CAS to
   *       FAILED would silently no-op (the bug this fix addresses).
   *   <li>If retries remain, call {@code scheduleJobRetry} (which accepts both {@code RUNNING} and
   *       {@code FAILED}). If that returns {@code false}, the job was already finalized by a
   *       competing completion between steps 2 and 3 — exit cleanly instead of double-escalating to
   *       DLQ.
   *   <li>Otherwise CAS {@code RUNNING → FAILED} and route through {@code handlePermanentFailure}.
   * </ol>
   *
   * <p><b>Known tradeoff:</b> If a normal completion wins the race between steps 2 and 3, the
   * successfully-completed job will end up with {@code attempts} one higher than it should. This is
   * an observability inaccuracy, not a correctness failure — the job is still COMPLETED. Do not
   * "fix" it by moving the increment after the CAS; that reintroduces the dead-code bug.
   */
  void processHardTimeout(Long jobId, long timeoutSec) {
    TimeoutException timeoutEx =
        new TimeoutException("Hard timeout exceeded (" + timeoutSec + "s)");
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.infof("Job %s no longer exists when timeout handler ran", jobId);
      return;
    }

    // Step 1: Increment attempts while status is still RUNNING.
    int newAttempts = jobStatusStore.incrementRetryAttempt(jobId);
    if (newAttempts < 0) {
      // Not in RUNNING anymore — worker already transitioned it. Nothing to do.
      log.infof("Job %s already left RUNNING when timeout handler ran", jobId);
      return;
    }

    // Step 2: Retries remain? Try to reschedule.
    if (newAttempts <= job.getMaxRetries()) {
      Instant retryTime = Instant.now().plusSeconds(timeoutSec);
      boolean rescheduled =
          jobStatusStore.scheduleJobRetry(jobId, timeoutEx.getMessage(), retryTime, newAttempts);
      if (rescheduled) {
        log.warnf(
            "Job %s timed out but has retries remaining (%s/%s) — rescheduled for %s",
            jobId, newAttempts, job.getMaxRetries(), retryTime);
        return;
      }
      // scheduleJobRetry returned false — a competing path finalized the job between the
      // increment and the reschedule. Do NOT escalate to DLQ; the job already has a terminal
      // state set by the competing path.
      log.infof(
          "Job %s timed out but was already finalized by a competing path — no DLQ escalation",
          jobId);
      return;
    }

    // Step 3: Retries exhausted — CAS to FAILED and route to DLQ.
    boolean marked =
        jobStatusStore.compareAndSwapStatus(
            jobId, JobStatus.RUNNING, JobStatus.FAILED, timeoutEx.getMessage());
    if (!marked) {
      log.infof("Job %s already in terminal state when timeout handler ran", jobId);
      return;
    }
    log.infof("Job %s marked as FAILED due to hard timeout (retries exhausted)", jobId);
    lifecycleFacade.handlePermanentFailure(job, timeoutEx);
  }
}

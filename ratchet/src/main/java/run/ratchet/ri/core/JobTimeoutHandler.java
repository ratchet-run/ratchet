package run.ratchet.ri.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobSignalTimedOutEvent;
import run.ratchet.api.exception.SignalTimeoutException;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.SignalStore;

/**
 * Enforces job execution timeouts with a two-tier strategy: a soft warning at a configurable
 * percentage of the limit (default 80%), then a hard cancel + DLQ escalation at 100%.
 */
public class JobTimeoutHandler {

  private static final Logger log = Logger.getLogger(JobTimeoutHandler.class);
  static final int DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE = 500;

  private final JobCrudStore jobCrudStore;
  private final JobRetryStore jobRetryStore;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final PostExecutionHandler lifecycleFacade;
  private final InternalEventPublisher eventPublisher;
  private final ChainScheduler chainScheduler;
  private final SignalStore signalStore;
  private final MetricsCollector metricsCollector;
  private final int softTimeoutPercent;
  private final long defaultTimeoutSeconds;
  private final Clock clock;
  private final int signalTimeoutBatchSize;

  protected JobTimeoutHandler() {
    this.jobCrudStore = null;
    this.jobRetryStore = null;
    this.jobBatchStatusStore = null;
    this.lifecycleFacade = null;
    this.eventPublisher = null;
    this.chainScheduler = null;
    this.signalStore = null;
    this.metricsCollector = null;
    this.softTimeoutPercent = 0;
    this.defaultTimeoutSeconds = 0;
    this.clock = null;
    this.signalTimeoutBatchSize = 0;
  }

  public JobTimeoutHandler(
      JobCrudStore jobCrudStore,
      JobRetryStore jobRetryStore,
      JobBatchStatusStore jobBatchStatusStore,
      PostExecutionHandler lifecycleFacade,
      int softTimeoutPercent,
      long defaultTimeoutSeconds) {
    this(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        lifecycleFacade,
        softTimeoutPercent,
        defaultTimeoutSeconds,
        Clock.systemUTC());
  }

  public JobTimeoutHandler(
      JobCrudStore jobCrudStore,
      JobRetryStore jobRetryStore,
      JobBatchStatusStore jobBatchStatusStore,
      PostExecutionHandler lifecycleFacade,
      int softTimeoutPercent,
      long defaultTimeoutSeconds,
      Clock clock) {
    this(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        lifecycleFacade,
        softTimeoutPercent,
        defaultTimeoutSeconds,
        clock,
        null,
        null,
        null,
        null);
  }

  public JobTimeoutHandler(
      JobCrudStore jobCrudStore,
      JobRetryStore jobRetryStore,
      JobBatchStatusStore jobBatchStatusStore,
      PostExecutionHandler lifecycleFacade,
      int softTimeoutPercent,
      long defaultTimeoutSeconds,
      Clock clock,
      InternalEventPublisher eventPublisher,
      ChainScheduler chainScheduler,
      SignalStore signalStore) {
    this(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        lifecycleFacade,
        softTimeoutPercent,
        defaultTimeoutSeconds,
        clock,
        eventPublisher,
        chainScheduler,
        signalStore,
        null);
  }

  public JobTimeoutHandler(
      JobCrudStore jobCrudStore,
      JobRetryStore jobRetryStore,
      JobBatchStatusStore jobBatchStatusStore,
      PostExecutionHandler lifecycleFacade,
      int softTimeoutPercent,
      long defaultTimeoutSeconds,
      Clock clock,
      InternalEventPublisher eventPublisher,
      ChainScheduler chainScheduler,
      SignalStore signalStore,
      MetricsCollector metricsCollector) {
    this(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        lifecycleFacade,
        softTimeoutPercent,
        defaultTimeoutSeconds,
        clock,
        eventPublisher,
        chainScheduler,
        signalStore,
        metricsCollector,
        DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE);
  }

  public JobTimeoutHandler(
      JobCrudStore jobCrudStore,
      JobRetryStore jobRetryStore,
      JobBatchStatusStore jobBatchStatusStore,
      PostExecutionHandler lifecycleFacade,
      int softTimeoutPercent,
      long defaultTimeoutSeconds,
      Clock clock,
      InternalEventPublisher eventPublisher,
      ChainScheduler chainScheduler,
      SignalStore signalStore,
      MetricsCollector metricsCollector,
      int signalTimeoutBatchSize) {
    this.jobCrudStore = jobCrudStore;
    this.jobRetryStore = jobRetryStore;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.lifecycleFacade = lifecycleFacade;
    this.softTimeoutPercent = softTimeoutPercent;
    this.defaultTimeoutSeconds = defaultTimeoutSeconds;
    this.clock = clock;
    this.eventPublisher = eventPublisher;
    this.chainScheduler = chainScheduler;
    this.signalStore = signalStore;
    this.metricsCollector = metricsCollector;
    this.signalTimeoutBatchSize = Math.max(1, signalTimeoutBatchSize);
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }

  public TimeoutHandles scheduleTimeoutMonitoring(
      JobEntity job,
      Future<?> future,
      ScheduledExecutorService scheduler,
      Instant executionStartTime) {
    return scheduleTimeoutMonitoring(
        job.getId(), job.getTimeoutSec(), future, scheduler, executionStartTime);
  }

  public TimeoutHandles scheduleTimeoutMonitoring(
      UUID jobId,
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

    ScheduledFuture<?> soft =
        scheduler.schedule(
            () ->
                handleSoftTimeoutById(
                    jobId, future, softTimeoutSent, executionStartTime, finalTimeoutSec),
            softTimeoutSec,
            TimeUnit.SECONDS);

    ScheduledFuture<?> hard =
        scheduler.schedule(
            () -> handleHardTimeoutById(jobId, future, executionStartTime, finalTimeoutSec),
            timeoutSec,
            TimeUnit.SECONDS);

    return new TimeoutHandles(soft, hard);
  }

  /** Applies timeout routing: retry if attempts remain, otherwise fail permanently. */
  void processHardTimeout(UUID jobId, long timeoutSec) {
    TimeoutException timeoutEx =
        new TimeoutException("Hard timeout exceeded (" + timeoutSec + "s)");
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.infof("Job %s no longer exists when timeout handler ran", jobId);
      return;
    }

    // Step 1: Increment attempts while status is still RUNNING.
    int newAttempts = jobRetryStore.incrementRetryAttempt(jobId);
    if (newAttempts < 0) {
      // Not in RUNNING anymore — worker already transitioned it. Nothing to do.
      log.infof("Job %s already left RUNNING when timeout handler ran", jobId);
      return;
    }

    // Step 2: Retries remain? Try to reschedule.
    if (newAttempts <= job.getMaxRetries()) {
      Instant retryTime = effective().instant().plusSeconds(timeoutSec);
      boolean rescheduled =
          jobRetryStore.scheduleJobRetry(jobId, timeoutEx.getMessage(), retryTime, newAttempts);
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
        jobBatchStatusStore.compareAndSwapStatus(
            jobId, JobStatus.RUNNING, JobStatus.FAILED, timeoutEx.getMessage());
    if (!marked) {
      log.infof("Job %s already in terminal state when timeout handler ran", jobId);
      return;
    }
    log.infof("Job %s marked as FAILED due to hard timeout (retries exhausted)", jobId);
    lifecycleFacade.handlePermanentFailure(job, timeoutEx);
  }

  /**
   * Scans for WAITING jobs whose signal timeout has elapsed and fails them. Should be called
   * periodically (e.g., from the poller tick). No-op if no {@code SignalStore} was wired at
   * construction time.
   */
  public void scanSignalTimeouts() {
    if (signalStore == null) {
      return;
    }
    Instant now = effective().instant();
    List<JobEntity> timedOut = signalStore.findTimedOutSignalJobs(now, signalTimeoutBatchSize);
    for (JobEntity job : timedOut) {
      try {
        processSignalTimeout(job, now);
      } catch (Exception e) {
        log.errorf(e, "Signal timeout post-processing error for job %s", job.getId());
      }
    }
  }

  void processSignalTimeout(JobEntity job, Instant now) {
    UUID jobId = job.getId();
    String message = "Signal timeout exceeded for key: " + job.getSignalKey();
    SignalTimeoutException timeoutEx = new SignalTimeoutException(message);

    int newAttempts = jobRetryStore.incrementRetryAttempt(jobId);
    if (newAttempts < 0) {
      log.infof("Job %s already left WAITING when signal timeout scanner ran", jobId);
      return;
    }

    if (newAttempts <= job.getMaxRetries()) {
      long backoffMs =
          job.getBackoffPolicy() != null
              ? BackoffPolicyHandler.computeDelay(
                  job.getBackoffPolicy(), job.getBackoffParamMs(), newAttempts)
              : 0L;
      Instant retryTime = now.plusMillis(backoffMs);
      boolean rescheduled = jobRetryStore.scheduleJobRetry(jobId, message, retryTime, newAttempts);
      if (rescheduled) {
        publishSignalTimedOutEvent(job, now);
        log.warnf(
            "Job %s signal timed out but has retries remaining (%s/%s) — rescheduled for %s",
            jobId, newAttempts, job.getMaxRetries(), retryTime);
        return;
      }
      log.infof(
          "Job %s signal timed out but was already finalized by a competing path — no DLQ escalation",
          jobId);
      return;
    }

    boolean marked =
        jobBatchStatusStore.compareAndSwapStatus(
            jobId, JobStatus.WAITING, JobStatus.FAILED, message);
    if (!marked) {
      log.infof("Job %s already left WAITING when signal timeout scanner ran", jobId);
      return;
    }

    log.infof("Job %s FAILED due to signal timeout (key=%s)", jobId, job.getSignalKey());
    publishSignalTimedOutEvent(job, now);
    lifecycleFacade.handlePermanentFailure(job, timeoutEx);

    if (chainScheduler != null) {
      chainScheduler.cancelChain(job);
    }
  }

  private void publishSignalTimedOutEvent(JobEntity job, Instant now) {
    if (metricsCollector != null) {
      metricsCollector.signalTimedOut(job.getId(), job.getPublicJobType(), job.getSignalKey());
    }
    if (eventPublisher == null) {
      return;
    }
    Duration elapsed =
        job.getSignalTimeout() != null ? Duration.between(job.getSignalTimeout(), now) : null;
    eventPublisher.publish(
        new JobSignalTimedOutEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            null,
            job.getSignalKey(),
            elapsed != null ? elapsed.abs() : null));
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
      UUID jobId,
      Future<?> future,
      AtomicBoolean softTimeoutSent,
      Instant executionStartTime,
      long timeoutSec) {
    if (!future.isDone() && softTimeoutSent.compareAndSet(false, true)) {
      Duration elapsed = Duration.between(executionStartTime, effective().instant());
      log.warnf(
          "Job %s approaching timeout - %d%% threshold reached. Elapsed: %s, Timeout: %ds",
          jobId, softTimeoutPercent, formatDuration(elapsed), timeoutSec);
    }
  }

  private void handleHardTimeoutById(
      UUID jobId, Future<?> future, Instant executionStartTime, long timeoutSec) {
    if (future.isDone()) {
      return;
    }
    Duration elapsed = Duration.between(executionStartTime, effective().instant());
    log.errorf(
        "Job %s exceeded timeout of %ds. Cancelling execution. Elapsed: %s",
        jobId, timeoutSec, formatDuration(elapsed));

    future.cancel(true);

    try {
      processHardTimeout(jobId, timeoutSec);
    } catch (Exception e) {
      log.errorf(e, "Timeout post-processing error for job %s", jobId);
    }
  }

  /**
   * Cancellable handle bundle for the soft and hard timeout tasks scheduled against a job
   * execution. Callers must invoke {@link #cancel()} on job completion so the tasks do not linger
   * in the scheduler queue until their original fire time.
   */
  public record TimeoutHandles(ScheduledFuture<?> soft, ScheduledFuture<?> hard) {
    public void cancel() {
      if (soft != null) {
        soft.cancel(false);
      }
      if (hard != null) {
        hard.cancel(false);
      }
    }
  }
}

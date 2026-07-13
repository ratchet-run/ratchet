/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.core.internal;

import jakarta.transaction.TransactionSynchronizationRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobExecutionTimedOutEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobSignalTimedOutEvent;
import run.ratchet.api.exception.SignalTimeoutException;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.ri.core.internal.JobWakeupService.AfterCommitRegistrationResult;
import run.ratchet.ri.core.internal.PostExecutionHandler.TerminalTimeoutTransition;
import run.ratchet.spi.ErrorSanitizer;
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

  static final int DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE = 500;
  private static final String SIGNAL_TIMEOUT_LEASE_NAME = "signalTimeoutScan";
  private static final Duration SIGNAL_TIMEOUT_LEASE_TTL = Duration.ofMinutes(2);
  private static final Logger log = Logger.getLogger(JobTimeoutHandler.class);
  private final JobCrudStore jobCrudStore;
  private final JobRetryStore jobRetryStore;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final PostExecutionHandler lifecycleFacade;
  private final InternalEventPublisher eventPublisher;
  private final SignalStore signalStore;
  private final MetricsCollector metricsCollector;
  private final int softTimeoutPercent;
  private final long defaultTimeoutSeconds;
  private final Clock clock;
  private final int signalTimeoutBatchSize;
  private final TransactionSynchronizationRegistry txRegistry;
  private final SingletonLeaseService singletonLeaseService;
  private final ErrorSanitizer errorSanitizer;

  /**
   * Job ids the hard-timeout watchdog has cancelled and is about to retry/finalize itself. The
   * watchdog records the id before it interrupts the worker, so when the interrupt lands in {@link
   * JobTask#handleFailure} the worker can see the timeout is watchdog-owned and skip its own
   * attempt increment. Without this, both the watchdog and the interrupted worker increment while
   * the row is still RUNNING and a single timeout burns two attempts.
   */
  private final Set<UUID> watchdogCancelledJobIds = ConcurrentHashMap.newKeySet();

  protected JobTimeoutHandler() {
    this.jobCrudStore = null;
    this.jobRetryStore = null;
    this.jobBatchStatusStore = null;
    this.lifecycleFacade = null;
    this.eventPublisher = null;
    this.signalStore = null;
    this.metricsCollector = null;
    this.softTimeoutPercent = 0;
    this.defaultTimeoutSeconds = 0;
    this.clock = null;
    this.signalTimeoutBatchSize = 0;
    this.txRegistry = null;
    this.singletonLeaseService = null;
    this.errorSanitizer = null;
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
      SignalStore signalStore,
      MetricsCollector metricsCollector,
      int signalTimeoutBatchSize) {
    this(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        lifecycleFacade,
        softTimeoutPercent,
        defaultTimeoutSeconds,
        clock,
        eventPublisher,
        signalStore,
        metricsCollector,
        signalTimeoutBatchSize,
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
      SignalStore signalStore,
      MetricsCollector metricsCollector,
      int signalTimeoutBatchSize,
      TransactionSynchronizationRegistry txRegistry) {
    this(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        lifecycleFacade,
        softTimeoutPercent,
        defaultTimeoutSeconds,
        clock,
        eventPublisher,
        signalStore,
        metricsCollector,
        signalTimeoutBatchSize,
        txRegistry,
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
      SignalStore signalStore,
      MetricsCollector metricsCollector,
      int signalTimeoutBatchSize,
      TransactionSynchronizationRegistry txRegistry,
      SingletonLeaseService singletonLeaseService) {
    this(
        jobCrudStore,
        jobRetryStore,
        jobBatchStatusStore,
        lifecycleFacade,
        softTimeoutPercent,
        defaultTimeoutSeconds,
        clock,
        eventPublisher,
        signalStore,
        metricsCollector,
        signalTimeoutBatchSize,
        txRegistry,
        singletonLeaseService,
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
      SignalStore signalStore,
      MetricsCollector metricsCollector,
      int signalTimeoutBatchSize,
      TransactionSynchronizationRegistry txRegistry,
      SingletonLeaseService singletonLeaseService,
      ErrorSanitizer errorSanitizer) {
    this.jobCrudStore = jobCrudStore;
    this.jobRetryStore = jobRetryStore;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.lifecycleFacade = lifecycleFacade;
    this.softTimeoutPercent = softTimeoutPercent;
    this.defaultTimeoutSeconds = defaultTimeoutSeconds;
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.eventPublisher = eventPublisher;
    this.signalStore = signalStore;
    this.metricsCollector = metricsCollector;
    this.signalTimeoutBatchSize = Math.max(1, signalTimeoutBatchSize);
    this.txRegistry = txRegistry;
    this.singletonLeaseService = singletonLeaseService;
    this.errorSanitizer = errorSanitizer;
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

  /**
   * Scans for WAITING jobs whose signal timeout has elapsed and fails them. Should be called
   * periodically (e.g., from the poller tick). No-op if no {@code SignalStore} was wired at
   * construction time.
   *
   * <p>The scan runs under a cluster-wide singleton lease, the same coordination the orphan, batch,
   * and dead-letter recoveries use. Every node ticks the poller, so without the lease two nodes
   * scanning the same window both fail and re-increment the same WAITING job, which can also write
   * back a stale lower attempt count. When no {@code LockStore} is present the lease degrades to
   * single-node semantics (always granted), so a core-only store still scans.
   */
  public void scanSignalTimeouts() {
    if (signalStore == null) {
      return;
    }
    if (singletonLeaseService == null) {
      scanSignalTimeoutsWithLease();
      return;
    }
    Optional<SingletonLease> lease =
        singletonLeaseService.tryAcquire(SIGNAL_TIMEOUT_LEASE_NAME, SIGNAL_TIMEOUT_LEASE_TTL);
    if (lease.isEmpty()) {
      log.debug("Signal timeout scan skipped - singleton lease held by another node");
      return;
    }
    try (SingletonLease ignored = lease.get()) {
      scanSignalTimeoutsWithLease();
    }
  }

  private void scanSignalTimeoutsWithLease() {
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

  /** Applies timeout routing: retry if attempts remain, otherwise fail permanently. */
  void processHardTimeout(UUID jobId, long timeoutSec) {
    processHardTimeout(jobId, timeoutSec, Duration.ofSeconds(timeoutSec));
  }

  void processHardTimeout(UUID jobId, long timeoutSec, Duration elapsedTime) {
    Duration observedElapsedTime = elapsedTime.isNegative() ? Duration.ZERO : elapsedTime;
    TimeoutException timeoutEx =
        new TimeoutException("Hard timeout exceeded (" + timeoutSec + "s)");
    lifecycleFacade.handleTimeoutTransition(
        timeoutEx,
        false,
        () -> applyHardTimeoutTransition(jobId, timeoutEx, timeoutSec, observedElapsedTime));
  }

  private Optional<TerminalTimeoutTransition> applyHardTimeoutTransition(
      UUID jobId, TimeoutException timeoutEx, long timeoutSec, Duration observedElapsedTime) {
    String sanitizedError = sanitizeTimeoutError(timeoutEx);
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.infof("Job %s no longer exists when timeout handler ran", jobId);
      return Optional.empty();
    }

    // Step 1: Increment attempts while status is still RUNNING.
    int newAttempts = jobRetryStore.incrementRetryAttempt(jobId);
    if (newAttempts < 0) {
      // Not in RUNNING anymore — worker already transitioned it. Nothing to do.
      log.infof("Job %s already left RUNNING when timeout handler ran", jobId);
      return Optional.empty();
    }

    // Step 2: Retries remain? Try to reschedule.
    if (newAttempts <= job.getMaxRetries()) {
      Instant retryTime = hardTimeoutRetryTime(jobId, timeoutSec, newAttempts);
      boolean rescheduled =
          jobRetryStore.scheduleJobRetry(jobId, sanitizedError, retryTime, newAttempts);
      if (rescheduled) {
        publishHardTimeoutRetryEvents(
            job, sanitizedError, newAttempts, retryTime, timeoutSec, observedElapsedTime);
        log.warnf(
            "Job %s timed out but has retries remaining (%s/%s) — rescheduled for %s",
            jobId, newAttempts, job.getMaxRetries(), retryTime);
        return Optional.empty();
      }
      // scheduleJobRetry returned false — a competing path finalized the job between the
      // increment and the reschedule. Do NOT escalate to DLQ; the job already has a terminal
      // state set by the competing path.
      log.infof(
          "Job %s timed out but was already finalized by a competing path — no DLQ escalation",
          jobId);
      return Optional.empty();
    }

    // Step 3: Retries exhausted — CAS to FAILED and route to DLQ.
    boolean marked =
        jobBatchStatusStore.compareAndSwapStatus(
            jobId, JobStatus.RUNNING, JobStatus.FAILED, sanitizedError);
    if (!marked) {
      log.infof("Job %s already in terminal state when timeout handler ran", jobId);
      return Optional.empty();
    }
    log.infof("Job %s marked as FAILED due to hard timeout (retries exhausted)", jobId);
    job.setAttempts(newAttempts);
    job.setStatus(JobStatus.FAILED);
    job.setLastError(sanitizedError);
    return Optional.of(
        terminalHardTimeoutTransition(
            job, sanitizedError, newAttempts, timeoutSec, observedElapsedTime));
  }

  private String sanitizeTimeoutError(TimeoutException timeout) {
    if (errorSanitizer == null) {
      return timeout.getMessage();
    }
    try {
      String sanitized = errorSanitizer.sanitize(timeout);
      return sanitized != null ? sanitized : timeout.getClass().getName();
    } catch (Throwable sanitizerError) {
      log.warnf(
          sanitizerError,
          "Error sanitizer failed while preparing hard-timeout metadata; using exception class fallback");
      return timeout.getClass().getName();
    }
  }

  void processSignalTimeout(JobEntity job, Instant now) {
    String message = "Signal timeout exceeded for key: " + job.getSignalKey();
    SignalTimeoutException timeoutEx = new SignalTimeoutException(message);

    lifecycleFacade.handleTimeoutTransition(
        timeoutEx, true, () -> applySignalTimeoutTransition(job.getId(), now, message));
  }

  private Optional<TerminalTimeoutTransition> applySignalTimeoutTransition(
      UUID jobId, Instant now, String message) {
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.infof("Job %s no longer exists when signal timeout scanner ran", jobId);
      return Optional.empty();
    }
    int newAttempts = jobRetryStore.incrementRetryAttempt(jobId);
    if (newAttempts < 0) {
      log.infof("Job %s already left WAITING when signal timeout scanner ran", jobId);
      return Optional.empty();
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
        job.setAttempts(newAttempts);
        job.setLastError(message);
        job.setScheduledTime(retryTime);
        job.setStatus(JobStatus.PENDING);
        publishRetryingEvent(job, message, newAttempts, retryTime, now);
        log.warnf(
            "Job %s signal timed out but has retries remaining (%s/%s) — rescheduled for %s",
            jobId, newAttempts, job.getMaxRetries(), retryTime);
        return Optional.empty();
      }
      log.infof(
          "Job %s signal timed out but was already finalized by a competing path — no DLQ escalation",
          jobId);
      return Optional.empty();
    }

    boolean marked =
        jobBatchStatusStore.compareAndSwapStatus(
            jobId, JobStatus.WAITING, JobStatus.FAILED, message);
    if (!marked) {
      log.infof("Job %s already left WAITING when signal timeout scanner ran", jobId);
      return Optional.empty();
    }

    log.infof("Job %s FAILED due to signal timeout (key=%s)", jobId, job.getSignalKey());
    job.setAttempts(newAttempts);
    job.setLastError(message);
    job.setStatus(JobStatus.FAILED);
    return Optional.of(terminalSignalTimeoutTransition(job, message, newAttempts, now));
  }

  private Clock effective() {
    if (clock == null) {
      throw new IllegalStateException("JobTimeoutHandler clock was not initialized");
    }
    return clock;
  }

  private Instant hardTimeoutRetryTime(UUID jobId, long timeoutSec, int attempt) {
    long jitterBoundMs = Math.max(1L, TimeUnit.SECONDS.toMillis(timeoutSec) / 4L);
    long jitterMs = 1L + Math.floorMod((long) Objects.hash(jobId, attempt), jitterBoundMs);
    return effective().instant().plusSeconds(timeoutSec).plusMillis(jitterMs);
  }

  private TerminalTimeoutTransition terminalSignalTimeoutTransition(
      JobEntity job, String errorMessage, int retryAttempt, Instant timestamp) {
    if (metricsCollector != null) {
      metricsCollector.signalTimedOut(job.getId(), job.getPublicJobType(), job.getSignalKey());
    }
    if (eventPublisher == null) {
      return new TerminalTimeoutTransition(job, List.of());
    }
    Duration configuredTimeout =
        job.getCreatedAt() != null && job.getSignalTimeout() != null
            ? Duration.between(job.getCreatedAt(), job.getSignalTimeout())
            : null;
    JobSignalTimedOutEvent event =
        new JobSignalTimedOutEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            job.getSignalKey(),
            configuredTimeout);
    JobFailedEvent failedEvent =
        new JobFailedEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            errorMessage,
            retryAttempt);
    return new TerminalTimeoutTransition(job, List.of(event, failedEvent));
  }

  private AfterCommitRegistrationResult registerAfterCommit(Runnable action) {
    return JobWakeupService.registerAfterCommit(
        resolveTxRegistry(),
        action,
        log,
        "After-commit timeout event registration failed; events suppressed: %s");
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    return txRegistry != null ? txRegistry : JobWakeupService.lookupTxRegistry(log);
  }

  private void publishHardTimeoutRetryEvents(
      JobEntity job,
      String errorMessage,
      int retryAttempt,
      Instant retryTime,
      long timeoutSec,
      Duration elapsedTime) {
    if (eventPublisher == null) {
      return;
    }
    Instant timestamp = effective().instant();
    JobExecutionTimedOutEvent timedOutEvent =
        executionTimedOutEvent(job, timestamp, timeoutSec, elapsedTime, retryAttempt);
    JobRetryingEvent retryingEvent =
        new JobRetryingEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            errorMessage,
            retryAttempt,
            retryTime);
    publishAfterCommit(
        () -> {
          eventPublisher.publish(timedOutEvent);
          eventPublisher.publish(retryingEvent);
        });
  }

  private TerminalTimeoutTransition terminalHardTimeoutTransition(
      JobEntity job, String errorMessage, int retryAttempt, long timeoutSec, Duration elapsedTime) {
    if (eventPublisher == null) {
      return new TerminalTimeoutTransition(job, List.of());
    }
    Instant timestamp = effective().instant();
    JobExecutionTimedOutEvent timedOutEvent =
        executionTimedOutEvent(job, timestamp, timeoutSec, elapsedTime, retryAttempt);
    JobFailedEvent failedEvent =
        new JobFailedEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            errorMessage,
            retryAttempt);
    return new TerminalTimeoutTransition(job, List.of(timedOutEvent, failedEvent));
  }

  private JobExecutionTimedOutEvent executionTimedOutEvent(
      JobEntity job, Instant timestamp, long timeoutSec, Duration elapsedTime, int retryAttempt) {
    return new JobExecutionTimedOutEvent(
        job.getId(),
        job.getBusinessKey(),
        job.getPublicJobType(),
        job.getPriority(),
        job.getPickedBy(),
        timestamp,
        Duration.ofSeconds(timeoutSec),
        elapsedTime,
        retryAttempt);
  }

  private void publishRetryingEvent(
      JobEntity job, String errorMessage, int retryAttempt, Instant retryTime, Instant timestamp) {
    if (eventPublisher == null) {
      return;
    }
    JobRetryingEvent event =
        new JobRetryingEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            errorMessage,
            retryAttempt,
            retryTime);
    publishAfterCommit(() -> eventPublisher.publish(event));
  }

  private void publishAfterCommit(Runnable action) {
    if (registerAfterCommit(action) == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      action.run();
    }
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

    // Claim ownership of the retry/finalize for this timeout BEFORE interrupting the worker, so the
    // interrupt that lands in JobTask.handleFailure already sees the marker and defers to us. The
    // marker is cleared in processHardTimeout's finally once this path is done with it.
    watchdogCancelledJobIds.add(jobId);

    future.cancel(true);

    try {
      processHardTimeout(jobId, timeoutSec, elapsed);
    } catch (Exception e) {
      log.errorf(e, "Timeout post-processing error for job %s", jobId);
      throw new IllegalStateException("Timeout post-processing failed for job " + jobId, e);
    } finally {
      watchdogCancelledJobIds.remove(jobId);
    }
  }

  /**
   * Reports whether the hard-timeout watchdog has claimed this job's timeout retry/finalize. When
   * the interrupted worker sees {@code true} it must skip its own attempt increment and let the
   * watchdog own the transition — otherwise one timeout consumes two attempts. A genuine,
   * non-watchdog interrupt is absent from the set and still counts as a normal failed attempt.
   */
  boolean isWatchdogCancelled(UUID jobId) {
    return watchdogCancelledJobIds.contains(jobId);
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

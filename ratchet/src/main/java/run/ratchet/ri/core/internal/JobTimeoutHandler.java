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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.Nullable;
import run.ratchet.api.event.JobExecutionTimedOutEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobSignalTimedOutEvent;
import run.ratchet.api.exception.SignalTimeoutException;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.ri.core.internal.PostExecutionHandler.TerminalTimeoutTransition;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.SignalStore;

/**
 * Enforces job execution timeouts with three tiers: a soft warning at a configurable percentage
 * (default 80%), an optional cooperative cancellation request at timeout minus grace, and a hard
 * cancel at 100%.
 */
public class JobTimeoutHandler {

  static final int DEFAULT_SIGNAL_TIMEOUT_BATCH_SIZE = 500;
  private static final String SIGNAL_TIMEOUT_LEASE_NAME = "signalTimeoutScan";
  private final Duration signalTimeoutLeaseTtl;
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
  private final long cancellationGraceSeconds;
  private final AfterCommitRegistrar afterCommitRegistrar;
  private final SingletonLeaseService singletonLeaseService;
  private final ErrorSanitizer errorSanitizer;
  // Runs onFailure after a terminal timeout commits: on the hard-timeout watchdog's thread, or on
  // the thread running the signal-timeout scan.
  private final LifecycleCallbackInvoker callbackInvoker;

  /**
   * Job ids the hard-timeout watchdog has cancelled and is about to retry/finalize itself. The
   * watchdog records the id before it interrupts the worker, so when {@link JobTask} handles the
   * resulting failure, the worker can see the timeout is watchdog-owned and skip its own attempt
   * increment. Without this, both the watchdog and the interrupted worker increment while the row
   * is still RUNNING and a single timeout burns two attempts.
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
    this.cancellationGraceSeconds = 0L;
    this.afterCommitRegistrar = null;
    this.singletonLeaseService = null;
    this.errorSanitizer = null;
    this.callbackInvoker = null;
    this.signalTimeoutLeaseTtl = null;
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
      TransactionSynchronizationRegistry txRegistry,
      SingletonLeaseService singletonLeaseService,
      ErrorSanitizer errorSanitizer) {
    this(
        new JakartaAfterCommitRegistrar(txRegistry),
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
        0L,
        singletonLeaseService,
        errorSanitizer,
        null,
        120);
  }

  public JobTimeoutHandler(
      AfterCommitRegistrar afterCommitRegistrar,
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
      long cancellationGraceSeconds,
      SingletonLeaseService singletonLeaseService,
      ErrorSanitizer errorSanitizer,
      LifecycleCallbackInvoker callbackInvoker,
      long signalTimeoutLeaseTtlSeconds) {
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
    this.cancellationGraceSeconds = Math.max(0L, cancellationGraceSeconds);
    this.afterCommitRegistrar = afterCommitRegistrar;
    this.singletonLeaseService = singletonLeaseService;
    this.errorSanitizer = errorSanitizer;
    this.callbackInvoker = callbackInvoker;
    this.signalTimeoutLeaseTtl = Duration.ofSeconds(signalTimeoutLeaseTtlSeconds);
  }

  private long effectiveTimeoutSeconds(int jobTimeoutSec) {
    return jobTimeoutSec > 0 ? jobTimeoutSec : defaultTimeoutSeconds;
  }

  public JobAttemptControl newAttempt(
      UUID jobId,
      int jobTimeoutSec,
      Instant executionStartTime,
      int baselineAttempts,
      long claimSeq,
      @Nullable String nodeId) {
    long timeoutSec = effectiveTimeoutSeconds(jobTimeoutSec);
    return new JobAttemptControl(
        jobId,
        executionStartTime.plusSeconds(timeoutSec),
        timeoutSec,
        executionStartTime,
        baselineAttempts,
        claimSeq,
        nodeId);
  }

  public TimeoutHandles scheduleTimeoutMonitoring(
      JobAttemptControl attempt,
      Future<?> future,
      ScheduledExecutorService scheduler,
      Instant executionStartTime) {
    UUID jobId = attempt.jobId();
    long timeoutSec = attempt.timeoutSeconds();
    AtomicBoolean softTimeoutSent = new AtomicBoolean(false);
    ScheduledFuture<?> soft =
        scheduler.schedule(
            () ->
                handleSoftTimeoutById(
                    jobId, future, softTimeoutSent, executionStartTime, timeoutSec),
            (timeoutSec * softTimeoutPercent) / 100,
            TimeUnit.SECONDS);
    ScheduledFuture<?> cancellationRequest = null;
    if (cancellationGraceSeconds > 0 && cancellationGraceSeconds < timeoutSec) {
      cancellationRequest =
          scheduler.schedule(
              () -> {
                if (!future.isDone()) {
                  attempt.requestCancellation();
                  log.infof(
                      "Job %s requested to stop: %ds before its %ds timeout",
                      jobId, cancellationGraceSeconds, timeoutSec);
                }
              },
              timeoutSec - cancellationGraceSeconds,
              TimeUnit.SECONDS);
    }
    ScheduledFuture<?> hard =
        scheduler.schedule(
            () -> handleHardTimeoutById(attempt, future), timeoutSec, TimeUnit.SECONDS);
    return new TimeoutHandles(soft, cancellationRequest, hard, attempt);
  }

  void processCooperativeTimeout(JobAttemptControl attempt) {
    Duration elapsed = Duration.between(attempt.executionStartTime(), effective().instant());
    log.warnf(
        "Job %s stopped cooperatively after a cancellation request; handling as a timeout",
        attempt.jobId());
    processHardTimeout(
        attempt.jobId(),
        attempt.timeoutSeconds(),
        elapsed,
        attempt.baselineAttempts(),
        true,
        attempt.claimSeq(),
        attempt.nodeId());
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
    // Most polls have no expired signal waits. Avoid writing the shared lease row in that case.
    // This is only a probe: re-read the batch under the lease before processing any jobs.
    if (signalStore.findTimedOutSignalJobs(effective().instant(), 1).isEmpty()) {
      return;
    }
    Optional<SingletonLease> lease =
        singletonLeaseService.tryAcquire(SIGNAL_TIMEOUT_LEASE_NAME, signalTimeoutLeaseTtl);
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

  /** Applies timeout routing without an attempt baseline; each RUNNING re-read counts a failure. */
  void processHardTimeout(
      UUID jobId, long timeoutSec, long expectedClaimSeq, @Nullable String nodeId) {
    processHardTimeout(jobId, timeoutSec, Duration.ofSeconds(timeoutSec), expectedClaimSeq, nodeId);
  }

  void processHardTimeout(
      UUID jobId,
      long timeoutSec,
      Duration elapsedTime,
      long expectedClaimSeq,
      @Nullable String nodeId) {
    processHardTimeout(jobId, timeoutSec, elapsedTime, null, false, expectedClaimSeq, nodeId);
  }

  private void processHardTimeout(
      UUID jobId,
      long timeoutSec,
      Duration elapsedTime,
      @Nullable Integer baselineAttempts,
      boolean workerContextBound,
      long expectedClaimSeq,
      @Nullable String nodeId) {
    Duration observedElapsedTime = elapsedTime.isNegative() ? Duration.ZERO : elapsedTime;
    TimeoutException timeoutEx =
        new TimeoutException("Hard timeout exceeded (" + timeoutSec + "s)");
    // The interrupted worker cannot also run onFailure: it defers to the watchdog marker, gets
    // incrementRetryAttempt == -1, or loses completeFailure's CAS in transitionToDlq.
    runTimeoutTransition(
        timeoutEx,
        false,
        workerContextBound,
        () ->
            applyHardTimeoutTransition(
                jobId,
                timeoutEx,
                timeoutSec,
                observedElapsedTime,
                baselineAttempts,
                expectedClaimSeq,
                nodeId));
  }

  /**
   * Runs a timeout transition and, when it failed the job terminally, invokes the job's {@code
   * onFailure} callback on this thread once {@link PostExecutionHandler#handleTimeoutTransition}
   * returns {@code true}, which happens only after the terminal transition has committed. Under CDI
   * the call goes through the transactional proxy, which commits its {@code REQUIRES_NEW}
   * transaction before returning; with no managed transaction around it, the store commits {@code
   * commitCompletion} itself. This thread has no outer transaction to defer to. Only the path that
   * won the terminal compare-and-swap in {@code commitCompletion} gets a transition back, so the
   * callback runs at most once per terminal transition. A retried or already-finalised job gets
   * none. A cooperative stop reuses the worker's bound context; the other paths bind a context for
   * the callback.
   */
  private void runTimeoutTransition(
      Throwable timeoutEx,
      boolean cancelChainOnFailure,
      boolean workerContextBound,
      Supplier<Optional<TerminalTimeoutTransition>> transition) {
    AtomicReference<JobEntity> terminalJob = new AtomicReference<>();
    boolean committed =
        lifecycleFacade.handleTimeoutTransition(
            timeoutEx,
            cancelChainOnFailure,
            () -> {
              Optional<TerminalTimeoutTransition> outcome = transition.get();
              outcome.ifPresent(terminal -> terminalJob.set(terminal.job()));
              return outcome;
            });
    JobEntity job = terminalJob.get();
    if (committed && callbackInvoker != null && job != null) {
      if (workerContextBound) {
        callbackInvoker.invokeOnFailure(job, timeoutEx);
      } else {
        callbackInvoker.invokeOnFailureInJobContext(job, timeoutEx);
      }
    }
  }

  private Optional<TerminalTimeoutTransition> applyHardTimeoutTransition(
      UUID jobId,
      TimeoutException timeoutEx,
      long timeoutSec,
      Duration observedElapsedTime,
      @Nullable Integer baselineAttempts,
      long expectedClaimSeq,
      @Nullable String nodeId) {
    String sanitizedError = sanitizeTimeoutError(timeoutEx);
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.infof("Job %s no longer exists when timeout handler ran", jobId);
      return Optional.empty();
    }

    if (job.getStatus() != JobStatus.RUNNING) {
      return Optional.empty();
    }
    job.setClaimSeq(expectedClaimSeq);
    job.setPickedBy(nodeId);
    boolean incrementAlreadyApplied =
        baselineAttempts != null && job.getAttempts() > baselineAttempts;
    int newAttempts = incrementAlreadyApplied ? job.getAttempts() : job.getAttempts() + 1;
    if (incrementAlreadyApplied) {
      log.infof(
          "Job %s timeout retry increment was already applied (%s attempts)", jobId, newAttempts);
    }
    // An attempt baseline prevents re-runs from repeating a committed retry increment. Terminal
    // attempts are part of commitCompletion, including for stores without ambient JTA.
    if (!incrementAlreadyApplied && newAttempts <= job.getMaxRetries()) {
      newAttempts = jobRetryStore.incrementRetryAttempt(jobId, expectedClaimSeq);
      if (newAttempts < 0) {
        warnRejectedOwnerWrite(jobId, expectedClaimSeq, nodeId);
        return Optional.empty();
      }
    }
    if (newAttempts <= job.getMaxRetries()) {
      Instant retryTime = hardTimeoutRetryTime(jobId, timeoutSec, newAttempts);
      boolean rescheduled =
          jobRetryStore.scheduleJobRetry(
              jobId, sanitizedError, retryTime, newAttempts, expectedClaimSeq);
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
      warnRejectedOwnerWrite(jobId, expectedClaimSeq, nodeId);
      return Optional.empty();
    }

    // Step 3: Retries exhausted — CAS to FAILED and route to DLQ.
    job.setAttempts(newAttempts);
    job.setLastError(sanitizedError);
    TerminalTimeoutTransition outcome =
        terminalHardTimeoutTransition(
            job, sanitizedError, newAttempts, timeoutSec, observedElapsedTime);
    boolean marked =
        lifecycleFacade.completeTimeoutFailure(
            job, JobStatus.RUNNING, false, timeoutEx, outcome.eventsBeforeDlq());
    if (!marked) {
      return Optional.empty();
    }
    log.infof("Job %s marked as FAILED due to hard timeout (retries exhausted)", jobId);
    job.setAttempts(newAttempts);
    job.setStatus(JobStatus.FAILED);
    job.setLastError(sanitizedError);
    return Optional.of(outcome);
  }

  private void warnRejectedOwnerWrite(UUID jobId, long claimSeq, String nodeId) {
    log.warnf(
        "Rejected owner write for job %s, stale claimSeq %s, node %s", jobId, claimSeq, nodeId);
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
          "Error sanitizer failed while preparing hard-timeout metadata; using exception class"
              + " fallback");
      return timeout.getClass().getName();
    }
  }

  void processSignalTimeout(JobEntity job, Instant now) {
    String message = "Signal timeout exceeded for key: " + job.getSignalKey();
    SignalTimeoutException timeoutEx = new SignalTimeoutException(message);

    runTimeoutTransition(
        timeoutEx, true, false, () -> applySignalTimeoutTransition(job.getId(), now, message));
  }

  private Optional<TerminalTimeoutTransition> applySignalTimeoutTransition(
      UUID jobId, Instant now, String message) {
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.infof("Job %s no longer exists when signal timeout scanner ran", jobId);
      return Optional.empty();
    }
    if (job.getStatus() != JobStatus.WAITING) {
      return Optional.empty();
    }
    int newAttempts = job.getAttempts() + 1;
    if (newAttempts <= job.getMaxRetries()) {
      newAttempts = jobRetryStore.incrementRetryAttempt(jobId, null);
      if (newAttempts < 0) {
        return Optional.empty();
      }
    }
    if (newAttempts <= job.getMaxRetries()) {
      long backoffMs =
          job.getBackoffPolicy() != null
              ? BackoffPolicyHandler.computeDelay(
                  job.getBackoffPolicy(), job.getBackoffParamMs(), newAttempts)
              : 0L;
      Instant retryTime = now.plusMillis(backoffMs);
      boolean rescheduled =
          jobRetryStore.scheduleJobRetry(jobId, message, retryTime, newAttempts, null);
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
          "Job %s signal timed out but was already finalized by a competing path — no DLQ"
              + " escalation",
          jobId);
      return Optional.empty();
    }

    job.setAttempts(newAttempts);
    job.setLastError(message);
    TerminalTimeoutTransition outcome =
        terminalSignalTimeoutTransition(job, message, newAttempts, now);
    boolean marked =
        lifecycleFacade.completeTimeoutFailure(
            job,
            JobStatus.WAITING,
            true,
            new SignalTimeoutException(message),
            outcome.eventsBeforeDlq());
    if (!marked) {
      log.infof("Job %s already left WAITING when signal timeout scanner ran", jobId);
      return Optional.empty();
    }

    log.infof("Job %s FAILED due to signal timeout (key=%s)", jobId, job.getSignalKey());
    job.setAttempts(newAttempts);
    job.setLastError(message);
    job.setStatus(JobStatus.FAILED);
    if (metricsCollector != null) {
      metricsCollector.signalTimedOut(job.getId(), job.getPublicJobType(), job.getSignalKey());
    }
    return Optional.of(outcome);
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
            job.getRecurringMasterId(),
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
            job.getRecurringMasterId(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            errorMessage,
            retryAttempt);
    return new TerminalTimeoutTransition(job, List.of(event, failedEvent));
  }

  private AfterCommitRegistrar.Result registerAfterCommit(Runnable action) {
    return afterCommitRegistrar.registerAfterCommit(action);
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
            job.getRecurringMasterId(),
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
            job.getRecurringMasterId(),
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
        job.getRecurringMasterId(),
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
            job.getRecurringMasterId(),
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
    if (registerAfterCommit(action) == AfterCommitRegistrar.Result.NO_ACTIVE_TRANSACTION) {
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

  private void handleHardTimeoutById(JobAttemptControl attempt, Future<?> future) {
    UUID jobId = attempt.jobId();
    Instant executionStartTime = attempt.executionStartTime();
    long timeoutSec = attempt.timeoutSeconds();
    if (future.isDone() && !attempt.isTimeoutHandedBack()) {
      return;
    }
    if (!attempt.claimTimeoutForWatchdog()) {
      log.debugf("Job %s already stopped cooperatively; the worker owns the timeout", jobId);
      return;
    }
    attempt.requestCancellation();
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
      processHardTimeout(
          jobId,
          timeoutSec,
          elapsed,
          attempt.baselineAttempts(),
          false,
          attempt.claimSeq(),
          attempt.nodeId());
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
   * Cancellable handle bundle for the soft, cancellation-request, and hard timeout tasks scheduled
   * against a job execution. Callers must invoke {@link #cancel()} on job completion so the tasks
   * do not linger in the scheduler queue until their original fire time. A handed-back timeout
   * keeps its hard task so the watchdog can finish the failed cooperative transition.
   */
  public record TimeoutHandles(
      @Nullable ScheduledFuture<?> soft,
      @Nullable ScheduledFuture<?> cancellationRequest,
      ScheduledFuture<?> hard,
      @Nullable JobAttemptControl attempt) {
    public void cancel() {
      if (soft != null) {
        soft.cancel(false);
      }
      if (cancellationRequest != null) {
        cancellationRequest.cancel(false);
      }
      if (hard != null && (attempt == null || !attempt.isTimeoutHandedBack())) {
        hard.cancel(false);
      }
    }
  }
}

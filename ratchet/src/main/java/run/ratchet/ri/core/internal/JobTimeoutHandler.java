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
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobSignalTimedOutEvent;
import run.ratchet.api.exception.SignalTimeoutException;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.ri.core.internal.JobWakeupService.AfterCommitRegistrationResult;
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
  private final ChainScheduler chainScheduler;
  private final SignalStore signalStore;
  private final MetricsCollector metricsCollector;
  private final int softTimeoutPercent;
  private final long defaultTimeoutSeconds;
  private final Clock clock;
  private final int signalTimeoutBatchSize;
  private final TransactionSynchronizationRegistry txRegistry;
  private final SingletonLeaseService singletonLeaseService;

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
    this.chainScheduler = null;
    this.signalStore = null;
    this.metricsCollector = null;
    this.softTimeoutPercent = 0;
    this.defaultTimeoutSeconds = 0;
    this.clock = null;
    this.signalTimeoutBatchSize = 0;
    this.txRegistry = null;
    this.singletonLeaseService = null;
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
        signalTimeoutBatchSize,
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
        chainScheduler,
        signalStore,
        metricsCollector,
        signalTimeoutBatchSize,
        txRegistry,
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
      MetricsCollector metricsCollector,
      int signalTimeoutBatchSize,
      TransactionSynchronizationRegistry txRegistry,
      SingletonLeaseService singletonLeaseService) {
    this.jobCrudStore = jobCrudStore;
    this.jobRetryStore = jobRetryStore;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.lifecycleFacade = lifecycleFacade;
    this.softTimeoutPercent = softTimeoutPercent;
    this.defaultTimeoutSeconds = defaultTimeoutSeconds;
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.eventPublisher = eventPublisher;
    this.chainScheduler = chainScheduler;
    this.signalStore = signalStore;
    this.metricsCollector = metricsCollector;
    this.signalTimeoutBatchSize = Math.max(1, signalTimeoutBatchSize);
    this.txRegistry = txRegistry;
    this.singletonLeaseService = singletonLeaseService;
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
      Instant retryTime = hardTimeoutRetryTime(jobId, timeoutSec, newAttempts);
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
    job.setAttempts(newAttempts);
    job.setStatus(JobStatus.FAILED);
    job.setLastError(timeoutEx.getMessage());
    publishHardTimeoutFailureEvent(job, timeoutEx.getMessage(), newAttempts);
    lifecycleFacade.handlePermanentFailure(job, timeoutEx);
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
        job.setAttempts(newAttempts);
        job.setLastError(message);
        job.setScheduledTime(retryTime);
        job.setStatus(JobStatus.PENDING);
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
    job.setAttempts(newAttempts);
    job.setLastError(message);
    job.setStatus(JobStatus.FAILED);
    publishSignalTimedOutEvent(job);
    lifecycleFacade.handlePermanentFailure(job, timeoutEx);

    if (chainScheduler != null) {
      chainScheduler.cancelChain(job);
    }
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

  private void publishSignalTimedOutEvent(JobEntity job) {
    if (metricsCollector != null) {
      metricsCollector.signalTimedOut(job.getId(), job.getPublicJobType(), job.getSignalKey());
    }
    if (eventPublisher == null) {
      return;
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
            null,
            job.getSignalKey(),
            configuredTimeout);
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  private AfterCommitRegistrationResult registerAfterCommit(Runnable action) {
    return JobWakeupService.registerAfterCommit(
        resolveTxRegistry(),
        action,
        log,
        "After-commit timeout event registration failed; event suppressed: %s");
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    return txRegistry != null ? txRegistry : JobWakeupService.lookupTxRegistry(log);
  }

  private void publishHardTimeoutFailureEvent(
      JobEntity job, String errorMessage, int retryAttempt) {
    if (eventPublisher == null) {
      return;
    }
    JobFailedEvent event =
        new JobFailedEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            errorMessage,
            retryAttempt);
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
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
      processHardTimeout(jobId, timeoutSec);
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

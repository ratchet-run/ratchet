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

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.ri.core.internal.JobWakeupService.AfterCommitRegistrationResult;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Manages the Dead Letter Queue (DLQ): moves permanently failed jobs there and purges old entries
 * on a cron schedule.
 *
 * <p>Internal RI service. Public methods inherit the class-level Jakarta Transactions {@code
 * REQUIRED} behavior unless a method declares a narrower transaction attribute.
 */
@ApplicationScoped
@Transactional
public class DeadLetterService {

  private static final Logger log = Logger.getLogger(DeadLetterService.class);

  private static final String LEASE_NAME = "dlqPurger";
  private static final Duration LEASE_TTL = Duration.ofMinutes(10);

  private final ExecutorProvider executorProvider;
  private final JobBulkStore jobBulkStore;
  private final JobTerminalStore jobTerminalStore;
  private final SingletonLeaseService singletonLeaseService;
  private final InternalEventPublisher eventPublisher;
  private final ErrorSanitizer errorSanitizer;
  private final Clock clock;

  private volatile TransactionSynchronizationRegistry txRegistry;

  private Duration purgeAfter;
  private Cron cron;
  private ZoneId zone;

  private volatile boolean stopped = false;

  protected DeadLetterService() {
    this.executorProvider = null;
    this.jobBulkStore = null;
    this.jobTerminalStore = null;
    this.singletonLeaseService = null;
    this.eventPublisher = null;
    this.errorSanitizer = null;
    this.clock = null;
  }

  public DeadLetterService(
      ExecutorProvider executorProvider,
      JobBulkStore jobBulkStore,
      JobTerminalStore jobTerminalStore,
      SingletonLeaseService singletonLeaseService,
      ErrorSanitizer errorSanitizer) {
    this(
        executorProvider,
        jobBulkStore,
        jobTerminalStore,
        singletonLeaseService,
        null,
        errorSanitizer,
        Clock.systemUTC());
  }

  public DeadLetterService(
      ExecutorProvider executorProvider,
      JobBulkStore jobBulkStore,
      JobTerminalStore jobTerminalStore,
      SingletonLeaseService singletonLeaseService,
      ErrorSanitizer errorSanitizer,
      Clock clock) {
    this(
        executorProvider,
        jobBulkStore,
        jobTerminalStore,
        singletonLeaseService,
        null,
        errorSanitizer,
        clock);
  }

  @Inject
  public DeadLetterService(
      ExecutorProvider executorProvider,
      JobBulkStore jobBulkStore,
      JobTerminalStore jobTerminalStore,
      SingletonLeaseService singletonLeaseService,
      InternalEventPublisher eventPublisher,
      ErrorSanitizer errorSanitizer,
      Clock clock) {
    this.executorProvider = executorProvider;
    this.jobBulkStore = jobBulkStore;
    this.jobTerminalStore = jobTerminalStore;
    this.singletonLeaseService = singletonLeaseService;
    this.eventPublisher = eventPublisher;
    this.errorSanitizer = errorSanitizer;
    this.clock = clock;
  }

  /**
   * Moves a job to the terminal DLQ state.
   *
   * <p>Because this method owns the terminal transition, a winning call publishes the ordered
   * terminal event pair {@link JobFailedEvent} then {@link JobDlqEvent}. Both events are deferred
   * until the transition transaction commits.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}, inherited from the class-level {@link
   * Transactional}.
   *
   * @return {@code true} when this call moved the live job to FAILED, or {@code false} when another
   *     path had already moved it out of RUNNING
   */
  public boolean moveToDlq(JobEntity job, Throwable cause) {
    // Post hot/cold-split: setStatus(FAILED)+save() is rejected by the MySQL store's hot-mutation
    // guard. The terminal transition (DELETE hot + UPDATE cold to FAILED + DELETE bkres) is now
    // a single explicit store call that captures total_attempts atomically.
    String sanitized = sanitizeSafely(cause);
    boolean transitioned =
        jobTerminalStore.markJobFailedTerminal(job.getId(), sanitized, job.getAttempts());
    if (!transitioned) {
      log.debugf("Job %s was already outside RUNNING; DLQ transition skipped", job.getId());
      return false;
    }

    recordServiceOwnedTransition(job, sanitized);
    return true;
  }

  /**
   * Records a DLQ transition that the caller has already applied with a successful status CAS.
   *
   * <p>This path deliberately does not repeat the terminal store mutation. It publishes only the
   * after-commit {@link JobDlqEvent}, because callers that own the successful status CAS also own
   * the preceding {@link JobFailedEvent}. It is reserved for callers that can prove they won the
   * transition race.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRES_NEW}. The caller has already committed the
   * terminal status transition, so event delivery must not be suppressed if later batch or workflow
   * bookkeeping rolls back.
   */
  @Transactional(Transactional.TxType.REQUIRES_NEW)
  public void recordDlqTransition(JobEntity job, Throwable cause) {
    String persistedError = job.getLastError();
    recordCallerOwnedTransition(
        job, persistedError != null ? persistedError : sanitizeSafely(cause));
  }

  private String sanitizeSafely(Throwable cause) {
    try {
      String sanitized = errorSanitizer.sanitize(cause);
      return sanitized != null ? sanitized : fallbackError(cause);
    } catch (Throwable sanitizerError) {
      log.warnf(
          sanitizerError,
          "Error sanitizer failed while preparing DLQ metadata; using exception class fallback");
      return fallbackError(cause);
    }
  }

  private static String fallbackError(Throwable cause) {
    return cause == null ? "null" : cause.getClass().getName();
  }

  private void recordServiceOwnedTransition(JobEntity job, String sanitized) {
    job.setStatus(JobStatus.FAILED);
    job.setLastError(sanitized);
    publishTerminalEvents(job, sanitized);

    log.warnf("Job %s moved to DLQ", job.getId());
  }

  private void recordCallerOwnedTransition(JobEntity job, String sanitized) {
    job.setLastError(sanitized);
    publishDlqEvent(job, sanitized);

    log.warnf("Job %s moved to DLQ", job.getId());
  }

  private void publishTerminalEvents(JobEntity job, String sanitizedError) {
    if (eventPublisher == null) {
      return;
    }
    Instant timestamp = effective().instant();
    int attempts = Math.max(0, job.getAttempts());
    JobFailedEvent failedEvent =
        new JobFailedEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            sanitizedError,
            attempts);
    JobDlqEvent dlqEvent =
        new JobDlqEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            sanitizedError,
            attempts);
    publishAfterCommit(
        () -> {
          eventPublisher.publish(failedEvent);
          eventPublisher.publish(dlqEvent);
        });
  }

  private void publishDlqEvent(JobEntity job, String sanitizedError) {
    if (eventPublisher == null) {
      return;
    }
    JobDlqEvent event =
        new JobDlqEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            effective().instant(),
            sanitizedError,
            Math.max(0, job.getAttempts()));
    publishAfterCommit(() -> eventPublisher.publish(event));
  }

  private void publishAfterCommit(Runnable action) {
    if (registerAfterCommit(action) == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      action.run();
    }
  }

  private AfterCommitRegistrationResult registerAfterCommit(Runnable action) {
    return JobWakeupService.registerAfterCommit(
        resolveTxRegistry(),
        action,
        log,
        "After-commit DLQ event registration failed; events suppressed: %s");
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    TransactionSynchronizationRegistry reg = txRegistry;
    if (reg == null) {
      synchronized (this) {
        reg = txRegistry;
        if (reg == null) {
          reg = JobWakeupService.lookupTxRegistry(log);
          txRegistry = reg;
        }
      }
    }
    return reg;
  }

  void setTxRegistryForTesting(TransactionSynchronizationRegistry txRegistry) {
    this.txRegistry = txRegistry;
  }

  /**
   * Stops future DLQ purge scheduling.
   *
   * <p><b>Transaction attribute:</b> {@code NOT_SUPPORTED}. Called from {@code @PreDestroy} during
   * shutdown to flip a volatile lifecycle flag; opening a JDBC transaction just for that risks
   * spurious connection-pool activity when the pool is already tearing down.
   */
  @Transactional(Transactional.TxType.NOT_SUPPORTED)
  public void stop() {
    stopped = true;
  }

  /**
   * Configures DLQ retention and schedules the first purge tick.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}, inherited from the class-level {@link
   * Transactional}.
   */
  public void init(long purgeDays, Cron cronExpression) {
    this.purgeAfter = Duration.ofDays(purgeDays);
    this.cron = cronExpression;
    this.zone = ZoneId.systemDefault();

    scheduleNext();

    log.infof("DeadLetterService scheduled DLQ purge (retention=%s days)", purgeDays);
  }

  /**
   * Runs one scheduled purge tick.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED} when invoked through a CDI proxy. Scheduled
   * executor callbacks invoke this instance directly, so this method does not rely on the
   * interceptor boundary for correctness.
   */
  void run() {
    try {
      purge();
    } finally {
      scheduleNextAfterRun();
    }
  }

  /**
   * Deletes expired DLQ rows when this node holds the purge lease.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED} when invoked through a CDI proxy. The
   * scheduled callback path is direct and treats purge failures as logged background-task failures.
   */
  void purge() {
    try {
      Optional<SingletonLease> lease = singletonLeaseService.tryAcquire(LEASE_NAME, LEASE_TTL);
      if (lease.isEmpty()) {
        log.debug("DLQ purge skipped - singleton lease held by another node");
        return;
      }

      try (SingletonLease ignored = lease.get()) {
        Instant cutoff = effective().instant().minus(purgeAfter);
        int deleted = jobBulkStore.deleteDlqOlderThan(cutoff);
        if (deleted > 0) {
          log.infof("Purged %s DLQ rows older than %s", deleted, cutoff);
        }
      }
    } catch (Exception e) {
      log.error("DLQ purge failed", e);
    }
  }

  private void scheduleNextAfterRun() {
    try {
      scheduleNext();
    } catch (RuntimeException e) {
      log.warnf(e, "DLQ purge reschedule failed");
    }
  }

  private void scheduleNext() {
    if (stopped) {
      return;
    }
    Instant now = effective().instant();
    Optional<Instant> next =
        ExecutionTime.forCron(cron).nextExecution(now.atZone(zone)).map(ZonedDateTime::toInstant);

    next.ifPresent(
        instant ->
            executorProvider
                .getScheduledExecutor()
                .schedule(
                    this::run, Duration.between(now, instant).toMillis(), TimeUnit.MILLISECONDS));
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}

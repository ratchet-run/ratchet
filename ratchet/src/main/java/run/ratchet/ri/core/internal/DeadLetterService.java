package run.ratchet.ri.core.internal;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import run.ratchet.ri.core.SingletonLease;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
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
  private static final Duration ALERT_DEDUP_WINDOW = Duration.ofHours(1);

  private final ExecutorProvider executorProvider;
  private final JobCrudStore jobCrudStore;
  private final JobBulkStore jobBulkStore;
  private final JobTerminalStore jobTerminalStore;
  private final SingletonLeaseService singletonLeaseService;
  private final DlqAlertStore dlqAlertStore;
  private final InternalEventPublisher eventPublisher;
  private final ErrorSanitizer errorSanitizer;
  private final Clock clock;

  private Duration purgeAfter;
  private Cron cron;
  private ZoneId zone;

  private volatile boolean stopped = false;

  protected DeadLetterService() {
    this.executorProvider = null;
    this.jobCrudStore = null;
    this.jobBulkStore = null;
    this.jobTerminalStore = null;
    this.singletonLeaseService = null;
    this.dlqAlertStore = null;
    this.eventPublisher = null;
    this.errorSanitizer = null;
    this.clock = null;
  }

  public DeadLetterService(
      ExecutorProvider executorProvider,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      JobTerminalStore jobTerminalStore,
      SingletonLeaseService singletonLeaseService,
      DlqAlertStore dlqAlertStore,
      InternalEventPublisher eventPublisher,
      ErrorSanitizer errorSanitizer) {
    this(
        executorProvider,
        jobCrudStore,
        jobBulkStore,
        jobTerminalStore,
        singletonLeaseService,
        dlqAlertStore,
        eventPublisher,
        errorSanitizer,
        Clock.systemUTC());
  }

  @Inject
  public DeadLetterService(
      ExecutorProvider executorProvider,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      JobTerminalStore jobTerminalStore,
      SingletonLeaseService singletonLeaseService,
      DlqAlertStore dlqAlertStore,
      InternalEventPublisher eventPublisher,
      ErrorSanitizer errorSanitizer,
      Clock clock) {
    this.executorProvider = executorProvider;
    this.jobCrudStore = jobCrudStore;
    this.jobBulkStore = jobBulkStore;
    this.jobTerminalStore = jobTerminalStore;
    this.singletonLeaseService = singletonLeaseService;
    this.dlqAlertStore = dlqAlertStore;
    this.eventPublisher = eventPublisher;
    this.errorSanitizer = errorSanitizer;
    this.clock = clock;
  }

  /**
   * Moves a job to the terminal DLQ state.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}, inherited from the class-level {@link
   * Transactional}.
   */
  public void moveToDlq(JobEntity job, Throwable cause) {
    // Post hot/cold-split: setStatus(FAILED)+save() is rejected by the MySQL store's hot-mutation
    // guard. The terminal transition (DELETE hot + UPDATE cold to FAILED + DELETE bkres) is now
    // a single explicit store call that captures total_attempts atomically.
    String sanitized = errorSanitizer.sanitize(cause);
    jobTerminalStore.markJobFailedTerminal(job.getId(), sanitized, job.getAttempts());
    job.setLastError(sanitized);

    recordDlqAlert(job, cause);

    log.warnf("Job %s moved to DLQ", job.getId());
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

  private void recordDlqAlert(JobEntity job, Throwable cause) {
    try {
      String errorHash = hashError(cause);
      Instant now = effective().instant();
      Instant cutoff = now.minus(ALERT_DEDUP_WINDOW);

      if (dlqAlertStore.existsRecentDlqAlert(job.getId(), errorHash, cutoff)) {
        log.debugf("DLQ alert suppressed for job %s (duplicate within window)", job.getId());
        return;
      }

      DlqAlertEntity alert = new DlqAlertEntity();
      alert.setJobId(job.getId());
      alert.setErrorHash(errorHash);
      alert.setAlertSentAt(now);
      alert.setAlertChannel("system");
      dlqAlertStore.saveDlqAlert(alert);
    } catch (Exception e) {
      log.warnf(e, "DLQ alert error for job %s", job.getId());
    }
  }

  private String hashError(Throwable cause) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] hash = md.digest(cause.toString().getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash, 0, 8);
    } catch (Exception e) {
      return cause.getClass().getSimpleName();
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

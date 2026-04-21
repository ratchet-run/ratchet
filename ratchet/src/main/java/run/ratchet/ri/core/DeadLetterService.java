package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobTerminalStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

/**
 * Manages the Dead Letter Queue (DLQ): moves permanently failed jobs there and purges old entries
 * on a cron schedule.
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
      ErrorSanitizer errorSanitizer) {
    this.executorProvider = executorProvider;
    this.jobCrudStore = jobCrudStore;
    this.jobBulkStore = jobBulkStore;
    this.jobTerminalStore = jobTerminalStore;
    this.singletonLeaseService = singletonLeaseService;
    this.dlqAlertStore = dlqAlertStore;
    this.eventPublisher = eventPublisher;
    this.errorSanitizer = errorSanitizer;
  }

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

  public void stop() {
    stopped = true;
  }

  public void init(long purgeDays, Cron cronExpression) {
    this.purgeAfter = Duration.ofDays(purgeDays);
    this.cron = cronExpression;
    this.zone = ZoneId.systemDefault();

    scheduleNext();

    log.infof("DeadLetterService scheduled DLQ purge (retention=%s days)", purgeDays);
  }

  void run() {
    try {
      purge();
    } finally {
      scheduleNext();
    }
  }

  void purge() {
    try {
      Optional<SingletonLease> lease = singletonLeaseService.tryAcquire(LEASE_NAME, LEASE_TTL);
      if (lease.isEmpty()) {
        log.debug("DLQ purge skipped - singleton lease held by another node");
        return;
      }

      try (SingletonLease ignored = lease.get()) {
        Instant cutoff = Instant.now().minus(purgeAfter);
        int deleted = jobBulkStore.deleteDlqOlderThan(cutoff);
        if (deleted > 0) {
          log.infof("Purged %s DLQ rows older than %s", deleted, cutoff);
        }
      }
    } catch (Exception e) {
      log.error("DLQ purge failed", e);
    }
  }

  private void recordDlqAlert(JobEntity job, Throwable cause) {
    try {
      String errorHash = hashError(cause);
      Instant cutoff = Instant.now().minus(ALERT_DEDUP_WINDOW);

      if (dlqAlertStore.existsRecentDlqAlert(job.getId(), errorHash, cutoff)) {
        log.debugf("DLQ alert suppressed for job %s (duplicate within window)", job.getId());
        return;
      }

      DlqAlertEntity alert = new DlqAlertEntity();
      alert.setJobId(job.getId());
      alert.setErrorHash(errorHash);
      alert.setAlertSentAt(Instant.now());
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
    Instant now = Instant.now();
    Optional<Instant> next =
        ExecutionTime.forCron(cron).nextExecution(now.atZone(zone)).map(ZonedDateTime::toInstant);

    next.ifPresent(
        instant ->
            executorProvider
                .getScheduledExecutor()
                .schedule(
                    this::run, Duration.between(now, instant).toMillis(), TimeUnit.MILLISECONDS));
  }
}

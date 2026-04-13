package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.LockStore;
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

  private static final String LOCK_NAME = "dlqPurger";
  private static final Duration ALERT_DEDUP_WINDOW = Duration.ofHours(1);

  private final ExecutorProvider executorProvider;
  private final JobCrudStore jobCrudStore;
  private final JobBulkStore jobBulkStore;
  private final LockStore lockStore;
  private final DlqAlertStore dlqAlertStore;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final InternalEventPublisher eventPublisher;
  private final ErrorSanitizer errorSanitizer;

  private Duration purgeAfter;
  private Cron cron;
  private ZoneId zone;

  /** Prevents re-scheduling after shutdown. */
  private volatile boolean stopped = false;

  // Required by CDI proxy
  protected DeadLetterService() {
    this.executorProvider = null;
    this.jobCrudStore = null;
    this.jobBulkStore = null;
    this.lockStore = null;
    this.dlqAlertStore = null;
    this.nodeIdentityProvider = null;
    this.eventPublisher = null;
    this.errorSanitizer = null;
  }

  @Inject
  public DeadLetterService(
      ExecutorProvider executorProvider,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      LockStore lockStore,
      DlqAlertStore dlqAlertStore,
      NodeIdentityProvider nodeIdentityProvider,
      InternalEventPublisher eventPublisher,
      ErrorSanitizer errorSanitizer) {
    this.executorProvider = executorProvider;
    this.jobCrudStore = jobCrudStore;
    this.jobBulkStore = jobBulkStore;
    this.lockStore = lockStore;
    this.dlqAlertStore = dlqAlertStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.eventPublisher = eventPublisher;
    this.errorSanitizer = errorSanitizer;
  }

  /**
   * Moves a permanently failed job to the Dead Letter Queue (DLQ).
   *
   * @param job the {@link JobEntity} representing the job to be moved to the DLQ
   * @param cause the {@link Throwable} representing the reason for the failure
   */
  public void moveToDlq(JobEntity job, Throwable cause) {
    job.setStatus(JobStatus.FAILED);
    job.setLastError(errorSanitizer.sanitize(cause));
    jobCrudStore.save(job);

    recordDlqAlert(job, cause);

    log.warnf("Job %s moved to DLQ", job.getId());
  }

  /**
   * Records a DLQ alert for audit trail and duplicate suppression.
   *
   * <p>Uses an error hash to deduplicate alerts — if the same job+error combination has already
   * been recorded within the dedup window, the alert is suppressed. This prevents notification
   * storms when the same error occurs repeatedly.
   */
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
      log.warnf(e, "Failed to record DLQ alert for job %s", job.getId());
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

  /**
   * Stops the DLQ purge scheduler. Cron-based scheduling uses one-shot delays; this flag prevents
   * re-scheduling after the current run completes.
   */
  public void stop() {
    stopped = true;
  }

  /**
   * Initializes the DeadLetterService with cron-based purge scheduling.
   *
   * @param purgeDays number of days to retain DLQ entries
   * @param cronExpression cron expression for scheduling (Quartz format)
   */
  public void init(long purgeDays, Cron cronExpression) {
    this.purgeAfter = Duration.ofDays(purgeDays);
    this.cron = cronExpression;
    this.zone = ZoneId.systemDefault();

    scheduleNext();

    log.infof("DeadLetterService scheduled DLQ purge (retention=%s days)", purgeDays);
  }

  /** Purges old DLQ entries and schedules the next run. */
  void run() {
    try {
      purge();
    } finally {
      scheduleNext();
    }
  }

  /** Removes old entries from the Dead Letter Queue. */
  void purge() {
    try {
      if (!lockStore.tryLock(LOCK_NAME, Duration.ofMinutes(10), nodeIdentityProvider.getNodeId())) {
        log.debug("DLQ purge skipped - lock held by another node");
        return;
      }

      Instant cutoff = Instant.now().minus(purgeAfter);
      int deleted = jobBulkStore.deleteDlqOlderThan(cutoff);
      if (deleted > 0) {
        log.infof("Purged %s DLQ rows older than %s", deleted, cutoff);
      }
    } catch (Exception e) {
      log.error("DLQ purge failed", e);
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

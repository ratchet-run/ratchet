package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service responsible for managing the Dead Letter Queue (DLQ) for permanently failed jobs. The DLQ
 * serves as a final destination for jobs that have exhausted all retry attempts and cannot be
 * processed successfully, providing a mechanism for audit, troubleshooting, and manual
 * intervention.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li>Moving permanently failed jobs to the DLQ with detailed error information
 *   <li>Automatically purging old DLQ entries based on configurable retention period
 *   <li>Preventing DLQ growth from consuming excessive database storage
 * </ul>
 *
 * @see JobStatus#FAILED for DLQ job status
 */
@ApplicationScoped
@Transactional
public class DeadLetterService {

  private static final Logger log = Logger.getLogger(DeadLetterService.class.getName());

  /** Distributed lock name used to coordinate DLQ purge operations across cluster nodes. */
  private static final String LOCK_NAME = "dlqPurger";

  /** Rate-limit window for duplicate DLQ alert suppression. */
  private static final Duration ALERT_DEDUP_WINDOW = Duration.ofHours(1);

  /** Provider for scheduled executor services. */
  private final ExecutorProvider executorProvider;

  /** Store for job entity operations. */
  private final JobCrudStore jobCrudStore;

  /** Store for bulk job operations (DLQ purge). */
  private final JobBulkStore jobBulkStore;

  /** Store for distributed lock operations. */
  private final LockStore lockStore;

  /** Store for DLQ alert audit trail and rate-limiting. */
  private final DlqAlertStore dlqAlertStore;

  /** Provider for the unique identifier of this cluster node. */
  private final NodeIdentityProvider nodeIdentityProvider;

  /** Publisher for DLQ-related events. */
  private final InternalEventPublisher eventPublisher;

  /** Retention period for DLQ entries before automatic purging. */
  private Duration purgeAfter;

  private Cron cron;
  private ZoneId zone;

  /** Set to true during shutdown to prevent re-scheduling after the current run completes. */
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
  }

  @Inject
  public DeadLetterService(
      ExecutorProvider executorProvider,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      LockStore lockStore,
      DlqAlertStore dlqAlertStore,
      NodeIdentityProvider nodeIdentityProvider,
      InternalEventPublisher eventPublisher) {
    this.executorProvider = executorProvider;
    this.jobCrudStore = jobCrudStore;
    this.jobBulkStore = jobBulkStore;
    this.lockStore = lockStore;
    this.dlqAlertStore = dlqAlertStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.eventPublisher = eventPublisher;
  }

  /**
   * Moves a permanently failed job to the Dead Letter Queue (DLQ).
   *
   * @param job the {@link JobEntity} representing the job to be moved to the DLQ
   * @param cause the {@link Throwable} representing the reason for the failure
   */
  public void moveToDlq(JobEntity job, Throwable cause) {
    job.setStatus(JobStatus.FAILED);
    job.setLastError(cause.toString());
    jobCrudStore.save(job);

    recordDlqAlert(job, cause);

    log.warning("Job " + job.getId() + " moved to DLQ");
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
        log.fine("DLQ alert suppressed for job " + job.getId() + " (duplicate within window)");
        return;
      }

      DlqAlertEntity alert = new DlqAlertEntity();
      alert.setJobId(job.getId());
      alert.setErrorHash(errorHash);
      alert.setAlertSentAt(Instant.now());
      alert.setAlertChannel("system");
      dlqAlertStore.saveDlqAlert(alert);
    } catch (Exception e) {
      log.log(Level.WARNING, "Failed to record DLQ alert for job " + job.getId(), e);
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

    log.info("DeadLetterService scheduled DLQ purge (retention=" + purgeDays + " days)");
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
        log.fine("DLQ purge skipped - lock held by another node");
        return;
      }

      Instant cutoff = Instant.now().minus(purgeAfter);
      int deleted = jobBulkStore.deleteDlqOlderThan(cutoff);
      if (deleted > 0) {
        log.info("Purged " + deleted + " DLQ rows older than " + cutoff);
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "DLQ purge failed", e);
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

package run.ratchet.ri.core;

import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.LockStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
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

  /** Provider for scheduled executor services. */
  private final ExecutorProvider executorProvider;

  /** Store for job entity operations. */
  private final JobCrudStore jobCrudStore;

  /** Store for bulk job operations (DLQ purge). */
  private final JobBulkStore jobBulkStore;

  /** Store for distributed lock operations. */
  private final LockStore lockStore;

  /** Provider for the unique identifier of this cluster node. */
  private final NodeIdentityProvider nodeIdentityProvider;

  /** Publisher for DLQ-related events. */
  private final InternalEventPublisher eventPublisher;

  /** Retention period for DLQ entries before automatic purging. */
  private Duration purgeAfter;

  // Required by CDI proxy
  protected DeadLetterService() {
    this.executorProvider = null;
    this.jobCrudStore = null;
    this.jobBulkStore = null;
    this.lockStore = null;
    this.nodeIdentityProvider = null;
    this.eventPublisher = null;
  }

  @Inject
  public DeadLetterService(
      ExecutorProvider executorProvider,
      JobCrudStore jobCrudStore,
      JobBulkStore jobBulkStore,
      LockStore lockStore,
      NodeIdentityProvider nodeIdentityProvider,
      InternalEventPublisher eventPublisher) {
    this.executorProvider = executorProvider;
    this.jobCrudStore = jobCrudStore;
    this.jobBulkStore = jobBulkStore;
    this.lockStore = lockStore;
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

    log.warning("Job " + job.getId() + " moved to DLQ");
  }

  /**
   * Initializes the DeadLetterService by calculating the retention period and scheduling the purge
   * operation to run daily at 2 AM.
   *
   * @param purgeDays number of days to retain DLQ entries
   */
  public void init(long purgeDays) {
    purgeAfter = Duration.ofDays(purgeDays);

    long first = computeDelayTo2am();
    executorProvider
        .getScheduledExecutor()
        .scheduleAtFixedRate(this::purge, first, TimeUnit.DAYS.toMillis(1), TimeUnit.MILLISECONDS);
    log.info("DeadLetterService scheduled purge every 24h (retention=" + purgeDays + " days)");
  }

  /** Removes old entries from the Dead Letter Queue. */
  void purge() {
    if (!lockStore.tryLock(LOCK_NAME, Duration.ofMinutes(10), nodeIdentityProvider.getNodeId())) {
      log.fine("DLQ purge skipped - lock held by another node");
      return;
    }

    Instant cutoff = Instant.now().minus(purgeAfter);
    int deleted = jobBulkStore.deleteDlqOlderThan(cutoff);
    if (deleted > 0) {
      log.info("Purged " + deleted + " DLQ rows older than " + cutoff);
    }
  }

  /**
   * Computes the delay in milliseconds from the current time until the next occurrence of 2:00 AM.
   *
   * @return the delay in milliseconds until the next 2:00 AM
   */
  private long computeDelayTo2am() {
    var now = ZonedDateTime.now();
    var next = now.withHour(2).withMinute(0).withSecond(0).withNano(0);
    if (!next.isAfter(now)) {
      next = next.plusDays(1);
    }
    return Duration.between(now, next).toMillis();
  }
}

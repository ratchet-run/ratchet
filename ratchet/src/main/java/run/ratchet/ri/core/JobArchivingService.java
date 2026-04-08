package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.LockStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

/**
 * Manages the lifecycle and retention of job execution history through automated archiving. This
 * service transitions completed jobs from the active scheduler table to long-term archive storage,
 * maintaining system performance while preserving audit trails.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li><b>Active Table Management:</b> Moves completed jobs older than retention period to archive
 *       tables
 *   <li><b>Archive Lifecycle:</b> Maintains archived jobs for extended periods (3x retention)
 *       before final purging
 *   <li><b>Performance Optimization:</b> Batch processing with configurable sizes
 *   <li><b>Distributed Coordination:</b> Uses cluster-wide locking to ensure single-node execution
 * </ul>
 *
 * @see ArchiveStore for archive storage operations
 */
@ApplicationScoped
@Transactional
public class JobArchivingService {

  private static final Logger log = Logger.getLogger(JobArchivingService.class);

  private static final String ARCHIVED_BY_SYSTEM = "system";
  private static final String ARCHIVE_REASON_RETENTION = "retention_policy";
  private static final String LOCK_NAME = "jobArchiver";

  private final JobBulkStore jobBulkStore;
  private final ArchiveStore archiveStore;
  private final LockStore lockStore;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final ExecutorProvider executorProvider;

  private Duration retentionPeriod;
  private int batchSize;
  private Cron cron;
  private ZoneId zone;
  private boolean enabled;

  /** Set to true during shutdown to prevent re-scheduling after the current run completes. */
  private volatile boolean stopped = false;

  // Required by CDI proxy
  protected JobArchivingService() {
    this.jobBulkStore = null;
    this.archiveStore = null;
    this.lockStore = null;
    this.nodeIdentityProvider = null;
    this.executorProvider = null;
  }

  @Inject
  public JobArchivingService(
      JobBulkStore jobBulkStore,
      ArchiveStore archiveStore,
      LockStore lockStore,
      NodeIdentityProvider nodeIdentityProvider,
      ExecutorProvider executorProvider) {
    this.jobBulkStore = jobBulkStore;
    this.archiveStore = archiveStore;
    this.lockStore = lockStore;
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.executorProvider = executorProvider;
  }

  /**
   * Stops the archiving scheduler. Cron-based scheduling uses one-shot delays; this flag prevents
   * re-scheduling after the current run completes.
   */
  public void stop() {
    stopped = true;
  }

  /**
   * Initializes the archiving service with the given configuration.
   *
   * @param enabled whether archiving is enabled
   * @param retentionDays retention period in days
   * @param batchSize number of jobs per archiving batch
   * @param cronExpression cron expression for scheduling
   */
  public void init(boolean enabled, long retentionDays, int batchSize, Cron cronExpression) {
    this.enabled = enabled;

    if (!enabled) {
      log.info("Job archiving is disabled");
      return;
    }

    this.cron = cronExpression;
    this.zone = ZoneId.systemDefault();
    this.retentionPeriod = Duration.ofDays(retentionDays);
    this.batchSize = batchSize;

    scheduleNext();

    log.info(
        String.format(
            "Job archiving service initialized: enabled=%s, retention=%s days, batch=%d",
            enabled, retentionDays, batchSize));
  }

  /** Manually triggers an archiving run outside the normal schedule. */
  public void triggerArchiving() {
    if (!enabled) {
      log.warn("Cannot trigger archiving: service is disabled");
      return;
    }

    log.info("Manual archiving trigger requested");
    executorProvider.getJobExecutor().submit(this::performArchiving);
  }

  /** Main archiving operation that processes eligible jobs in batches. */
  void run() {
    if (!enabled) {
      log.debug("Job archiving is disabled, skipping run");
      return;
    }

    // Try to acquire leader lock with 2-hour TTL
    if (!lockStore.tryLock(LOCK_NAME, Duration.ofHours(2), nodeIdentityProvider.getNodeId())) {
      log.debug("Another node is running job archiving, skipping");
      return;
    }

    try {
      performArchiving();
    } catch (Exception e) {
      log.error("Job archiving failed", e);
    } finally {
      scheduleNext();
    }
  }

  private void performArchiveCleanup() {
    Duration archiveRetention = retentionPeriod.multipliedBy(3);
    Instant archiveCutoff = Instant.now().minus(archiveRetention);

    try {
      int purged = archiveStore.purgeArchivedJobs(archiveCutoff);
      if (purged > 0) {
        log.infof("Purged %s archived jobs older than %s", purged, archiveCutoff);
      }
    } catch (Exception e) {
      log.error("Failed to purge old archived jobs", e);
    }
  }

  private void performArchiving() {
    Instant cutoffTime = Instant.now().minus(retentionPeriod);

    log.infof("Starting job archiving for jobs older than %s", cutoffTime);

    long totalEligible = archiveStore.countJobsForArchiving(cutoffTime);
    if (totalEligible == 0) {
      log.info("No jobs found for archiving");
      return;
    }

    log.infof("Found %s jobs eligible for archiving", totalEligible);

    int totalArchived = 0;
    int batchCount = 0;
    boolean hasMore = true;

    while (hasMore) {
      batchCount++;
      List<JobEntity> batch = archiveStore.findJobsForArchiving(cutoffTime, batchSize);

      if (batch.isEmpty()) {
        break;
      }

      try {
        int archivedCount =
            archiveStore.archiveJobsBatch(batch, ARCHIVE_REASON_RETENTION, ARCHIVED_BY_SYSTEM);

        if (archivedCount > 0) {
          List<Long> jobIds = batch.stream().limit(archivedCount).map(JobEntity::getId).toList();

          int deletedCount = jobBulkStore.deleteJobsByIds(jobIds);

          totalArchived += archivedCount;

          log.infof(
              "Batch %s: Archived %s jobs, deleted %s from active table",
              batchCount, archivedCount, deletedCount);
        }

        if (batch.size() < batchSize) {
          hasMore = false;
        }

      } catch (Exception e) {
        log.errorf(e, "Failed to process archiving batch %s", batchCount);
      }
    }

    if (totalArchived > 0) {
      log.infof(
          "Job archiving completed: %s jobs archived in %s batches", totalArchived, batchCount);
    }

    performArchiveCleanup();
  }

  private void scheduleNext() {
    if (!enabled || stopped) {
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

package run.ratchet.ri.core;

import com.cronutils.model.Cron;
import com.cronutils.model.time.ExecutionTime;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.JobBulkStore;

/**
 * Moves completed jobs older than the retention period to archive storage, then purges archived
 * jobs older than 3x the retention period. Runs on a cron schedule with a singleton lease to
 * prevent duplicate runs across nodes.
 */
@ApplicationScoped
@Transactional
public class JobArchivingService {

  private static final Logger log = Logger.getLogger(JobArchivingService.class);

  private static final String ARCHIVED_BY_SYSTEM = "system";
  private static final String ARCHIVE_REASON_RETENTION = "retention_policy";
  private static final String LEASE_NAME = "jobArchiver";
  private static final Duration LEASE_TTL = Duration.ofHours(2);

  private final JobBulkStore jobBulkStore;
  private final ArchiveStore archiveStore;
  private final SingletonLeaseService singletonLeaseService;
  private final ExecutorProvider executorProvider;

  private Duration retentionPeriod;
  private int batchSize;
  private Cron cron;
  private ZoneId zone;
  private boolean enabled;

  private volatile boolean stopped = false;

  protected JobArchivingService() {
    this.jobBulkStore = null;
    this.archiveStore = null;
    this.singletonLeaseService = null;
    this.executorProvider = null;
  }

  @Inject
  public JobArchivingService(
      JobBulkStore jobBulkStore,
      ArchiveStore archiveStore,
      SingletonLeaseService singletonLeaseService,
      ExecutorProvider executorProvider) {
    this.jobBulkStore = jobBulkStore;
    this.archiveStore = archiveStore;
    this.singletonLeaseService = singletonLeaseService;
    this.executorProvider = executorProvider;
  }

  public void stop() {
    stopped = true;
  }

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

    log.infof(
        "Job archiving service initialized: retention=%s days, batch=%d", retentionDays, batchSize);
  }

  @Transactional(TxType.NOT_SUPPORTED)
  public Future<?> triggerArchiving() {
    if (!enabled) {
      log.warn("Cannot trigger archiving: service is disabled");
      return CompletableFuture.completedFuture(null);
    }

    log.info("Manual archiving trigger requested");
    return executorProvider.getJobExecutor().submit(this::performArchivingWithLease);
  }

  void run() {
    if (!enabled) {
      log.debug("Job archiving is disabled, skipping run");
      return;
    }

    try {
      performArchivingWithLease();
    } catch (Exception e) {
      log.error("Job archiving failed", e);
    } finally {
      scheduleNext();
    }
  }

  private void performArchivingWithLease() {
    Optional<SingletonLease> lease = singletonLeaseService.tryAcquire(LEASE_NAME, LEASE_TTL);
    if (lease.isEmpty()) {
      log.debug("Job archiving skipped - singleton lease held by another node");
      return;
    }

    try (SingletonLease ignored = lease.get()) {
      performArchiving();
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
      log.error("Archive purge error", e);
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
          List<UUID> jobIds = batch.stream().limit(archivedCount).map(JobEntity::getId).toList();

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
        break;
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

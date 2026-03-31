package run.ratchet.ri.cdi;

import com.cronutils.model.Cron;
import run.ratchet.ri.core.BatchRecoveryTimer;
import run.ratchet.ri.core.DeadLetterService;
import run.ratchet.ri.core.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.JobTask;
import run.ratchet.ri.core.LogPurgeTimer;
import run.ratchet.ri.core.OrphanRecoveryTimer;
import run.ratchet.ri.core.Poller;
import run.ratchet.ri.core.PollerWakeupListener;
import run.ratchet.ri.core.RecurringScheduler;
import run.ratchet.ri.util.RatchetConfiguration;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.logging.Logger;

/**
 * CDI lifecycle observer that initializes and shuts down the Ratchet job scheduler subsystem.
 *
 * <p>On application startup, this bean eagerly initializes all scheduler components. On shutdown,
 * it stops all to allow graceful termination.
 *
 * <p>Startup order:
 *
 * <ol>
 *   <li>Poller — starts claiming pending jobs
 *   <li>RecurringScheduler — starts spawning due recurring job children
 *   <li>OrphanRecoveryTimer — periodic scan for stuck jobs from crashed nodes
 *   <li>BatchRecoveryTimer — periodic scan for stuck batch completions
 *   <li>DeadLetterService — schedules daily DLQ purge
 *   <li>JobArchivingService — schedules cron-based job archiving
 *   <li>LogPurgeTimer — schedules cron-based log purging
 *   <li>PollerWakeupListener — registers for cluster wakeup notifications
 * </ol>
 */
@ApplicationScoped
public class RatchetLifecycle {

  private static final Logger log = Logger.getLogger(RatchetLifecycle.class.getName());

  private final Poller poller;
  private final RecurringScheduler recurringScheduler;
  private final OrphanRecoveryTimer orphanRecoveryTimer;
  private final BatchRecoveryTimer batchRecoveryTimer;
  private final DeadLetterService deadLetterService;
  private final JobArchivingService jobArchivingService;
  private final LogPurgeTimer logPurgeTimer;
  private final PollerWakeupListener pollerWakeupListener;
  private final ExecutorProvider executorProvider;
  private final NodeIdentityProvider nodeIdentityProvider;
  private final RatchetConfiguration config;

  protected RatchetLifecycle() {
    this.poller = null;
    this.recurringScheduler = null;
    this.orphanRecoveryTimer = null;
    this.batchRecoveryTimer = null;
    this.deadLetterService = null;
    this.jobArchivingService = null;
    this.logPurgeTimer = null;
    this.pollerWakeupListener = null;
    this.executorProvider = null;
    this.nodeIdentityProvider = null;
    this.config = null;
  }

  @Inject
  public RatchetLifecycle(
      Poller poller,
      RecurringScheduler recurringScheduler,
      OrphanRecoveryTimer orphanRecoveryTimer,
      BatchRecoveryTimer batchRecoveryTimer,
      DeadLetterService deadLetterService,
      JobArchivingService jobArchivingService,
      LogPurgeTimer logPurgeTimer,
      PollerWakeupListener pollerWakeupListener,
      ExecutorProvider executorProvider,
      NodeIdentityProvider nodeIdentityProvider,
      RatchetConfiguration config) {
    this.poller = poller;
    this.recurringScheduler = recurringScheduler;
    this.orphanRecoveryTimer = orphanRecoveryTimer;
    this.batchRecoveryTimer = batchRecoveryTimer;
    this.deadLetterService = deadLetterService;
    this.jobArchivingService = jobArchivingService;
    this.logPurgeTimer = logPurgeTimer;
    this.pollerWakeupListener = pollerWakeupListener;
    this.executorProvider = executorProvider;
    this.nodeIdentityProvider = nodeIdentityProvider;
    this.config = config;
  }

  /**
   * Initializes the job scheduler subsystem at application startup.
   *
   * @param init the CDI initialization event
   */
  void onStartup(@Observes @Initialized(ApplicationScoped.class) Object init) {
    log.info("Initializing Ratchet job scheduler...");

    // Core job processing
    poller.init();
    recurringScheduler.init();

    // Periodic maintenance timers
    orphanRecoveryTimer.start(
        executorProvider.getScheduledExecutor(), config.getOrphanScanIntervalMinutes());
    batchRecoveryTimer.start(executorProvider.getScheduledExecutor());

    // DLQ purge (opt-out via SCHEDULER_DLQ_PURGE_ENABLED=false)
    if (config.isDlqPurgeEnabled()) {
      Cron dlqCron = RecurringScheduler.PARSER.parse(config.getDlqPurgeCron());
      deadLetterService.init(config.getDlqPurgeDays(), dlqCron);
    }

    // Job archiving (opt-out via SCHEDULER_JOB_ARCHIVE_ENABLED=false)
    if (config.isJobArchiveEnabled()) {
      Cron archiveCron = RecurringScheduler.PARSER.parse(config.getJobArchiverCron());
      jobArchivingService.init(
          true, config.getJobRetentionDays(), config.getJobArchiveBatchSize(), archiveCron);
    }

    // Log purge (opt-out via SCHEDULER_LOG_PURGE_ENABLED=false)
    if (config.isLogPurgeEnabled()) {
      Cron logCron = RecurringScheduler.PARSER.parse(config.getLogPurgeCron());
      logPurgeTimer.init(config.getLogRetentionDays(), logCron);
    }

    // Cluster wakeup optimization
    pollerWakeupListener.init();

    log.info("Ratchet job scheduler initialized");
  }

  /** Stops all scheduler components during application shutdown. */
  @PreDestroy
  void onShutdown() {
    log.info("Shutting down Ratchet job scheduler...");
    poller.stop();
    recurringScheduler.stop();
    orphanRecoveryTimer.stop();
    batchRecoveryTimer.stop();
    deadLetterService.stop();
    jobArchivingService.stop();
    logPurgeTimer.stop();
    if (nodeIdentityProvider instanceof DefaultNodeIdentityProvider defaultProvider) {
      defaultProvider.shutdown();
    }

    // Release classloader references held by static reflection caches
    // BatchService caches are cleared via @PreDestroy on the @ApplicationScoped bean
    JobTask.clearCaches();
  }
}

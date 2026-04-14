package run.ratchet.ri.cdi;

import com.cronutils.model.Cron;
import run.ratchet.ri.core.BatchRecoveryTimer;
import run.ratchet.ri.core.DeadLetterService;
import run.ratchet.ri.core.DefaultNodeIdentityProvider;
import run.ratchet.ri.core.DrainController;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.ri.core.JobExecutionCoordinator;
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
import org.jboss.logging.Logger;

/** CDI lifecycle observer that initializes and shuts down the Ratchet job scheduler subsystem. */
@ApplicationScoped
public class RatchetLifecycle {

  private static final Logger log = Logger.getLogger(RatchetLifecycle.class);

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
  private final DrainController drainController;
  private final RatchetConfiguration config;
  private final JobExecutionCoordinator jobExecutionCoordinator;

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
    this.drainController = null;
    this.config = null;
    this.jobExecutionCoordinator = null;
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
      DrainController drainController,
      RatchetConfiguration config,
      JobExecutionCoordinator jobExecutionCoordinator) {
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
    this.drainController = drainController;
    this.config = config;
    this.jobExecutionCoordinator = jobExecutionCoordinator;
  }

  void onStartup(@Observes @Initialized(ApplicationScoped.class) Object init) {
    log.info("Ratchet starting");

    poller.init();
    recurringScheduler.init();

    orphanRecoveryTimer.start(
        executorProvider.getScheduledExecutor(), config.getOrphanScanIntervalMinutes());
    batchRecoveryTimer.start(executorProvider.getScheduledExecutor());

    if (config.isDlqPurgeEnabled()) {
      Cron dlqCron = RecurringScheduler.PARSER.parse(config.getDlqPurgeCron());
      deadLetterService.init(config.getDlqPurgeDays(), dlqCron);
    }

    if (config.isJobArchiveEnabled()) {
      Cron archiveCron = RecurringScheduler.PARSER.parse(config.getJobArchiverCron());
      jobArchivingService.init(
          true, config.getJobRetentionDays(), config.getJobArchiveBatchSize(), archiveCron);
    }

    if (config.isLogPurgeEnabled()) {
      Cron logCron = RecurringScheduler.PARSER.parse(config.getLogPurgeCron());
      logPurgeTimer.init(config.getLogRetentionDays(), logCron);
    }

    pollerWakeupListener.init();

    log.info("Ratchet started");
  }

  @PreDestroy
  void onShutdown() {
    log.info("Ratchet stopping");
    // Drain before stop to prevent new claims
    drainController.setDraining(true);

    if (nodeIdentityProvider instanceof DefaultNodeIdentityProvider defaultProvider) {
      defaultProvider.shutdown();
    }

    poller.stop();
    recurringScheduler.stop();
    orphanRecoveryTimer.stop();
    batchRecoveryTimer.stop();
    deadLetterService.stop();
    jobArchivingService.stop();
    logPurgeTimer.stop();
    // Reset RUNNING jobs to PENDING so other nodes can pick them up
    jobExecutionCoordinator.shutdown();

    JobTask.clearCaches();
  }
}

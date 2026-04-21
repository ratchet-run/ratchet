package run.ratchet.ri.cdi;

import com.cronutils.model.Cron;
import run.ratchet.api.RatchetOptions;
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
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import java.util.function.Consumer;
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
  private final RatchetOptions options;
  private final JobExecutionCoordinator jobExecutionCoordinator;
  private final Iterable<SchedulerLifecycleHook> lifecycleHooks;

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
    this.options = null;
    this.jobExecutionCoordinator = null;
    this.lifecycleHooks = List.of();
  }

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
      RatchetOptions options,
      JobExecutionCoordinator jobExecutionCoordinator) {
    this(
        poller,
        recurringScheduler,
        orphanRecoveryTimer,
        batchRecoveryTimer,
        deadLetterService,
        jobArchivingService,
        logPurgeTimer,
        pollerWakeupListener,
        executorProvider,
        nodeIdentityProvider,
        drainController,
        options,
        jobExecutionCoordinator,
        List.of());
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
      Instance<RatchetOptions> options,
      JobExecutionCoordinator jobExecutionCoordinator,
      Instance<SchedulerLifecycleHook> lifecycleHooks) {
    this(
        poller,
        recurringScheduler,
        orphanRecoveryTimer,
        batchRecoveryTimer,
        deadLetterService,
        jobArchivingService,
        logPurgeTimer,
        pollerWakeupListener,
        executorProvider,
        nodeIdentityProvider,
        drainController,
        options.get(),
        jobExecutionCoordinator,
        (Iterable<SchedulerLifecycleHook>) lifecycleHooks);
  }

  private RatchetLifecycle(
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
      RatchetOptions options,
      JobExecutionCoordinator jobExecutionCoordinator,
      Iterable<SchedulerLifecycleHook> lifecycleHooks) {
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
    this.options = options;
    this.jobExecutionCoordinator = jobExecutionCoordinator;
    this.lifecycleHooks = lifecycleHooks;
  }

  void onStartup(@Observes @Initialized(ApplicationScoped.class) Object init) {
    log.info("Ratchet starting");
    notifyHooks("beforeStart", SchedulerLifecycleHook::beforeStart);

    recurringScheduler.configure(
        options.recurring().pollMs(),
        options.recurring().maxPollMs(),
        options.recurring().batchLimit());
    poller.init();
    recurringScheduler.init();

    orphanRecoveryTimer.start(
        executorProvider.getScheduledExecutor(), options.node().orphanScanIntervalMinutes());
    batchRecoveryTimer.start(executorProvider.getScheduledExecutor());

    if (options.maintenance().dlqPurgeEnabled()) {
      Cron dlqCron = RecurringScheduler.PARSER.parse(options.maintenance().dlqPurgeCron());
      deadLetterService.init(options.maintenance().dlqPurgeDays(), dlqCron);
    }

    if (options.maintenance().jobArchiveEnabled()) {
      Cron archiveCron = RecurringScheduler.PARSER.parse(options.maintenance().jobArchiveCron());
      jobArchivingService.init(
          true,
          options.maintenance().jobRetentionDays(),
          options.maintenance().jobArchiveBatchSize(),
          archiveCron);
    }

    if (options.maintenance().logPurgeEnabled()) {
      Cron logCron = RecurringScheduler.PARSER.parse(options.maintenance().logPurgeCron());
      logPurgeTimer.init(options.maintenance().logRetentionDays(), logCron);
    }

    pollerWakeupListener.init();
    jobExecutionCoordinator.initRetryBufferDrainer();

    notifyHooks("afterStart", SchedulerLifecycleHook::afterStart);
    log.info("Ratchet started");
  }

  @PreDestroy
  void onShutdown() {
    log.info("Ratchet stopping");
    notifyHooks("beforeStop", SchedulerLifecycleHook::beforeStop);
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
    // Stop background resubmission before resetting RUNNING jobs to PENDING.
    jobExecutionCoordinator.shutdown();

    JobTask.clearCaches();
    notifyHooks("afterStop", SchedulerLifecycleHook::afterStop);
  }

  private void notifyHooks(String phase, Consumer<SchedulerLifecycleHook> callback) {
    for (SchedulerLifecycleHook hook : lifecycleHooks) {
      try {
        callback.accept(hook);
      } catch (Exception e) {
        log.warnf(e, "Scheduler lifecycle hook failed during %s: %s", phase, e.getMessage());
      }
    }
  }
}

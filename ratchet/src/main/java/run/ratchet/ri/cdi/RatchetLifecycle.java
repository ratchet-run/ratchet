package run.ratchet.ri.cdi;

import com.cronutils.model.Cron;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import org.jboss.logging.Logger;
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
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.migration.SchemaInitializationException;

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
  private final Instance<SchedulerLifecycleHook> lifecycleHooks;
  private volatile List<SchedulerLifecycleHook> resolvedHooks;
  private volatile List<SchedulerLifecycleHook> startedHooks = List.of();
  private volatile boolean shutdownComplete;

  private static final Comparator<SchedulerLifecycleHook> HOOK_ORDER =
      Comparator.comparingInt(RatchetLifecycle::priorityValue)
          .thenComparing(RatchetLifecycle::hookSortName);

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
    this.lifecycleHooks = null;
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
        null);
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
      RatchetOptions options,
      JobExecutionCoordinator jobExecutionCoordinator,
      Instance<SchedulerLifecycleHook> lifecycleHooks) {
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

  void onStartup(
      @Observes
          @Priority(Interceptor.Priority.APPLICATION + 500)
          @Initialized(ApplicationScoped.class) Object init) {
    log.info("Ratchet starting");
    List<SchedulerLifecycleHook> beforeStartSucceeded =
        notifyHooks("beforeStart", hooks(), SchedulerLifecycleHook::beforeStart, true);

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

    startedHooks =
        notifyHooks("afterStart", beforeStartSucceeded, SchedulerLifecycleHook::afterStart, false);
    log.info("Ratchet started");
  }

  @PreDestroy
  void onShutdown() {
    List<SchedulerLifecycleHook> hooksToStop;
    synchronized (this) {
      if (shutdownComplete) {
        return;
      }
      shutdownComplete = true;
      hooksToStop = startedHooks;
    }

    log.info("Ratchet stopping");
    List<SchedulerLifecycleHook> beforeStopSucceeded =
        notifyHooks("beforeStop", hooksToStop, SchedulerLifecycleHook::beforeStop, false);
    // Drain before stop to prevent new claims
    drainController.setDraining(true);

    if (nodeIdentityProvider instanceof DefaultNodeIdentityProvider defaultProvider) {
      defaultProvider.shutdown();
    }

    stopService("poller", poller::stop);
    stopService("recurring scheduler", recurringScheduler::stop);
    stopService("orphan recovery timer", orphanRecoveryTimer::stop);
    stopService("batch recovery timer", batchRecoveryTimer::stop);
    stopService("dead letter service", deadLetterService::stop);
    stopService("job archiving service", jobArchivingService::stop);
    stopService("log purge timer", logPurgeTimer::stop);
    // Stop background resubmission before resetting RUNNING jobs to PENDING.
    stopService("job execution coordinator", jobExecutionCoordinator::shutdown);

    JobTask.clearCaches();
    JobPayloadFactory.clearCaches();
    notifyHooks("afterStop", beforeStopSucceeded, SchedulerLifecycleHook::afterStop, false);
    destroyHooks();
  }

  private synchronized List<SchedulerLifecycleHook> hooks() {
    if (lifecycleHooks == null) {
      return List.of();
    }
    if (resolvedHooks == null) {
      List<SchedulerLifecycleHook> resolved = new ArrayList<>();
      lifecycleHooks.forEach(resolved::add);
      resolved.sort(HOOK_ORDER);
      resolvedHooks = List.copyOf(resolved);
    }
    return resolvedHooks;
  }

  private List<SchedulerLifecycleHook> notifyHooks(
      String phase,
      List<SchedulerLifecycleHook> phaseHooks,
      Consumer<SchedulerLifecycleHook> callback,
      boolean abortOnSchemaFailure) {
    List<SchedulerLifecycleHook> succeeded = new ArrayList<>(phaseHooks.size());
    for (SchedulerLifecycleHook hook : phaseHooks) {
      try {
        callback.accept(hook);
        succeeded.add(hook);
      } catch (SchemaInitializationException e) {
        if (abortOnSchemaFailure) {
          // Schema initialization failures must abort startup so the scheduler does not begin
          // claiming jobs against an unmigrated or incompatible schema.
          log.errorf(e, "Scheduler lifecycle hook failed during %s: %s", phase, e.getMessage());
          throw e;
        }
        log.warnf(e, "Scheduler lifecycle hook failed during %s: %s", phase, e.getMessage());
      } catch (Exception e) {
        // SchedulerLifecycleHook allows non-schema hook failures to warn and continue; schema
        // failures are the one startup-aborting exception because they can make job claims unsafe.
        log.warnf(e, "Scheduler lifecycle hook failed during %s: %s", phase, e.getMessage());
      }
    }
    return List.copyOf(succeeded);
  }

  private static int priorityValue(SchedulerLifecycleHook hook) {
    Priority priority = findPriority(hook.getClass());
    return priority == null ? Integer.MAX_VALUE : priority.value();
  }

  private static Priority findPriority(Class<?> type) {
    for (Class<?> current = type;
        current != null && current != Object.class;
        current = current.getSuperclass()) {
      Priority priority = current.getAnnotation(Priority.class);
      if (priority != null) {
        return priority;
      }
      priority = findInterfacePriority(current);
      if (priority != null) {
        return priority;
      }
    }
    return null;
  }

  private static Priority findInterfacePriority(Class<?> type) {
    for (Class<?> iface : type.getInterfaces()) {
      Priority priority = iface.getAnnotation(Priority.class);
      if (priority != null) {
        return priority;
      }
      priority = findInterfacePriority(iface);
      if (priority != null) {
        return priority;
      }
    }
    return null;
  }

  private static String hookSortName(SchedulerLifecycleHook hook) {
    return hook.getClass().getName();
  }

  private void stopService(String name, Runnable stopAction) {
    try {
      stopAction.run();
    } catch (Exception e) {
      log.warnf(e, "Failed to stop %s: %s", name, e.getMessage());
    }
  }

  /**
   * Releases each resolved lifecycle hook back to the CDI container so that {@code @Dependent}
   * scoped hooks have their {@code @PreDestroy} methods invoked. For non-dependent scopes, {@link
   * Instance#destroy(Object)} is effectively a no-op (the container manages the lifecycle).
   */
  private void destroyHooks() {
    if (lifecycleHooks == null || resolvedHooks == null) {
      return;
    }
    for (SchedulerLifecycleHook hook : resolvedHooks) {
      try {
        lifecycleHooks.destroy(hook);
      } catch (Exception e) {
        log.warnf(e, "Failed to destroy scheduler lifecycle hook: %s", e.getMessage());
      }
    }
  }
}

package run.ratchet.ri.cdi;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import jakarta.interceptor.Interceptor;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.ri.core.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.RecurringRegistrationState;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.JobBatchStatusStore;

/**
 * Scans CDI beans for {@link Recurring}-annotated methods and registers them as recurring jobs at
 * startup, then cleans up orphaned jobs whose annotations have been removed.
 *
 * @see Recurring
 * @see RecurringMethodValidator
 * @see RecurringAnnotationParser
 * @see RecurringMethodInvoker
 */
@ApplicationScoped
public class RecurringJobProcessor {

  private static final Logger log = Logger.getLogger(RecurringJobProcessor.class);
  private static final String ORPHAN_CLEANUP_ACTION = "recurring-annotation-orphan-cleanup";
  private static final Duration ORPHAN_CLEANUP_LEASE_TTL = Duration.ofMinutes(5);

  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  private final Map<String, String> registeredJobIds = new ConcurrentHashMap<>();

  private final JobSchedulerService schedulerService;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService;
  private final BeanManager beanManager;
  private final RecurringMethodInvoker methodInvoker;
  private final StartupCoordinator startupCoordinator;
  private final RecurringRegistrationState registrationState;
  private final RatchetOptions options;

  protected RecurringJobProcessor() {
    this.schedulerService = null;
    this.jobBatchStatusStore = null;
    this.recurringAnnotationMaintenanceService = null;
    this.beanManager = null;
    this.methodInvoker = null;
    this.startupCoordinator = null;
    this.registrationState = null;
    this.options = null;
  }

  @Inject
  public RecurringJobProcessor(
      JobSchedulerService schedulerService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options) {
    this.schedulerService = schedulerService;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.recurringAnnotationMaintenanceService = recurringAnnotationMaintenanceService;
    this.beanManager = beanManager;
    this.methodInvoker = methodInvoker;
    this.startupCoordinator = startupCoordinator;
    this.registrationState = registrationState;
    this.options = options;
  }

  public RecurringJobProcessor(
      JobSchedulerService schedulerService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState) {
    this(
        schedulerService,
        jobBatchStatusStore,
        recurringAnnotationMaintenanceService,
        beanManager,
        methodInvoker,
        startupCoordinator,
        registrationState,
        RatchetOptions.defaults());
  }

  void onStartup(
      @Observes @Priority(Interceptor.Priority.APPLICATION) @Initialized(ApplicationScoped.class) Object init) {
    registerRecurringJobs();
  }

  void registerRecurringJobs() {
    Instant startTime = Instant.now();
    log.info("Starting registration of @Recurring annotated jobs");

    for (Bean<?> bean : beanManager.getBeans(Object.class, Any.Literal.INSTANCE)) {
      processBean(bean);
    }

    log.infof("Completed registration of %s recurring jobs", registeredJobIds.size());

    // Publish the discovered key set to the shared registration state BEFORE running cleanup,
    // so the executor's shouldFire gate is armed even if cleanup is delayed (e.g. another node
    // holds the startup lease, or cleanup throws and is retried).
    if (registrationState != null) {
      registrationState.markRegistrationComplete(registeredJobIds.keySet());
    }

    cleanupOrphanedRecurringJobs(startTime);
  }

  private void cleanupOrphanedRecurringJobs(Instant startTime) {
    // Cleanup is DESTRUCTIVE — cancel jobs whose business_key is not in this node's local
    // annotation set. Two guards are required for multi-node safety:
    //
    //  1. Startup lease: only one node in the cluster should run cleanup, otherwise a node that
    //     booted with a stale JAR would silently cancel jobs peers just registered.
    //  2. Convergence window: shift the cutoff back by N seconds so jobs that a peer node
    //     registered in the last N seconds (but after this node's startTime) are exempt.
    //     Closes a race window on rolling deploys where Node A's newer registration commits
    //     after Node B's startTime but before Node B's cleanup runs.
    boolean leaseAcquired = startupCoordinator == null;
    if (startupCoordinator != null) {
      leaseAcquired =
          startupCoordinator.tryAcquire(ORPHAN_CLEANUP_ACTION, ORPHAN_CLEANUP_LEASE_TTL);
    }
    if (!leaseAcquired) {
      log.info("Another node holds the startup lease, skipping orphan cleanup");
      return;
    }
    Instant cutoff = startTime.minusSeconds(options.recurring().convergenceWindowSeconds());
    try {
      Set<String> registeredIds = registeredJobIds.keySet();
      int canceled =
          recurringAnnotationMaintenanceService.cancelOrphanedRecurringAnnotationJobs(
              registeredIds, cutoff);
      if (canceled > 0) {
        log.infof(
            "Canceled %s orphaned recurring jobs (annotations removed from codebase)", canceled);
      }
    } catch (Exception e) {
      log.error("Orphan cleanup error", e);
    } finally {
      if (startupCoordinator != null) {
        try {
          startupCoordinator.release(ORPHAN_CLEANUP_ACTION);
        } catch (Exception e) {
          log.debug("Failed to release orphan cleanup startup lease", e);
        }
      }
    }
  }

  private void cancelExistingJobs(String jobId) {
    // Calls the store SPI directly: cancellation is a pure persistence-layer state flip with no
    // events, observers, or scheduling-side orchestration. Routing through JobSchedulerService
    // would just re-enter the same SPI method one stack frame deeper.
    int canceled = jobBatchStatusStore.cancelRecurringJobByBusinessKey(jobId);
    if (canceled > 0) {
      log.infof("Canceled %s existing recurring job(s) with ID: %s", canceled, jobId);
    }
  }

  private void processBean(Bean<?> bean) {
    Class<?> beanClass = bean.getBeanClass();
    // Walk the class hierarchy so @Recurring methods declared on a superclass are picked up.
    // getDeclaredMethods() alone misses inherited methods. Filter synthetic/bridge methods
    // (which Weld and other CDI implementations sometimes generate) and dedupe by signature
    // so a bridge + real method pair only registers once.
    Set<String> seen = new HashSet<>();
    Class<?> current = beanClass;
    while (current != null && current != Object.class) {
      for (var method : current.getDeclaredMethods()) {
        if (method.isSynthetic() || method.isBridge()) {
          continue;
        }
        Recurring annotation = method.getAnnotation(Recurring.class);
        if (annotation == null) {
          continue;
        }
        String signature = method.getName() + Arrays.toString(method.getParameterTypes());
        if (!seen.add(signature)) {
          continue;
        }
        String methodName = method.getName();
        boolean hasJobContextParam = method.getParameterCount() == 1;
        try {
          RecurringMethodValidator.validate(method);
        } catch (IllegalArgumentException e) {
          log.errorf(e, "Invalid @Recurring method: %s.%s", beanClass.getName(), methodName);
          continue;
        }
        processRecurringMethod(beanClass, methodName, hasJobContextParam, annotation);
      }
      current = current.getSuperclass();
    }
  }

  private void processRecurringMethod(
      Class<?> beanClass, String methodName, boolean hasJobContextParam, Recurring annotation) {
    try {
      if (!RecurringAnnotationParser.isEnabled(annotation)) {
        log.infof("Skipping disabled recurring job: %s.%s", beanClass.getName(), methodName);
        return;
      }

      methodInvoker.validateBeanResolvable(beanClass);

      registerJob(beanClass, methodName, hasJobContextParam, annotation);
    } catch (Exception e) {
      log.errorf(e, "@Recurring registration error: %s.%s", beanClass.getName(), methodName);
    }
  }

  private void registerJob(
      Class<?> beanClass, String methodName, boolean hasJobContextParam, Recurring annotation) {
    String jobId =
        RecurringAnnotationParser.generateJobId(annotation, beanClass.getName(), methodName);

    // Validate cron expression at registration time
    try {
      CRON_PARSER.parse(annotation.cron()).validate();
    } catch (IllegalArgumentException e) {
      log.errorf(
          "Invalid cron expression '%s' for @Recurring method %s.%s: %s",
          annotation.cron(), beanClass.getName(), methodName, e.getMessage());
      return;
    }

    // Validate timezone at registration time
    ZoneId zone;
    try {
      zone = ZoneId.of(annotation.zone());
    } catch (ZoneRulesException | IllegalArgumentException e) {
      log.errorf(
          "Invalid timezone '%s' for @Recurring method %s.%s: %s",
          annotation.zone(), beanClass.getName(), methodName, e.getMessage());
      return;
    }

    cancelExistingJobs(jobId);

    String className = beanClass.getName();

    RecurringJobBuilder builder =
        schedulerService.scheduleRecurring(
            annotation.cron(),
            zone,
            () -> methodInvoker.invoke(className, methodName, hasJobContextParam));

    JobOptions options =
        JobOptions.defaults()
            .withPriority(RecurringAnnotationParser.mapPriority(annotation.priority()))
            .withMaxRetries(annotation.maxRetries())
            .withBackoff(annotation.backoffPolicy(), Duration.ofMillis(annotation.backoffDelayMs()))
            .withTimeout(Duration.ofSeconds(annotation.timeoutSeconds()));

    builder.withOptions(options);
    builder.withBusinessKey(jobId);

    String signature =
        className + "#" + methodName + "(" + (hasJobContextParam ? "JobContext" : "") + ")";
    List<String> tags = new ArrayList<>(Arrays.asList(annotation.tags()));
    tags.add("recurring-annotation");
    tags.add("sig:" + signature.hashCode());
    builder.withTags(tags);

    JobHandle handle = builder.submit();
    registeredJobIds.put(jobId, String.valueOf(handle.id()));

    log.infof("Registered recurring job: %s with cron: %s", jobId, annotation.cron());
  }
}

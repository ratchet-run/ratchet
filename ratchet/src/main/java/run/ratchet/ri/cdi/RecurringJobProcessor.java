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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
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
  private final Set<Class<?>> discoveredRecurringBeanClasses;
  private final Clock clock;

  protected RecurringJobProcessor() {
    this.schedulerService = null;
    this.jobBatchStatusStore = null;
    this.recurringAnnotationMaintenanceService = null;
    this.beanManager = null;
    this.methodInvoker = null;
    this.startupCoordinator = null;
    this.registrationState = null;
    this.options = null;
    this.discoveredRecurringBeanClasses = Set.of();
    this.clock = null;
  }

  public RecurringJobProcessor(
      JobSchedulerService schedulerService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options) {
    this(
        schedulerService,
        jobBatchStatusStore,
        recurringAnnotationMaintenanceService,
        beanManager,
        methodInvoker,
        startupCoordinator,
        registrationState,
        options,
        Clock.systemUTC());
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
      RatchetOptions options,
      Clock clock) {
    this(
        schedulerService,
        jobBatchStatusStore,
        recurringAnnotationMaintenanceService,
        beanManager,
        methodInvoker,
        startupCoordinator,
        registrationState,
        options,
        RecurringMethodDiscoveryExtension.recurringBeanClasses(),
        clock);
  }

  RecurringJobProcessor(
      JobSchedulerService schedulerService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options,
      Set<Class<?>> discoveredRecurringBeanClasses) {
    this(
        schedulerService,
        jobBatchStatusStore,
        recurringAnnotationMaintenanceService,
        beanManager,
        methodInvoker,
        startupCoordinator,
        registrationState,
        options,
        discoveredRecurringBeanClasses,
        Clock.systemUTC());
  }

  RecurringJobProcessor(
      JobSchedulerService schedulerService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options,
      Set<Class<?>> discoveredRecurringBeanClasses,
      Clock clock) {
    this.schedulerService = schedulerService;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.recurringAnnotationMaintenanceService = recurringAnnotationMaintenanceService;
    this.beanManager = beanManager;
    this.methodInvoker = methodInvoker;
    this.startupCoordinator = startupCoordinator;
    this.registrationState = registrationState;
    this.options = options;
    this.discoveredRecurringBeanClasses = Set.copyOf(discoveredRecurringBeanClasses);
    this.clock = clock;
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
    Instant startTime = effective().instant();
    log.info("Starting registration of @Recurring annotated jobs");

    List<RecurringMethodRegistration> registrations = discoverRecurringMethods();
    Set<String> jobIds =
        registrations.stream()
            .map(RecurringMethodRegistration::jobId)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    cancelExistingJobs(jobIds);
    for (RecurringMethodRegistration registration : registrations) {
      try {
        registerJob(registration);
      } catch (Exception e) {
        log.errorf(
            e,
            "@Recurring registration error: %s.%s",
            registration.beanClass().getName(),
            registration.methodName());
      }
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

  private void cancelExistingJobs(Set<String> jobIds) {
    if (jobIds.isEmpty()) {
      return;
    }
    // Calls the store SPI directly: cancellation is a pure persistence-layer state flip with no
    // events, observers, or scheduling-side orchestration. Routing through JobSchedulerService
    // would just re-enter the same SPI method one stack frame deeper.
    int canceled = jobBatchStatusStore.cancelRecurringJobsByBusinessKeys(jobIds);
    if (canceled > 0) {
      log.infof("Canceled %s existing recurring job(s) with IDs: %s", canceled, jobIds);
    }
  }

  private List<Bean<?>> recurringBeans() {
    if (discoveredRecurringBeanClasses.isEmpty()) {
      return new ArrayList<>(beanManager.getBeans(Object.class, Any.Literal.INSTANCE));
    }
    Set<Bean<?>> beans = new LinkedHashSet<>();
    for (Class<?> beanClass : discoveredRecurringBeanClasses) {
      beans.addAll(beanManager.getBeans(beanClass, Any.Literal.INSTANCE));
    }
    return new ArrayList<>(beans);
  }

  private List<RecurringMethodRegistration> discoverRecurringMethods() {
    List<RecurringMethodRegistration> registrations = new ArrayList<>();
    for (Bean<?> bean : recurringBeans()) {
      processBean(bean, registrations);
    }
    return registrations;
  }

  private void processBean(Bean<?> bean, List<RecurringMethodRegistration> registrations) {
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
        prepareRecurringMethod(beanClass, methodName, hasJobContextParam, annotation)
            .ifPresent(registrations::add);
      }
      current = current.getSuperclass();
    }
  }

  private Optional<RecurringMethodRegistration> prepareRecurringMethod(
      Class<?> beanClass, String methodName, boolean hasJobContextParam, Recurring annotation) {
    try {
      if (!RecurringAnnotationParser.isEnabled(annotation)) {
        log.infof("Skipping disabled recurring job: %s.%s", beanClass.getName(), methodName);
        return Optional.empty();
      }

      methodInvoker.validateBeanResolvable(beanClass);

      String jobId =
          RecurringAnnotationParser.generateJobId(annotation, beanClass.getName(), methodName);

      try {
        CRON_PARSER.parse(annotation.cron()).validate();
      } catch (IllegalArgumentException e) {
        log.errorf(
            "Invalid cron expression '%s' for @Recurring method %s.%s: %s",
            annotation.cron(), beanClass.getName(), methodName, e.getMessage());
        return Optional.empty();
      }

      ZoneId zone;
      try {
        zone = ZoneId.of(annotation.zone());
      } catch (ZoneRulesException | IllegalArgumentException e) {
        log.errorf(
            "Invalid timezone '%s' for @Recurring method %s.%s: %s",
            annotation.zone(), beanClass.getName(), methodName, e.getMessage());
        return Optional.empty();
      }

      RecurringAnnotationParser.mapPriority(annotation.priority());

      return Optional.of(
          new RecurringMethodRegistration(
              beanClass, methodName, hasJobContextParam, annotation, jobId, zone));
    } catch (Exception e) {
      log.errorf(e, "@Recurring registration error: %s.%s", beanClass.getName(), methodName);
      return Optional.empty();
    }
  }

  private void registerJob(RecurringMethodRegistration registration) {
    Class<?> beanClass = registration.beanClass();
    String methodName = registration.methodName();
    boolean hasJobContextParam = registration.hasJobContextParam();
    Recurring annotation = registration.annotation();
    String jobId = registration.jobId();
    String className = beanClass.getName();

    RecurringJobBuilder builder =
        schedulerService.scheduleRecurring(
            annotation.cron(),
            registration.zone(),
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

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }

  private record RecurringMethodRegistration(
      Class<?> beanClass,
      String methodName,
      boolean hasJobContextParam,
      Recurring annotation,
      String jobId,
      ZoneId zone) {}
}

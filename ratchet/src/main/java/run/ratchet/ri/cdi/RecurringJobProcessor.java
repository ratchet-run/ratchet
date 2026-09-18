/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.ri.cdi;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.spi.InvocationSubmissionService;
import run.ratchet.spi.JobInvocation;
import run.ratchet.spi.StartupCoordinator;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.RecurringJobStore;

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
  private static final long REGISTRATION_RETRY_DELAY_MS = 500;
  private static final int MAX_REGISTRATION_ATTEMPTS = 10;
  private static final String RECURRING_INVOKE_DESCRIPTOR =
      "(Ljava/lang/String;Ljava/lang/String;Z)V";

  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  private final Map<String, String> registeredJobIds = new ConcurrentHashMap<>();

  private final InvocationSubmissionService invocationSubmissionService;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService;
  private final BeanManager beanManager;
  private final RecurringMethodInvoker methodInvoker;
  private final StartupCoordinator startupCoordinator;
  private final RecurringRegistrationState registrationState;
  private final RatchetOptions options;
  private final Set<Class<?>> discoveredRecurringBeanClasses;
  private final Clock clock;
  private final RecurringJobStore explicitRecurringJobStore;
  private final Function<Class<?>, Map<Method, Recurring>> recurringMethods;

  // Field-injected (not constructor) so direct-construction test/SE paths leave them null and
  // register inline; the CDI-managed bean uses the managed scheduled executor (a post-deployment
  // thread that carries a Jakarta EE component invocation, which @Transactional registration needs)
  // and the store to verify each master committed before declaring registration complete.
  @Inject private ExecutorProvider executorProvider;
  @Inject private Instance<RecurringJobStore> recurringJobStoreInstance;

  // Guarded by this: publication arms the grace gate; cleanup may need subsequent attempts.
  private boolean registrationPublished;
  private boolean cleanupCompleted;
  private Set<String> publishedJobIds;
  private Instant cleanupCutoff;

  protected RecurringJobProcessor() {
    this.invocationSubmissionService = null;
    this.jobBatchStatusStore = null;
    this.recurringAnnotationMaintenanceService = null;
    this.beanManager = null;
    this.methodInvoker = null;
    this.startupCoordinator = null;
    this.registrationState = null;
    this.options = null;
    this.discoveredRecurringBeanClasses = Set.of();
    this.clock = null;
    this.explicitRecurringJobStore = null;
    this.recurringMethods = RecurringJobProcessor::declaredRecurringMethods;
  }

  RecurringJobProcessor(
      InvocationSubmissionService invocationSubmissionService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options) {
    this(
        invocationSubmissionService,
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
      InvocationSubmissionService invocationSubmissionService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options,
      Clock clock) {
    this(
        invocationSubmissionService,
        jobBatchStatusStore,
        recurringAnnotationMaintenanceService,
        beanManager,
        methodInvoker,
        startupCoordinator,
        registrationState,
        options,
        resolveRecurringBeanClasses(beanManager),
        clock);
  }

  RecurringJobProcessor(
      InvocationSubmissionService invocationSubmissionService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options,
      Set<Class<?>> discoveredRecurringBeanClasses) {
    this(
        invocationSubmissionService,
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
      InvocationSubmissionService invocationSubmissionService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options,
      Set<Class<?>> discoveredRecurringBeanClasses,
      Clock clock) {
    this(
        invocationSubmissionService,
        jobBatchStatusStore,
        recurringAnnotationMaintenanceService,
        beanManager,
        methodInvoker,
        startupCoordinator,
        registrationState,
        options,
        discoveredRecurringBeanClasses,
        clock,
        null,
        RecurringJobProcessor::declaredRecurringMethods);
  }

  private RecurringJobProcessor(
      InvocationSubmissionService invocationSubmissionService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState,
      RatchetOptions options,
      Set<Class<?>> discoveredRecurringBeanClasses,
      Clock clock,
      RecurringJobStore recurringJobStore,
      Function<Class<?>, Map<Method, Recurring>> recurringMethods) {
    this.invocationSubmissionService = invocationSubmissionService;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.recurringAnnotationMaintenanceService = recurringAnnotationMaintenanceService;
    this.beanManager = beanManager;
    this.methodInvoker = methodInvoker;
    this.startupCoordinator = startupCoordinator;
    this.registrationState = registrationState;
    this.options = options;
    this.discoveredRecurringBeanClasses = Set.copyOf(discoveredRecurringBeanClasses);
    this.clock = clock;
    this.explicitRecurringJobStore = recurringJobStore;
    this.recurringMethods = Objects.requireNonNull(recurringMethods);
  }

  RecurringJobProcessor(
      InvocationSubmissionService invocationSubmissionService,
      JobBatchStatusStore jobBatchStatusStore,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      StartupCoordinator startupCoordinator,
      RecurringRegistrationState registrationState) {
    this(
        invocationSubmissionService,
        jobBatchStatusStore,
        recurringAnnotationMaintenanceService,
        beanManager,
        methodInvoker,
        startupCoordinator,
        registrationState,
        RatchetOptions.defaults());
  }

  /** Uses host-resolved annotations, including annotations inherited from interfaces. */
  public RecurringJobProcessor(
      InvocationSubmissionService submissions,
      JobBatchStatusStore store,
      RecurringAnnotationMaintenanceService maintenance,
      RecurringMethodInvoker invoker,
      StartupCoordinator startup,
      RecurringRegistrationState registration,
      RatchetOptions options,
      Set<Class<?>> types,
      Clock clock,
      RecurringJobStore recurringStore,
      Function<Class<?>, Map<Method, Recurring>> recurringMethods) {
    this(
        submissions,
        store,
        maintenance,
        null,
        invoker,
        startup,
        registration,
        options,
        types,
        clock,
        recurringStore,
        recurringMethods);
  }

  void onStartup(
      @Observes
          @Priority(RatchetRuntimeStart.PRIORITY_RECURRING_REGISTRATION)
          @Initialized(ApplicationScoped.class) Object init) {
    // Deferred on build-time-CDI runtimes (e.g. Quarkus), which run this observer during
    // STATIC_INIT before the EntityManager exists; they drive startup via RatchetRuntimeStart
    // instead.
    if (RatchetRuntimeStart.logIfDeferred(
        log,
        "@Recurring registration deferred pending RatchetRuntimeStart event; if this runtime"
            + " never fires that event, recurring jobs will never register")) {
      return;
    }
    ScheduledExecutorService scheduler = resolveScheduledExecutor();
    if (scheduler == null) {
      // Plain-CDI / SE / unit tests: no managed executor, and the calling thread already carries a
      // usable transaction context, so register inline.
      registerInline();
      return;
    }
    // On a Jakarta EE container the @Initialized(ApplicationScoped) observer can fire before the
    // application's component invocation context is established (notably GlassFish 8, which fires
    // it
    // mid-deployment). Without that context the @Transactional submit path neither starts nor
    // commits a transaction, so on EclipseLink 5 + SQL Server (whose JTA pool pins autocommit off)
    // the recurring-master INSERT is rolled back on connection return and silently lost. Defer
    // registration to the managed scheduled executor, whose tasks run post-deployment with a proper
    // component context, and retry until every master is confirmed committed.
    scheduleDeferredRegistration(scheduler, 1);
  }

  void onRuntimeStart(
      @Observes @Priority(RatchetRuntimeStart.PRIORITY_RECURRING_REGISTRATION)
          RatchetRuntimeStart event) {
    ScheduledExecutorService scheduler = resolveScheduledExecutor();
    if (scheduler == null) {
      registerInline();
      return;
    }
    attemptDeferredRegistration(scheduler, 1);
  }

  private ScheduledExecutorService resolveScheduledExecutor() {
    if (executorProvider == null) {
      return null;
    }
    try {
      return executorProvider.getScheduledExecutor();
    } catch (RuntimeException e) {
      log.warnf(
          e,
          "Managed scheduled executor unavailable for @Recurring registration; registering inline");
      return null;
    }
  }

  private synchronized void registerInline() {
    try {
      registerRecurringJobs();
    } catch (RuntimeException e) {
      if (!registrationPublished) {
        throw e;
      }
      log.warn(
          "Orphan cleanup abandoned after one inline attempt; no managed executor for retries", e);
    }
  }

  private synchronized void scheduleDeferredRegistration(
      ScheduledExecutorService scheduler, int attempt) {
    try {
      scheduler.schedule(
          () -> attemptDeferredRegistration(scheduler, attempt),
          REGISTRATION_RETRY_DELAY_MS,
          TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      log.warnf(
          e,
          "%s; scheduling rejected for attempt %s/%s",
          registrationPublished
              ? "Orphan cleanup abandoned"
              : "@Recurring registration and orphan cleanup abandoned",
          attempt,
          MAX_REGISTRATION_ATTEMPTS);
    }
  }

  private synchronized void attemptDeferredRegistration(
      ScheduledExecutorService scheduler, int attempt) {
    try {
      if (registerRecurringJobs()) {
        return;
      }
      log.infof(
          "@Recurring registration not yet committed (attempt %s/%s)",
          attempt, MAX_REGISTRATION_ATTEMPTS);
    } catch (RuntimeException e) {
      log.warnf(
          e,
          "%s attempt %s/%s failed",
          registrationPublished ? "Orphan cleanup" : "@Recurring registration",
          attempt,
          MAX_REGISTRATION_ATTEMPTS);
    }
    if (attempt < MAX_REGISTRATION_ATTEMPTS) {
      scheduleDeferredRegistration(scheduler, attempt + 1);
    } else {
      log.warnf(
          "%s after %s registration attempts",
          registrationPublished
              ? "Orphan cleanup abandoned"
              : "@Recurring registration and orphan cleanup abandoned",
          attempt);
    }
  }

  /**
   * Registers every discovered {@code @Recurring} method. The recurring submit path reconciles
   * existing masters by business key, so registration is idempotent across retries and restarts
   * while still applying annotation changes.
   *
   * @return {@code true} once every discovered master is confirmed present in the store (or the
   *     store does not advertise the recurring capability) and cleanup has completed or been
   *     skipped for lease contention, so the caller can stop retrying
   */
  public synchronized boolean registerRecurringJobs() {
    if (registrationPublished) {
      completeCleanup();
      return true;
    }
    Instant startTime = effective().instant();
    log.info("Starting registration of @Recurring annotated jobs");

    RecurringJobStore store = resolveRecurringJobStore();
    List<RecurringMethodRegistration> registrations = discoverRecurringMethods();
    Set<String> discoveredJobIds =
        registrations.stream()
            .map(RecurringMethodRegistration::jobId)
            .collect(Collectors.toCollection(LinkedHashSet::new));
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

    boolean committed =
        store == null
            || registrations.stream()
                .allMatch(r -> store.findRecurringByBusinessKey(r.jobId()).isPresent());
    if (committed) {
      finalizeRegistration(startTime, discoveredJobIds);
    }
    return committed;
  }

  private RecurringJobStore resolveRecurringJobStore() {
    if (beanManager == null) return explicitRecurringJobStore;
    if (recurringJobStoreInstance == null || !recurringJobStoreInstance.isResolvable()) {
      return null;
    }
    return recurringJobStoreInstance.get();
  }

  private void finalizeRegistration(Instant startTime, Set<String> discoveredJobIds) {
    // Called under the registration monitor, so overlapping startup callbacks cannot clean up
    // concurrently. Freeze the snapshot and cutoff before publishing; retries only repeat cleanup.
    publishedJobIds = Set.copyOf(discoveredJobIds);
    cleanupCutoff = startTime.minusSeconds(options.recurring().convergenceWindowSeconds());
    if (registrationState != null) {
      registrationState.markRegistrationComplete(publishedJobIds);
    }
    registrationPublished = true;
    log.infof("Completed registration of %s recurring jobs", registeredJobIds.size());
    completeCleanup();
  }

  private void completeCleanup() {
    if (cleanupCompleted) {
      return;
    }
    cleanupOrphanedRecurringJobs();
    // An exception leaves cleanup pending. Explicit lease contention is a completed skip.
    cleanupCompleted = true;
  }

  private void cleanupOrphanedRecurringJobs() {
    // Cleanup is DESTRUCTIVE — cancel jobs whose business_key is not in this node's local
    // annotation set. Two guards are required for multi-node safety:
    //
    //  1. Startup lease: only one node in the cluster should run cleanup, otherwise a node that
    //     booted with a stale JAR would silently cancel jobs peers just registered.
    //  2. Convergence window: shift the cutoff back by N seconds so jobs that a peer node
    //     registered in the last N seconds (but after this node's startTime) are exempt.
    //     Closes a race window on rolling deploys where Node A's newer registration commits
    //     after Node B's startTime but before Node B's cleanup runs.
    // A null coordinator is the explicit single-node/test path. Clustered deployments must wire a
    // StartupCoordinator so destructive orphan cleanup is lease-guarded.
    boolean leaseAcquired = startupCoordinator == null;
    if (startupCoordinator != null) {
      leaseAcquired =
          startupCoordinator.tryAcquire(ORPHAN_CLEANUP_ACTION, ORPHAN_CLEANUP_LEASE_TTL);
    }
    if (!leaseAcquired) {
      log.info("Another node holds the startup lease, skipping orphan cleanup");
      return;
    }
    try {
      int canceled =
          recurringAnnotationMaintenanceService.cancelOrphanedRecurringAnnotationJobs(
              publishedJobIds, cleanupCutoff);
      if (canceled > 0) {
        log.infof(
            "Canceled %s orphaned recurring jobs (annotations removed from codebase)", canceled);
      }
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
    Set<Class<?>> beanClasses =
        beanManager == null
            ? discoveredRecurringBeanClasses
            : recurringBeans().stream().map(Bean::getBeanClass).collect(Collectors.toSet());
    for (Class<?> beanClass : beanClasses) {
      processBean(beanClass, registrations);
    }
    return registrations;
  }

  private void processBean(Class<?> beanClass, List<RecurringMethodRegistration> registrations) {
    recurringMethods
        .apply(beanClass)
        .forEach(
            (method, annotation) -> processMethod(beanClass, method, annotation, registrations));
  }

  private static Map<Method, Recurring> declaredRecurringMethods(Class<?> beanClass) {
    Map<Method, Recurring> methods = new LinkedHashMap<>();
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
        methods.put(method, annotation);
      }
      current = current.getSuperclass();
    }
    return methods;
  }

  private void processMethod(
      Class<?> beanClass,
      Method method,
      Recurring annotation,
      List<RecurringMethodRegistration> registrations) {
    try {
      RecurringMethodValidator.validate(method);
    } catch (IllegalArgumentException e) {
      log.errorf(e, "Invalid @Recurring method: %s.%s", beanClass.getName(), method.getName());
      return;
    }
    prepareRecurringMethod(beanClass, method.getName(), method.getParameterCount() == 1, annotation)
        .ifPresent(registrations::add);
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

      JobPriority priority = RecurringAnnotationParser.mapPriority(annotation.priority());

      return Optional.of(
          new RecurringMethodRegistration(
              beanClass, methodName, hasJobContextParam, annotation, jobId, zone, priority));
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
        invocationSubmissionService.scheduleRecurringInvocation(
            annotation.cron(),
            registration.zone(),
            new JobInvocation(
                RecurringMethodInvoker.class.getName(),
                "invoke",
                RECURRING_INVOKE_DESCRIPTOR,
                false,
                List.of(className, methodName, hasJobContextParam)));

    JobOptions options =
        JobOptions.defaults()
            .withPriority(registration.priority())
            .withMaxRetries(annotation.maxRetries())
            .withBackoff(annotation.backoffPolicy(), Duration.ofMillis(annotation.backoffDelayMs()))
            .withTimeout(Duration.ofSeconds(annotation.timeoutSeconds()));

    builder.withOptions(options);
    builder.withBusinessKey(jobId);
    builder.withMisfirePolicy(RecurringAnnotationParser.misfirePolicy(annotation));

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

  /**
   * Resolves the set of bean classes that declare {@link Recurring} methods for this CDI container
   * by looking up the {@link RecurringMethodDiscoveryExtension} instance from the provided {@link
   * BeanManager}. Using the instance (rather than a static accessor) ensures deployment-scoped
   * isolation when multiple CDI containers share a JVM.
   */
  private static Set<Class<?>> resolveRecurringBeanClasses(BeanManager bm) {
    if (bm == null) {
      return Set.of();
    }
    try {
      RecurringMethodDiscoveryExtension ext =
          bm.getExtension(RecurringMethodDiscoveryExtension.class);
      return ext.getRecurringBeanClasses();
    } catch (Exception e) {
      return Set.of();
    }
  }

  private record RecurringMethodRegistration(
      Class<?> beanClass,
      String methodName,
      boolean hasJobContextParam,
      Recurring annotation,
      String jobId,
      ZoneId zone,
      JobPriority priority) {}
}

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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.RatchetOptions;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.internal.RecurringRegistrationState;
import run.ratchet.spi.ExecutorProvider;
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

  // Field-injected (not constructor) so direct-construction test/SE paths leave them null and
  // register inline; the CDI-managed bean uses the managed scheduled executor (a post-deployment
  // thread that carries a Jakarta EE component invocation, which @Transactional registration needs)
  // and the store to verify each master committed before declaring registration complete.
  @Inject private ExecutorProvider executorProvider;
  @Inject private Instance<RecurringJobStore> recurringJobStoreInstance;

  private final Object registrationLifecycleLock = new Object();
  private long registrationGeneration;
  private boolean registrationTriggered;
  private ScheduledFuture<?> pendingRegistrationRetry;
  private final AtomicBoolean registrationFinalized = new AtomicBoolean();

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

  RecurringJobProcessor(
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
        resolveRecurringBeanClasses(beanManager),
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

  RecurringJobProcessor(
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
    registerFromApplicationStart();
  }

  void onRuntimeStart(
      @Observes @Priority(RatchetRuntimeStart.PRIORITY_RECURRING_REGISTRATION)
          RatchetRuntimeStart event) {
    registerFromRuntimeStart();
  }

  /**
   * Requests registration from the normal application-scope startup path.
   *
   * <p>The common runtime invokes this before node and worker startup. The retained CDI observer
   * invokes the same idempotent entry point after the lifecycle observer, preserving compatibility
   * without registering a second time.
   */
  public void registerFromApplicationStart() {
    triggerRegistration(
        generation -> {
          ScheduledExecutorService scheduler = resolveScheduledExecutor();
          if (scheduler == null) {
            // Plain-CDI / SE / unit tests: no managed executor, and the calling thread already
            // carries a usable transaction context, so register inline.
            if (isRegistrationActive(generation)) {
              registerRecurringJobs();
            }
            return;
          }
          // On a Jakarta EE container the @Initialized(ApplicationScoped) observer can fire before
          // the application's component invocation context is established (notably GlassFish 8).
          // Defer registration to a managed task with a proper component context, then retry until
          // every master is confirmed committed.
          scheduleDeferredRegistration(scheduler, 1, generation);
        });
  }

  /**
   * Requests registration from the deferred runtime-ready event.
   *
   * <p>The first attempt may run immediately because this event is fired only after the persistence
   * and component invocation contexts are ready.
   */
  public void registerFromRuntimeStart() {
    triggerRegistration(
        generation -> {
          ScheduledExecutorService scheduler = resolveScheduledExecutor();
          if (scheduler == null) {
            if (isRegistrationActive(generation)) {
              registerRecurringJobs();
            }
            return;
          }
          attemptDeferredRegistration(scheduler, 1, generation);
        });
  }

  /**
   * Invalidates this registration cycle and cancels its pending retry, if any.
   *
   * <p>An attempt already executing may finish, but it cannot schedule another retry after this
   * method returns. A later runtime start begins a new generation, so callbacks retained from this
   * cycle remain inert.
   */
  public void cancelRegistration() {
    ScheduledFuture<?> pendingRetry;
    synchronized (registrationLifecycleLock) {
      registrationGeneration++;
      registrationTriggered = false;
      pendingRetry = pendingRegistrationRetry;
      pendingRegistrationRetry = null;
    }
    if (pendingRetry != null) {
      pendingRetry.cancel(false);
    }
  }

  private void triggerRegistration(LongConsumer registration) {
    long generation;
    synchronized (registrationLifecycleLock) {
      if (registrationTriggered) {
        return;
      }
      registrationTriggered = true;
      generation = ++registrationGeneration;
    }
    try {
      registration.accept(generation);
    } catch (RuntimeException | Error failure) {
      synchronized (registrationLifecycleLock) {
        if (registrationGeneration == generation) {
          registrationTriggered = false;
        }
      }
      throw failure;
    }
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

  private void scheduleDeferredRegistration(
      ScheduledExecutorService scheduler, int attempt, long generation) {
    if (!isRegistrationActive(generation)) {
      return;
    }
    ScheduledFuture<?> scheduledRetry =
        scheduler.schedule(
            () -> attemptDeferredRegistration(scheduler, attempt, generation),
            REGISTRATION_RETRY_DELAY_MS,
            TimeUnit.MILLISECONDS);

    boolean cancelled;
    synchronized (registrationLifecycleLock) {
      cancelled = registrationGeneration != generation || !registrationTriggered;
      if (!cancelled) {
        pendingRegistrationRetry = scheduledRetry;
      }
    }
    if (cancelled && scheduledRetry != null) {
      scheduledRetry.cancel(false);
    }
  }

  private void attemptDeferredRegistration(
      ScheduledExecutorService scheduler, int attempt, long generation) {
    if (!beginRegistrationAttempt(generation)) {
      return;
    }
    try {
      boolean committed = registerRecurringJobs();
      if (!committed && attempt < MAX_REGISTRATION_ATTEMPTS) {
        log.infof(
            "@Recurring registration not yet committed (attempt %s/%s); retrying",
            attempt, MAX_REGISTRATION_ATTEMPTS);
        scheduleDeferredRegistration(scheduler, attempt + 1, generation);
      }
    } catch (RuntimeException e) {
      log.error("@Recurring registration attempt failed", e);
      if (attempt < MAX_REGISTRATION_ATTEMPTS) {
        scheduleDeferredRegistration(scheduler, attempt + 1, generation);
      }
    }
  }

  private boolean beginRegistrationAttempt(long generation) {
    synchronized (registrationLifecycleLock) {
      if (registrationGeneration != generation || !registrationTriggered) {
        return false;
      }
      pendingRegistrationRetry = null;
      return true;
    }
  }

  private boolean isRegistrationActive(long generation) {
    synchronized (registrationLifecycleLock) {
      return registrationGeneration == generation && registrationTriggered;
    }
  }

  /**
   * Registers every discovered {@code @Recurring} method. The recurring submit path reconciles
   * existing masters by business key, so registration is idempotent across retries and restarts
   * while still applying annotation changes.
   *
   * @return {@code true} once every discovered master is confirmed present in the store (or the
   *     store does not advertise the recurring capability), so the caller can stop retrying
   */
  boolean registerRecurringJobs() {
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
    if (recurringJobStoreInstance == null || !recurringJobStoreInstance.isResolvable()) {
      return null;
    }
    return recurringJobStoreInstance.get();
  }

  private void finalizeRegistration(Instant startTime, Set<String> discoveredJobIds) {
    if (!registrationFinalized.compareAndSet(false, true)) {
      return; // already finalized by an earlier attempt
    }
    log.infof("Completed registration of %s recurring jobs", registeredJobIds.size());

    // Publish the discovered key set to the shared registration state BEFORE running cleanup,
    // so the executor's shouldFire gate is armed even if cleanup is delayed (e.g. another node
    // holds the startup lease, or cleanup throws and is retried).
    if (registrationState != null) {
      registrationState.markRegistrationComplete(discoveredJobIds);
    }

    cleanupOrphanedRecurringJobs(startTime, discoveredJobIds);
  }

  private void cleanupOrphanedRecurringJobs(Instant startTime, Set<String> discoveredJobIds) {
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
    Instant cutoff = startTime.minusSeconds(options.recurring().convergenceWindowSeconds());
    try {
      int canceled =
          recurringAnnotationMaintenanceService.cancelOrphanedRecurringAnnotationJobs(
              discoveredJobIds, cutoff);
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
        schedulerService.scheduleRecurring(
            annotation.cron(),
            registration.zone(),
            () -> methodInvoker.invoke(className, methodName, hasJobContextParam));

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

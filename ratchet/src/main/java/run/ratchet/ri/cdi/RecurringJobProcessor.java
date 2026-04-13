package run.ratchet.ri.cdi;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.Recurring;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.ri.core.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.core.RecurringRegistrationState;
import run.ratchet.spi.ClusterCoordinator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;

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

  /**
   * System property controlling how far back the orphaned-recurring-job cleanup cutoff is shifted
   * from this node's startup instant. Jobs created within the convergence window are exempt from
   * cleanup regardless of leader state.
   *
   * @deprecated As of 0.2.0 the convergence window default is 0; the role it played is now covered
   *     more rigorously by {@link RecurringRegistrationState#shouldFire(String)}, which gates
   *     firing of orphaned masters during the post-registration startup grace window ({@link
   *     RecurringRegistrationState#STARTUP_GRACE_PROPERTY}). The convergence window property is
   *     still honored for one release for backward compatibility but will be removed in 0.3.0.
   */
  @Deprecated(since = "0.2.0", forRemoval = true)
  static final String CONVERGENCE_WINDOW_PROPERTY = "ratchet.recurring.convergence-window-seconds";

  private static final long DEFAULT_CONVERGENCE_WINDOW_SECONDS = 0L;

  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  private final Map<String, String> registeredJobIds = new ConcurrentHashMap<>();

  private final JobSchedulerService schedulerService;
  private final RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService;
  private final BeanManager beanManager;
  private final RecurringMethodInvoker methodInvoker;
  private final ClusterCoordinator clusterCoordinator;
  private final RecurringRegistrationState registrationState;

  protected RecurringJobProcessor() {
    this.schedulerService = null;
    this.recurringAnnotationMaintenanceService = null;
    this.beanManager = null;
    this.methodInvoker = null;
    this.clusterCoordinator = null;
    this.registrationState = null;
  }

  @Inject
  public RecurringJobProcessor(
      JobSchedulerService schedulerService,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker,
      ClusterCoordinator clusterCoordinator,
      RecurringRegistrationState registrationState) {
    this.schedulerService = schedulerService;
    this.recurringAnnotationMaintenanceService = recurringAnnotationMaintenanceService;
    this.beanManager = beanManager;
    this.methodInvoker = methodInvoker;
    this.clusterCoordinator = clusterCoordinator;
    this.registrationState = registrationState;
  }

  void onStartup(@Observes @Initialized(ApplicationScoped.class) Object init) {
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
    // so the executor's shouldFire gate is armed even if cleanup is delayed (e.g. leader gate
    // skips cleanup on this node, or cleanup throws and is retried).
    if (registrationState != null) {
      registrationState.markRegistrationComplete(registeredJobIds.keySet());
    }

    cleanupOrphanedRecurringJobs(startTime);
  }

  private void cleanupOrphanedRecurringJobs(Instant startTime) {
    // Cleanup is DESTRUCTIVE — cancel jobs whose business_key is not in this node's local
    // annotation set. Two guards are required for multi-node safety:
    //
    //  1. Leader gate: only one node in the cluster should run cleanup, otherwise a node that
    //     booted with a stale JAR would silently cancel jobs peers just registered.
    //  2. Convergence window: shift the cutoff back by N seconds so jobs that a peer node
    //     registered in the last N seconds (but after this node's startTime) are exempt.
    //     Closes a race window on rolling deploys where Node A's newer registration commits
    //     after Node B's startTime but before Node B's cleanup runs.
    if (clusterCoordinator != null && !clusterCoordinator.isLeader()) {
      log.info("Skipping orphaned recurring job cleanup — this node is not the cluster leader");
      return;
    }
    Instant cutoff = startTime.minusSeconds(convergenceWindowSeconds());
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
      log.error("Failed to cleanup orphaned recurring jobs", e);
    }
  }

  /**
   * Returns the convergence window in seconds, read fresh from system properties on each call (not
   * cached) so operators can tune behavior via {@code -D} flags without rebuilding.
   */
  static long convergenceWindowSeconds() {
    String raw = System.getProperty(CONVERGENCE_WINDOW_PROPERTY);
    if (raw == null || raw.isBlank()) {
      return DEFAULT_CONVERGENCE_WINDOW_SECONDS;
    }
    try {
      long parsed = Long.parseLong(raw.trim());
      return Math.max(0L, parsed);
    } catch (NumberFormatException e) {
      log.warnf(
          "Invalid value for %s: '%s' — falling back to default %ss",
          CONVERGENCE_WINDOW_PROPERTY, raw, DEFAULT_CONVERGENCE_WINDOW_SECONDS);
      return DEFAULT_CONVERGENCE_WINDOW_SECONDS;
    }
  }

  private void cancelExistingJobs(String jobId) {
    int canceled = schedulerService.cancelRecurringJobByBusinessKey(jobId);
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
    java.util.Set<String> seen = new java.util.HashSet<>();
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
        String signature = method.getName() + java.util.Arrays.toString(method.getParameterTypes());
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
      log.errorf(e, "Failed to register recurring job: %s.%s", beanClass.getName(), methodName);
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

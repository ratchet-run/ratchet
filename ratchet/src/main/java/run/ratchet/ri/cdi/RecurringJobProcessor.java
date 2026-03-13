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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processes methods annotated with {@link Recurring} and registers them as recurring jobs.
 *
 * <p>This processor orchestrates the discovery and registration of recurring jobs by:
 *
 * <ul>
 *   <li>Scanning CDI beans for @Recurring annotated methods
 *   <li>Delegating validation to {@link RecurringMethodValidator}
 *   <li>Delegating configuration parsing to {@link RecurringAnnotationParser}
 *   <li>Delegating invocation to {@link RecurringMethodInvoker}
 *   <li>Cleaning up orphaned jobs whose annotations have been removed
 * </ul>
 *
 * @see Recurring
 * @see RecurringMethodValidator
 * @see RecurringAnnotationParser
 * @see RecurringMethodInvoker
 */
@ApplicationScoped
public class RecurringJobProcessor {

  private static final Logger log = Logger.getLogger(RecurringJobProcessor.class.getName());

  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

  private final Map<String, String> registeredJobIds = new ConcurrentHashMap<>();

  private final JobSchedulerService schedulerService;
  private final RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService;
  private final BeanManager beanManager;
  private final RecurringMethodInvoker methodInvoker;

  protected RecurringJobProcessor() {
    this.schedulerService = null;
    this.recurringAnnotationMaintenanceService = null;
    this.beanManager = null;
    this.methodInvoker = null;
  }

  @Inject
  public RecurringJobProcessor(
      JobSchedulerService schedulerService,
      RecurringAnnotationMaintenanceService recurringAnnotationMaintenanceService,
      BeanManager beanManager,
      RecurringMethodInvoker methodInvoker) {
    this.schedulerService = schedulerService;
    this.recurringAnnotationMaintenanceService = recurringAnnotationMaintenanceService;
    this.beanManager = beanManager;
    this.methodInvoker = methodInvoker;
  }

  /**
   * Triggers registration of @Recurring jobs at application startup.
   *
   * @param init the CDI initialization event
   */
  void onStartup(@Observes @Initialized(ApplicationScoped.class) Object init) {
    registerRecurringJobs();
  }

  void registerRecurringJobs() {
    Instant startTime = Instant.now();
    log.info("Starting registration of @Recurring annotated jobs");

    for (Bean<?> bean : beanManager.getBeans(Object.class, Any.Literal.INSTANCE)) {
      processBean(bean);
    }

    log.info("Completed registration of " + registeredJobIds.size() + " recurring jobs");

    cleanupOrphanedRecurringJobs(startTime);
  }

  private void cleanupOrphanedRecurringJobs(Instant startTime) {
    try {
      Set<String> registeredIds = registeredJobIds.keySet();
      int canceled =
          recurringAnnotationMaintenanceService.cancelOrphanedRecurringAnnotationJobs(
              registeredIds, startTime);
      if (canceled > 0) {
        log.info(
            "Canceled "
                + canceled
                + " orphaned recurring jobs (annotations removed from codebase)");
      }
    } catch (Exception e) {
      log.log(Level.SEVERE, "Failed to cleanup orphaned recurring jobs", e);
    }
  }

  private void cancelExistingJobs(String jobId) {
    int canceled = schedulerService.cancelRecurringJobByBusinessKey(jobId);
    if (canceled > 0) {
      log.info("Canceled " + canceled + " existing recurring job(s) with ID: " + jobId);
    }
  }

  private void processBean(Bean<?> bean) {
    Class<?> beanClass = bean.getBeanClass();

    for (var method : beanClass.getDeclaredMethods()) {
      Recurring annotation = method.getAnnotation(Recurring.class);
      if (annotation != null) {
        String methodName = method.getName();
        boolean hasJobContextParam = method.getParameterCount() == 1;

        try {
          RecurringMethodValidator.validate(method);
        } catch (IllegalArgumentException e) {
          log.log(
              Level.SEVERE,
              "Invalid @Recurring method: " + beanClass.getName() + "." + methodName,
              e);
          continue;
        }

        processRecurringMethod(beanClass, methodName, hasJobContextParam, annotation);
      }
    }
  }

  private void processRecurringMethod(
      Class<?> beanClass, String methodName, boolean hasJobContextParam, Recurring annotation) {
    try {
      if (!RecurringAnnotationParser.isEnabled(annotation)) {
        log.info("Skipping disabled recurring job: " + beanClass.getName() + "." + methodName);
        return;
      }

      methodInvoker.validateBeanResolvable(beanClass);

      registerJob(beanClass, methodName, hasJobContextParam, annotation);
    } catch (Exception e) {
      log.log(
          Level.SEVERE,
          "Failed to register recurring job: " + beanClass.getName() + "." + methodName,
          e);
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
      log.severe(
          "Invalid cron expression '"
              + annotation.cron()
              + "' for @Recurring method "
              + beanClass.getName()
              + "."
              + methodName
              + ": "
              + e.getMessage());
      return;
    }

    // Validate timezone at registration time
    ZoneId zone;
    try {
      zone = ZoneId.of(annotation.zone());
    } catch (ZoneRulesException | IllegalArgumentException e) {
      log.severe(
          "Invalid timezone '"
              + annotation.zone()
              + "' for @Recurring method "
              + beanClass.getName()
              + "."
              + methodName
              + ": "
              + e.getMessage());
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

    log.info("Registered recurring job: " + jobId + " with cron: " + annotation.cron());
  }
}

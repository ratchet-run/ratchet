package run.ratchet.ri.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.event.JobCallbackFailedEvent;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobStartedEvent;
import run.ratchet.ri.resilience.ServiceUnavailableException;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.util.ObjectMapperFactory;
import jakarta.inject.Inject;
import java.io.Serial;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;
import org.objectweb.asm.Type;

/**
 * Runs a single job via reflection, handling retries, lifecycle events, and post-execution workflow
 * dispatch. Each instance is single-use and discarded after completion.
 */
public class JobTask implements Callable<Void> {

  private static final Logger log = Logger.getLogger(JobTask.class);

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.get();

  private static final ConcurrentHashMap<String, Method> METHOD_CACHE = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, String> SERVICE_NAME_CACHE =
      new ConcurrentHashMap<>();

  /**
   * System property controlling the maximum size in bytes of a serialized job result. Results
   * exceeding this size are truncated to a marker JSON noting the original size. Default 65536
   * (64KB). Set to 0 to disable the cap.
   */
  static final String RESULT_MAX_BYTES_PROPERTY = "ratchet.jobs.max-result-bytes";

  private static final long DEFAULT_RESULT_MAX_BYTES = 65536L;

  /**
   * Reads the maximum result-size cap from system properties on every call (not cached) so
   * operators can tune it via {@code -D} flags without rebuilding.
   */
  static long maxResultBytes() {
    String raw = System.getProperty(RESULT_MAX_BYTES_PROPERTY);
    if (raw == null || raw.isBlank()) {
      return DEFAULT_RESULT_MAX_BYTES;
    }
    try {
      return Math.max(0L, Long.parseLong(raw.trim()));
    } catch (NumberFormatException e) {
      return DEFAULT_RESULT_MAX_BYTES;
    }
  }

  private final JobStore jobStore;
  private final ResourcePermitService resourcePermitService;
  private final PostExecutionHandler lifecycleFacade;
  private final NodeIdentityProvider nodeIdProvider;
  private final ExecutionObserver observabilityFacade;
  private final PreExecutionValidator validationFacade;
  private final BeanResolver beanResolver;
  private final RetryPolicy retryPolicy;
  private final ResilienceStrategy resilienceStrategy;
  private final ErrorSanitizer errorSanitizer;
  private final ClassPolicy classPolicy;
  private JobEntity job;
  private JobClaimDto claim;
  private JobExecutionEntity currentExecution;
  private boolean permitAcquired;

  protected JobTask() {
    this.jobStore = null;
    this.resourcePermitService = null;
    this.lifecycleFacade = null;
    this.nodeIdProvider = null;
    this.observabilityFacade = null;
    this.validationFacade = null;
    this.beanResolver = null;
    this.retryPolicy = null;
    this.resilienceStrategy = null;
    this.errorSanitizer = null;
    this.classPolicy = null;
  }

  @Inject
  public JobTask(
      JobStore jobStore,
      ResourcePermitService resourcePermitService,
      PostExecutionHandler lifecycleFacade,
      NodeIdentityProvider nodeIdProvider,
      ExecutionObserver observabilityFacade,
      PreExecutionValidator validationFacade,
      BeanResolver beanResolver,
      RetryPolicy retryPolicy,
      ResilienceStrategy resilienceStrategy,
      ErrorSanitizer errorSanitizer,
      ClassPolicy classPolicy) {
    this.jobStore = jobStore;
    this.resourcePermitService = resourcePermitService;
    this.lifecycleFacade = lifecycleFacade;
    this.nodeIdProvider = nodeIdProvider;
    this.observabilityFacade = observabilityFacade;
    this.validationFacade = validationFacade;
    this.beanResolver = beanResolver;
    this.retryPolicy = retryPolicy;
    this.resilienceStrategy = resilienceStrategy;
    this.errorSanitizer = errorSanitizer;
    this.classPolicy = classPolicy;
  }

  // ONLY path that populates CLASS_CACHE. Re-checks ClassPolicy on every call (even cache hits)
  // to block post-load policy changes and cache-poisoning from code that bypasses validationFacade.
  private Class<?> loadAllowedClass(String className) throws ClassNotFoundException {
    if (className == null || className.isEmpty()) {
      throw new SecurityException("Class name cannot be null or empty");
    }
    if (classPolicy != null && !classPolicy.isAllowed(className)) {
      throw new SecurityException("Class " + className + " is not allowed for job execution.");
    }
    Class<?> cached = CLASS_CACHE.get(className);
    if (cached != null) {
      return cached;
    }
    Class<?> loaded =
        Class.forName(className, true, Thread.currentThread().getContextClassLoader());
    CLASS_CACHE.putIfAbsent(className, loaded);
    return loaded;
  }

  /**
   * Clears all static reflection caches. Must be called on application shutdown to release
   * classloader references and prevent memory leaks in redeployable containers (e.g., WildFly,
   * Payara).
   */
  public static void clearCaches() {
    METHOD_CACHE.clear();
    CLASS_CACHE.clear();
    SERVICE_NAME_CACHE.clear();
  }

  private static String simpleClassName(String fqcn) {
    int lastDot = fqcn.lastIndexOf('.');
    return lastDot >= 0 ? fqcn.substring(lastDot + 1) : fqcn;
  }

  /**
   * Does NOT run in a transaction to avoid holding database connections for long-running jobs. All
   * database updates are performed in separate short transactions.
   */
  @Override
  @SuppressWarnings("java:S1181")
  public Void call() {
    Long jobId = getJobId();

    JobEntity jobEntity;
    try {
      jobEntity = getJob();
    } catch (Exception e) {
      log.errorf(e, "Job %s failed to load entity from database - aborting execution", jobId);
      // Defensive: bind() hasn't run yet, but MDC.remove() of unset keys is a safe no-op.
      JobMdcContext.clear();
      return null;
    }

    JobMdcContext.bindJobContext(
        jobId, jobEntity.getParams(), nodeIdProvider.getNodeId(), jobEntity.getCreatedBy());

    if (jobEntity.getCreatedBy() != null) {
      log.debugf("Job %s created by user: %s", jobId, jobEntity.getCreatedBy());
    } else {
      log.debugf("Job %s created by system (no user context)", jobId);
    }

    observabilityFacade.recordJobStart(jobEntity);

    int attemptNumber = jobEntity.getAttempts() + 1;
    currentExecution =
        observabilityFacade.startExecution(jobId, attemptNumber, nodeIdProvider.getNodeId());

    observabilityFacade.publishEvent(
        new JobStartedEvent(
            jobEntity.getId(),
            jobEntity.getBusinessKey(),
            jobEntity.getPublicJobType(),
            jobEntity.getPriority(),
            jobEntity.getPickedBy()));

    if (log.isInfoEnabled()) {
      log.infov(
          "Job {0} starting execution [type={1}, priority={2}, attempt={3}/{4}, payload={5}.{6}]",
          jobId,
          jobEntity.getJobType(),
          jobEntity.getPriority(),
          jobEntity.getAttempts() + 1,
          jobEntity.getMaxRetries() + 1,
          jobEntity.getPayload().target(),
          jobEntity.getPayload().method());
    }

    Instant start = Instant.now();
    Object jobResult;
    permitAcquired = false;
    String resilienceServiceName = resolveResilienceServiceName(jobEntity.getPayload());
    try {
      if (wasJobCanceledDuringExecution()) {
        handleCanceledDuringExecution(start);
        return null;
      }

      if (!resilienceStrategy.isServiceAvailable(resilienceServiceName)) {
        log.infof(
            "Job %s skipped - circuit breaker OPEN for service: %s", jobId, resilienceServiceName);
        rescheduleForCircuitBreaker(jobEntity, resilienceServiceName);
        return null;
      }

      if (!tryAcquireResourcePermit()) {
        return null;
      }

      // Validate security BEFORE entering the resilience strategy scope. Otherwise a
      // misconfigured ClassPolicy or unknown class would surface as a SecurityException
      // INSIDE the circuit breaker, which would record it as a service failure and trip
      // the breaker for the target service. Configuration errors must not poison the
      // breaker; the breaker should only see real downstream failures.
      validationFacade.validateSecurity(jobEntity.getPayload());

      jobResult =
          resilienceStrategy.execute(
              resilienceServiceName, () -> runPayload(jobEntity.getPayload()));

      if (wasJobCanceledDuringExecution()) {
        handleCanceledDuringExecution(start);
      } else {
        handleSuccess(start, jobResult);
      }
    } catch (Throwable t) {
      try {
        handleFailure(t);
      } catch (Throwable failureHandlingError) {
        log.errorf(
            failureHandlingError,
            "Job %s failure handling itself failed, forcing FAILED status",
            job.getId());
        String safeError;
        try {
          safeError = errorSanitizer.sanitize(t);
        } catch (Throwable sanitizerError) {
          safeError = t.getClass().getName();
        }
        try {
          jobStore.compareAndSwapStatus(
              job.getId(), JobStatus.RUNNING, JobStatus.FAILED, safeError);
        } catch (Throwable lastResort) {
          log.errorf(
              lastResort,
              "Job %s could not be transitioned to FAILED — will require orphan recovery",
              job.getId());
        }
      }
    } finally {
      if (permitAcquired) {
        releaseResourcePermit();
      }
      log.infof("Job %s execution complete - cleaning up context", jobId);
      // Must be Throwable, not Exception — Error must still clear MDC. See
      // JobMdcContextThrowableTest
      JobMdcContext.clear();
    }
    return null;
  }

  void init(JobEntity job) {
    this.job = job;
    this.claim = null;
  }

  void initFromClaim(JobClaimDto claim) {
    this.claim = claim;
    this.job = null;
  }

  private JobEntity getJob() {
    if (job == null && claim != null) {
      job =
          jobStore
              .findById(claim.id())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Job " + claim.id() + " not found - may have been deleted"));
    }
    return job;
  }

  private Long getJobId() {
    return job != null ? job.getId() : claim.id();
  }

  private boolean tryAcquireResourcePermit() {
    String resourceName = job.getResourceName();
    if (resourceName == null) {
      return true;
    }

    try {
      boolean acquired =
          resourcePermitService.tryAcquire(resourceName, job.getId(), nodeIdProvider.getNodeId());

      if (!acquired) {
        int retryDelay = resourcePermitService.getRetryDelay(resourceName);
        Instant newScheduledTime = Instant.now().plusMillis(retryDelay);

        if (currentExecution != null) {
          currentExecution.markFailed(
              new ResourceCapacityException(
                  "Resource '" + resourceName + "' at capacity - rescheduling"));
          observabilityFacade.saveExecution(currentExecution);
        }

        jobStore.scheduleJobRetry(
            job.getId(),
            "Waiting for resource: " + resourceName,
            newScheduledTime,
            job.getAttempts());

        log.infof(
            "Job %s waiting for resource '%s' - rescheduled for %sms",
            job.getId(), resourceName, retryDelay);

        return false;
      }

      permitAcquired = true;
      log.infof("Job %s acquired permit for resource '%s'", job.getId(), resourceName);
      return true;

    } catch (IllegalArgumentException e) {
      log.errorf(e, "Job %s references unconfigured resource: %s", job.getId(), resourceName);
      handleFailure(e);
      return false;
    }
  }

  private void releaseResourcePermit() {
    String resourceName = job.getResourceName();
    if (resourceName != null) {
      try {
        resourcePermitService.release(resourceName, job.getId());
        log.infof("Job %s released permit for resource '%s'", job.getId(), resourceName);
      } catch (Exception e) {
        log.warnf("Failed to release permit for job %s: %s", job.getId(), e.getMessage());
      }
    }
  }

  /**
   * Reschedules a job when the circuit breaker is open, without counting it as a retry attempt.
   * Uses the same pattern as resource permit unavailability — the job goes back to PENDING with a
   * delay matching the circuit breaker's typical OPEN-to-HALF_OPEN transition window.
   */
  private void rescheduleForCircuitBreaker(JobEntity jobEntity, String serviceName) {
    long delayMs = resilienceStrategy.getRetryDelay(serviceName).toMillis();
    Instant newScheduledTime = Instant.now().plusMillis(delayMs);

    if (currentExecution != null) {
      currentExecution.markFailed(
          new ServiceUnavailableException("Circuit breaker OPEN for service: " + serviceName));
      observabilityFacade.saveExecution(currentExecution);
    }

    jobStore.scheduleJobRetry(
        jobEntity.getId(),
        "Circuit breaker OPEN for service: " + serviceName,
        newScheduledTime,
        jobEntity.getAttempts());
  }

  private boolean wasJobCanceledDuringExecution() {
    JobStatus freshStatus = jobStore.getJobStatus(job.getId());
    if (freshStatus == null) {
      log.warnf("Job %s was deleted during execution - treating as canceled", job.getId());
      return true;
    }
    return freshStatus == JobStatus.CANCELED;
  }

  private void handleCanceledDuringExecution(Instant start) {
    log.infof("Job %s was canceled during execution - result discarded", job.getId());

    Instant endTime = Instant.now();
    long executionMs = Duration.between(start, endTime).toMillis();

    job.setExecutionStartTime(start);
    job.setExecutionEndTime(endTime);
    job.setExecutionDurationMs(executionMs);

    if (currentExecution != null) {
      currentExecution.markCanceled();
      observabilityFacade.saveExecution(currentExecution);
    }

    observabilityFacade.recordJobCancellation(job);

    observabilityFacade.publishEvent(
        new JobCancelledEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            JobStatus.RUNNING.name(),
            executionMs));

    handleBatchOrWorkflowCancellation();
  }

  private void handleBatchOrWorkflowCancellation() {
    if (job.getJobType() == JobExecutionType.BATCH_CHILD) {
      lifecycleFacade.markBatchChildFailed(job);
    } else {
      lifecycleFacade.cancelChain(job);
    }
  }

  private void handleBatchOrWorkflowPermanentFailure() {
    if (job.getJobType() == JobExecutionType.BATCH_CHILD) {
      lifecycleFacade.markBatchChildFailed(job);
    } else {
      lifecycleFacade.scheduleNext(job);
    }
  }

  private void handleFailure(Throwable ex) {
    log.errorf(
        ex, "Job %s failed with %s: %s", job.getId(), ex.getClass().getName(), ex.getMessage());

    // Check shouldNotRetry FIRST, before burning an attempt slot. Non-retryable failures
    // go straight to the failure handler with the existing attempt count, so the audit trail
    // doesn't show a phantom retry attempt that never happened.
    if (validationFacade.shouldNotRetry(ex)) {
      observabilityFacade.recordJobFailure(job, ex, job.getAttempts());
      logIfTimeout(ex);
      handleNonRetryableFailure(ex, job.getAttempts());
      return;
    }

    int attempt = jobStore.incrementRetryAttempt(job.getId());
    if (attempt == -1) {
      log.infof("Job %s already in terminal state, skipping retry logic", job.getId());
      return;
    }

    job.setAttempts(attempt);
    observabilityFacade.recordJobFailure(job, ex, attempt);
    logIfTimeout(ex);

    if (attempt <= job.getMaxRetries() && retryPolicy.shouldRetry(attempt, ex)) {
      scheduleRetry(ex, attempt);
    } else {
      moveToDlq(ex, attempt);
    }
  }

  private void handleNonRetryableFailure(Throwable ex, int attempt) {
    log.warnf(
        "Job %s failed with non-retryable exception: %s - moving directly to DLQ",
        job.getId(), ex.getClass().getName());

    if (currentExecution != null) {
      currentExecution.markFailed(ex);
      observabilityFacade.saveExecution(currentExecution);
    }

    if (jobStore.compareAndSwapStatus(
        job.getId(), JobStatus.RUNNING, JobStatus.FAILED, errorSanitizer.sanitize(ex))) {
      job.setAttempts(attempt);
      job.setStatus(JobStatus.FAILED);
      publishDlqEvent(ex, attempt);
      lifecycleFacade.moveToDlq(job, ex);
      handleBatchOrWorkflowPermanentFailure();
      invokeCallback(job.getOnFailurePayload(), "onFailure");
    }
  }

  private void handleSuccess(Instant start, Object jobResult) {
    Instant endTime = Instant.now();
    long executionMs = Duration.between(start, endTime).toMillis();
    long queueMs =
        job.getPickedAt() != null ? Duration.between(job.getPickedAt(), start).toMillis() : 0;

    observabilityFacade.recordJobSuccess(job, executionMs);

    String resultJson = null;
    String resultType = null;
    if (jobResult != null) {
      try {
        resultJson = OBJECT_MAPPER.writeValueAsString(jobResult);
        resultType = jobResult.getClass().getName();
        long maxBytes = maxResultBytes();
        if (maxBytes > 0 && resultJson.length() > maxBytes) {
          // Truncate to a marker JSON that captures the original size and the cap. The job
          // still succeeds — operators can see the truncation in the row and adjust either
          // the cap or the job's return shape.
          log.warnf(
              "Job %s result exceeds %s=%s bytes (actual=%s); truncating to marker",
              job.getId(), RESULT_MAX_BYTES_PROPERTY, maxBytes, resultJson.length());
          resultJson =
              "{\"_truncated\":true,\"_originalSize\":"
                  + resultJson.length()
                  + ",\"_maxAllowed\":"
                  + maxBytes
                  + ",\"_resultType\":\""
                  + resultType.replace("\"", "\\\"")
                  + "\"}";
        }
      } catch (Exception e) {
        log.warnf("Failed to serialize job result for job %s: %s", job.getId(), e.getMessage());
      }
    }

    if (!jobStore.markJobSucceeded(
        job.getId(), resultJson, resultType, start, endTime, executionMs, queueMs)) {
      log.infof("Job %s already in terminal state, skipping success handling", job.getId());
      return;
    }

    if (currentExecution != null) {
      currentExecution.markSucceeded();
      observabilityFacade.saveExecution(currentExecution);
    }

    job.setStatus(JobStatus.SUCCEEDED);
    job.setJobResult(resultJson);
    job.setResultType(resultType);
    job.setExecutionStartTime(start);
    job.setExecutionEndTime(endTime);
    job.setExecutionDurationMs(executionMs);
    job.setQueueWaitMs(queueMs);

    observabilityFacade.publishEvent(
        new JobCompletedEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            executionMs));

    try {
      lifecycleFacade.handleJobSuccess(job);
    } catch (Exception e) {
      log.warnf(
          "Job %s [type=%s, key=%s] succeeded but post-success lifecycle processing failed: %s: %s",
          job.getId(),
          job.getJobType(),
          job.getBusinessKey(),
          e.getClass().getName(),
          e.getMessage());
    }

    invokeCallback(job.getOnSuccessPayload(), "onSuccess");

    log.infof("Job %s succeeded in %s ms", job.getId(), executionMs);
  }

  // Matches JobTimeoutException, TimeoutException, and InterruptedException at top level and
  // one cause deep — all three are produced by JobTimeoutHandler cancel paths.
  private void logIfTimeout(Throwable ex) {
    boolean wasTimeout =
        ex instanceof run.ratchet.api.exception.JobTimeoutException
            || ex instanceof java.util.concurrent.TimeoutException
            || ex instanceof InterruptedException
            || ex.getCause() instanceof run.ratchet.api.exception.JobTimeoutException
            || ex.getCause() instanceof java.util.concurrent.TimeoutException
            || ex.getCause() instanceof InterruptedException;
    if (wasTimeout) {
      log.errorf(ex, "Job %s was cancelled due to timeout", job.getId());
    }
  }

  private void moveToDlq(Throwable ex, int attempt) {
    if (currentExecution != null) {
      currentExecution.markFailed(ex);
      observabilityFacade.saveExecution(currentExecution);
    }

    if (jobStore.compareAndSwapStatus(
        job.getId(), JobStatus.RUNNING, JobStatus.FAILED, errorSanitizer.sanitize(ex))) {
      job.setAttempts(attempt);
      job.setStatus(JobStatus.FAILED);
      publishDlqEvent(ex, attempt);
      lifecycleFacade.moveToDlq(job, ex);
      handleBatchOrWorkflowPermanentFailure();
      invokeCallback(job.getOnFailurePayload(), "onFailure");
      log.errorf(ex, "Job %s moved to DLQ after %s attempts", job.getId(), attempt);
    }
  }

  private void publishDlqEvent(Throwable ex, int attempt) {
    observabilityFacade.publishEvent(
        new JobDlqEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            errorSanitizer.sanitize(ex),
            attempt));
  }

  private void invokeCallback(JobPayload callbackPayload, String callbackName) {
    if (callbackPayload == null) {
      return;
    }
    try {
      validationFacade.validateSecurity(callbackPayload);
      Class<?> cls;
      try {
        cls = loadAllowedClass(callbackPayload.target());
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Callback class not found: " + callbackPayload.target(), e);
      }
      Method method = resolveMethod(cls, callbackPayload);
      Object target = callbackPayload.isStatic() ? null : beanResolver.resolve(cls);
      List<Object> args = callbackPayload.args() != null ? callbackPayload.args() : List.of();
      method.invoke(target, args.toArray());
    } catch (Exception e) {
      // Callback failures are no longer silent. Operators see (a) a SEVERE log entry
      // with the full stack, (b) a metrics counter increment, and (c) a CDI/programmatic
      // event they can observe. The parent job still succeeds — callbacks are by design
      // fire-and-log, not failure-propagating.
      log.errorf(
          e,
          "Job %s %s callback failed: %s: %s",
          job.getId(),
          callbackName,
          e.getClass().getName(),
          e.getMessage());
      try {
        observabilityFacade.recordCallbackFailure(job, e, 1);
      } catch (Exception metricEx) {
        log.warnf(
            "Failed to record callback failure metric for job %s: %s",
            job.getId(), metricEx.getMessage());
      }
      try {
        JobCallbackFailedEvent.CallbackType type =
            "onSuccess".equals(callbackName)
                ? JobCallbackFailedEvent.CallbackType.ON_SUCCESS
                : JobCallbackFailedEvent.CallbackType.ON_FAILURE;
        observabilityFacade.publishEvent(
            new JobCallbackFailedEvent(
                job.getId(),
                job.getBusinessKey(),
                job.getPublicJobType(),
                job.getPriority(),
                job.getPickedBy(),
                type,
                e.getMessage(),
                e.getClass().getName(),
                1));
      } catch (Exception eventEx) {
        log.warnf(
            "Failed to publish JobCallbackFailedEvent for job %s: %s",
            job.getId(), eventEx.getMessage());
      }
    }
  }

  private Method resolveMethod(Class<?> clazz, JobPayload payload) throws NoSuchMethodException {
    String cacheKey = clazz.getName() + "#" + payload.method() + ":" + payload.methodDescriptor();
    Method cached = METHOD_CACHE.get(cacheKey);
    if (cached != null) {
      return cached;
    }

    for (Method m : clazz.getMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        METHOD_CACHE.put(cacheKey, m);
        return m;
      }
    }

    for (Method m : clazz.getDeclaredMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
        String visibility =
            Modifier.isPrivate(m.getModifiers())
                ? "private"
                : Modifier.isProtected(m.getModifiers()) ? "protected" : "package-private";
        throw new NoSuchMethodException(
            payload.method()
                + " in "
                + clazz.getName()
                + " is "
                + visibility
                + " — only public methods can be scheduled as jobs. "
                + "Change the method visibility to public.");
      }
    }

    throw new NoSuchMethodException(
        payload.method() + " with descriptor " + payload.methodDescriptor());
  }

  private String resolveResilienceServiceName(JobPayload payload) {
    String cacheKey = payload.target() + "#" + payload.method() + ":" + payload.methodDescriptor();
    String cached = SERVICE_NAME_CACHE.get(cacheKey);
    if (cached != null) {
      return cached;
    }

    String fallbackServiceName = simpleClassName(payload.target()) + "." + payload.method();
    try {
      // Route through the policy-guarded loader so CLASS_CACHE is never primed with a denied
      // class. This path runs BEFORE runPayload's security validation, so without this guard
      // an attacker-controlled class name would land in the cache unchecked.
      Class<?> clazz = loadAllowedClass(payload.target());
      Method method = resolveMethod(clazz, payload);
      CircuitBreakerProtected annotation = method.getAnnotation(CircuitBreakerProtected.class);
      if (annotation == null) {
        annotation = clazz.getAnnotation(CircuitBreakerProtected.class);
      }

      String resolved =
          annotation != null && annotation.service() != null && !annotation.service().isBlank()
              ? annotation.service()
              : clazz.getSimpleName() + "." + method.getName();
      SERVICE_NAME_CACHE.put(cacheKey, resolved);
      return resolved;
    } catch (Exception e) {
      SERVICE_NAME_CACHE.put(cacheKey, fallbackServiceName);
      return fallbackServiceName;
    }
  }

  @SuppressWarnings("java:S112")
  private Object runPayload(JobPayload payload) throws Exception {
    // validateSecurity() is called by call() BEFORE entering the resilience scope.
    // Do not re-validate here — security exceptions inside the breaker would poison it.

    log.infof(
        "Job %s resolving target: %s.%s (static=%s)",
        job.getId(), payload.target(), payload.method(), payload.isStatic());

    Class<?> cls;
    try {
      cls = loadAllowedClass(payload.target());
    } catch (ClassNotFoundException cnfe) {
      log.errorf(cnfe, "Job %s target class not found: %s", job.getId(), payload.target());
      throw cnfe;
    }

    Method m;
    try {
      m = resolveMethod(cls, payload);
    } catch (NoSuchMethodException e) {
      log.errorf(
          e,
          "Job %s target method not found: %s with descriptor %s in class %s",
          job.getId(),
          payload.method(),
          payload.methodDescriptor(),
          payload.target());
      throw e;
    }

    if (payload.isStatic()) {
      return m.invoke(null, payload.args().toArray());
    }

    Object bean;
    try {
      bean = beanResolver.resolve(cls);
    } catch (Exception e) {
      log.errorf(
          e,
          "Failed to resolve bean for instance method %s in class %s",
          payload.method(),
          payload.target());
      throw new IllegalStateException(
          "Cannot resolve bean for instance method "
              + payload.method()
              + " in class "
              + payload.target()
              + ". Ensure the class is a managed bean or use a static method.",
          e);
    }

    return m.invoke(bean, payload.args().toArray());
  }

  private void scheduleReadyJobsUpdate(long backoff) {
    if (backoff > 0) {
      observabilityFacade.scheduleDelayedJobReadyCallback(backoff);
    }
  }

  private void scheduleRetry(Throwable ex, int attempt) {
    if (currentExecution != null) {
      currentExecution.markFailed(ex);
      observabilityFacade.saveExecution(currentExecution);
    }

    // Consult RetryPolicy for delay; fall back to job-level backoff configuration
    Duration policyDelay = retryPolicy.getDelay(attempt);
    long backoff =
        policyDelay.isZero()
            ? BackoffPolicyHandler.computeDelay(
                job.getBackoffPolicy(), job.getBackoffParamMs(), attempt)
            : policyDelay.toMillis();
    Instant newScheduledTime = Instant.now().plusMillis(backoff);

    if (jobStore.scheduleJobRetry(
        job.getId(), errorSanitizer.sanitize(ex), newScheduledTime, attempt)) {
      job.setAttempts(attempt);
      job.setScheduledTime(newScheduledTime);
      job.setLastError(errorSanitizer.sanitize(ex));
      job.setStatus(JobStatus.PENDING);

      observabilityFacade.publishEvent(
          new JobRetryingEvent(
              job.getId(),
              job.getBusinessKey(),
              job.getPublicJobType(),
              job.getPriority(),
              job.getPickedBy(),
              errorSanitizer.sanitize(ex),
              attempt,
              newScheduledTime));
      scheduleReadyJobsUpdate(backoff);

      log.warnf(
          "Job %s retrying in %s ms (attempt %s/%s) due to: %s: %s",
          job.getId(),
          backoff,
          attempt,
          job.getMaxRetries(),
          ex.getClass().getName(),
          ex.getMessage());
    }
  }

  private static class ResourceCapacityException extends RuntimeException {

    @Serial private static final long serialVersionUID = 2983760024642099243L;

    ResourceCapacityException(String message) {
      super(message);
    }
  }
}

package run.ratchet.ri.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import run.ratchet.api.JobType;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobStartedEvent;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import java.io.Serial;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.objectweb.asm.Type;

/**
 * Core job execution engine responsible for running scheduled jobs within worker threads. This
 * class orchestrates the complete job lifecycle from execution through success or failure handling,
 * including retries, metrics collection, and event publishing.
 *
 * <p>The JobTask implements the Callable interface to enable asynchronous execution within the
 * worker pool thread model. Each instance is dedicated to a single job execution and is discarded
 * after completion.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li><b>Dynamic Invocation:</b> Uses reflection to invoke job methods based on payload
 *       specifications, supporting any managed bean
 *   <li><b>Observability:</b> Maintains MDC context for distributed tracing, collects detailed
 *       metrics, and publishes lifecycle events
 *   <li><b>Workflow Support:</b> Handles job type-specific post-processing for chains, batches, and
 *       workflow branches
 * </ul>
 *
 * @see JobExecutionCoordinator for the thread pool management
 * @see JobPayload for the execution specification format
 */
@ApplicationScoped
@Transactional
public class JobTask implements Callable<Void> {

  private static final Logger log = Logger.getLogger(JobTask.class.getName());

  /**
   * Thread-safe ObjectMapper singleton for JSON serialization of job results. ObjectMapper is
   * thread-safe and expensive to create (~1ms), so we use a static singleton.
   */
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final JobStore jobStore;
  private final ResourcePermitService resourcePermitService;
  private final PostExecutionHandler lifecycleFacade;
  private final NodeIdentityProvider nodeIdProvider;
  private final ExecutionObserver observabilityFacade;
  private final PreExecutionValidator validationFacade;
  private final BeanResolver beanResolver;
  private final RetryPolicy retryPolicy;

  /** Nullable resilience strategy for future circuit breaker integration (Phase 4f). */
  @SuppressWarnings("unused")
  private final ResilienceStrategy resilienceStrategy;

  private JobEntity job;
  private JobClaimDto claim;
  private JobExecutionEntity currentExecution;
  private boolean permitAcquired;

  // Required by CDI proxy
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
  }

  /**
   * Creates a new JobTask with all required dependencies.
   *
   * @param jobStore the store for job persistence operations
   * @param resourcePermitService service for acquiring and releasing resource permits
   * @param lifecycleFacade facade for post-execution lifecycle operations
   * @param nodeIdProvider provider for the unique node identifier
   * @param observabilityFacade facade for metrics and event publishing
   * @param validationFacade facade for pre-execution validation
   * @param beanResolver resolver for bean instances by type
   * @param retryPolicy policy for retry decisions and delay calculation
   */
  @Inject
  public JobTask(
      JobStore jobStore,
      ResourcePermitService resourcePermitService,
      PostExecutionHandler lifecycleFacade,
      NodeIdentityProvider nodeIdProvider,
      ExecutionObserver observabilityFacade,
      PreExecutionValidator validationFacade,
      BeanResolver beanResolver,
      RetryPolicy retryPolicy) {
    this.jobStore = jobStore;
    this.resourcePermitService = resourcePermitService;
    this.lifecycleFacade = lifecycleFacade;
    this.nodeIdProvider = nodeIdProvider;
    this.observabilityFacade = observabilityFacade;
    this.validationFacade = validationFacade;
    this.beanResolver = beanResolver;
    this.retryPolicy = retryPolicy;
    this.resilienceStrategy = null;
  }

  /**
   * Executes the main operation of this callable task. The method processes the job payload and
   * handles success or failure scenarios.
   *
   * <p>This method does NOT run in a transaction to avoid holding database connections for
   * long-running jobs. All database updates are performed in separate short transactions.
   *
   * @return null, as the callable task does not produce a result.
   */
  @Override
  @SuppressWarnings("java:S1181")
  public Void call() {
    Long jobId = getJobId();
    JobMdcContext.setup(jobId, nodeIdProvider.getNodeId());

    JobEntity jobEntity;
    try {
      jobEntity = getJob();
    } catch (Exception e) {
      log.log(
          Level.SEVERE,
          "Job " + jobId + " failed to load entity from database - aborting execution",
          e);
      JobMdcContext.clear();
      return null;
    }

    JobMdcContext.bindJobContext(jobId, jobEntity.getParams());

    if (jobEntity.getCreatedBy() != null) {
      log.info("Job " + jobId + " created by user: " + jobEntity.getCreatedBy());
      JobMdcContext.setJobCreator(jobEntity.getCreatedBy());
    } else {
      log.info("Job " + jobId + " created by system (no user context)");
    }

    observabilityFacade.recordJobStart(jobEntity);

    int attemptNumber = jobEntity.getAttempts() + 1;
    currentExecution =
        observabilityFacade.startExecution(jobId, attemptNumber, nodeIdProvider.getNodeId());

    observabilityFacade.publishEvent(
        new JobStartedEvent(
            jobEntity.getId(),
            jobEntity.getBusinessKey(),
            jobEntity.getJobType(),
            jobEntity.getPriority(),
            jobEntity.getPickedBy()));

    log.info(
        "Job "
            + jobId
            + " starting execution [type="
            + jobEntity.getJobType()
            + ", priority="
            + jobEntity.getPriority()
            + ", attempt="
            + (jobEntity.getAttempts() + 1)
            + "/"
            + (jobEntity.getMaxRetries() + 1)
            + ", payload="
            + jobEntity.getPayload().target()
            + "."
            + jobEntity.getPayload().method()
            + "]");

    Instant start = Instant.now();
    Object jobResult;
    permitAcquired = false;
    try {
      if (wasJobCanceledDuringExecution()) {
        handleCanceledDuringExecution(start);
        return null;
      }

      if (!tryAcquireResourcePermit()) {
        return null;
      }

      jobResult = runPayload(jobEntity.getPayload());

      if (wasJobCanceledDuringExecution()) {
        handleCanceledDuringExecution(start);
      } else {
        handleSuccess(start, jobResult);
      }
    } catch (Throwable t) {
      handleFailure(t);
    } finally {
      if (permitAcquired) {
        releaseResourcePermit();
      }
      log.info("Job " + jobId + " execution complete - cleaning up context");
      JobMdcContext.clear();
    }
    return null;
  }

  /**
   * Initializes this runner instance with the job to be executed.
   *
   * @param job the job entity containing execution details and configuration
   */
  void init(JobEntity job) {
    this.job = job;
    this.claim = null;
  }

  /**
   * Initializes this runner instance with a lightweight claim DTO for lazy entity loading.
   *
   * @param claim the job claim DTO containing metadata for lazy loading
   */
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

        log.info(
            "Job "
                + job.getId()
                + " waiting for resource '"
                + resourceName
                + "' - rescheduled for "
                + retryDelay
                + "ms");

        return false;
      }

      permitAcquired = true;
      log.info("Job " + job.getId() + " acquired permit for resource '" + resourceName + "'");
      return true;

    } catch (IllegalArgumentException e) {
      log.log(
          Level.SEVERE,
          "Job " + job.getId() + " references unconfigured resource: " + resourceName,
          e);
      handleFailure(e);
      return false;
    }
  }

  private void releaseResourcePermit() {
    String resourceName = job.getResourceName();
    if (resourceName != null) {
      try {
        resourcePermitService.release(resourceName, job.getId());
        log.info("Job " + job.getId() + " released permit for resource '" + resourceName + "'");
      } catch (Exception e) {
        log.warning("Failed to release permit for job " + job.getId() + ": " + e.getMessage());
      }
    }
  }

  private boolean wasJobCanceledDuringExecution() {
    JobStatus freshStatus = jobStore.getJobStatus(job.getId());
    if (freshStatus == null) {
      log.warning("Job " + job.getId() + " was deleted during execution - treating as canceled");
      return true;
    }
    return freshStatus == JobStatus.CANCELED;
  }

  private void handleCanceledDuringExecution(Instant start) {
    log.info("Job " + job.getId() + " was canceled during execution - result discarded");

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
            job.getJobType(),
            job.getPriority(),
            job.getPickedBy(),
            JobStatus.RUNNING.name(),
            executionMs));

    handleBatchOrWorkflowCancellation();
  }

  /**
   * Handles cancellation of a batch child or workflow/chain job when the job was canceled during
   * execution. Cancels all dependents unconditionally.
   */
  private void handleBatchOrWorkflowCancellation() {
    if (job.getJobType() == JobType.BATCH_CHILD) {
      lifecycleFacade.markBatchChildFailed(job);
    } else {
      lifecycleFacade.cancelChain(job);
    }
  }

  /**
   * Handles permanent failure of a batch child or workflow/chain job. Unlike cancellation, this
   * evaluates workflow conditions so that FAILURE branches can fire.
   */
  private void handleBatchOrWorkflowPermanentFailure() {
    if (job.getJobType() == JobType.BATCH_CHILD) {
      lifecycleFacade.markBatchChildFailed(job);
    } else {
      lifecycleFacade.scheduleNext(job);
    }
  }

  private void handleFailure(Throwable ex) {
    log.log(
        Level.SEVERE,
        "Job " + job.getId() + " failed with " + ex.getClass().getName() + ": " + ex.getMessage(),
        ex);

    observabilityFacade.recordJobFailure(job, ex);
    logIfTimeout(ex);

    if (validationFacade.shouldNotRetry(ex)) {
      handleNonRetryableFailure(ex);
      return;
    }

    int attempt = jobStore.incrementRetryAttempt(job.getId());
    if (attempt == -1) {
      log.info("Job " + job.getId() + " already in terminal state, skipping retry logic");
      return;
    }

    if (attempt <= job.getMaxRetries() && retryPolicy.shouldRetry(attempt, ex)) {
      scheduleRetry(ex, attempt);
    } else {
      moveToDlq(ex, attempt);
    }
  }

  private void handleNonRetryableFailure(Throwable ex) {
    log.warning(
        "Job "
            + job.getId()
            + " failed with non-retryable exception: "
            + ex.getClass().getName()
            + " - moving directly to DLQ");

    if (currentExecution != null) {
      currentExecution.markFailed(ex);
      observabilityFacade.saveExecution(currentExecution);
    }

    if (jobStore.compareAndSwapStatus(
        job.getId(), JobStatus.RUNNING, JobStatus.FAILED, ex.toString())) {
      job.setStatus(JobStatus.FAILED);
      publishDlqEvent(ex, job.getAttempts());
      lifecycleFacade.moveToDlq(job, ex);
      handleBatchOrWorkflowPermanentFailure();
    }
  }

  private void handleSuccess(Instant start, Object jobResult) {
    observabilityFacade.recordJobSuccess(job);

    Instant endTime = Instant.now();
    long executionMs = Duration.between(start, endTime).toMillis();
    long queueMs =
        job.getPickedAt() != null ? Duration.between(job.getPickedAt(), start).toMillis() : 0;

    String resultJson = null;
    String resultType = null;
    if (jobResult != null) {
      try {
        resultJson = OBJECT_MAPPER.writeValueAsString(jobResult);
        resultType = jobResult.getClass().getName();
      } catch (Exception e) {
        log.warning(
            "Failed to serialize job result for job " + job.getId() + ": " + e.getMessage());
      }
    }

    if (!jobStore.markJobSucceeded(
        job.getId(), resultJson, resultType, start, endTime, executionMs, queueMs)) {
      log.info("Job " + job.getId() + " already in terminal state, skipping success handling");
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
            job.getJobType(),
            job.getPriority(),
            job.getPickedBy(),
            executionMs));

    try {
      lifecycleFacade.handleJobSuccess(job);
    } catch (Exception e) {
      log.warning(
          "Job "
              + job.getId()
              + " [type="
              + job.getJobType()
              + ", key="
              + job.getBusinessKey()
              + "] succeeded but post-success lifecycle processing failed: "
              + e.getClass().getName()
              + ": "
              + e.getMessage());
    }

    log.info("Job " + job.getId() + " succeeded in " + executionMs + " ms");
  }

  private void logIfTimeout(Throwable ex) {
    boolean wasTimeout =
        ex instanceof InterruptedException || ex.getCause() instanceof InterruptedException;
    if (wasTimeout) {
      log.log(Level.SEVERE, "Job " + job.getId() + " was cancelled due to timeout", ex);
    }
  }

  private void moveToDlq(Throwable ex, int attempt) {
    if (currentExecution != null) {
      currentExecution.markFailed(ex);
      observabilityFacade.saveExecution(currentExecution);
    }

    if (jobStore.compareAndSwapStatus(
        job.getId(), JobStatus.RUNNING, JobStatus.FAILED, ex.toString())) {
      job.setAttempts(attempt);
      job.setStatus(JobStatus.FAILED);
      publishDlqEvent(ex, attempt);
      lifecycleFacade.moveToDlq(job, ex);
      handleBatchOrWorkflowPermanentFailure();
      log.log(
          Level.SEVERE, "Job " + job.getId() + " moved to DLQ after " + attempt + " attempts", ex);
    }
  }

  private void publishDlqEvent(Throwable ex, int attempt) {
    observabilityFacade.publishEvent(
        new JobDlqEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getJobType(),
            job.getPriority(),
            job.getPickedBy(),
            ex.toString(),
            attempt));
  }

  private Method resolveMethod(Class<?> clazz, JobPayload payload) throws NoSuchMethodException {
    for (Method m : clazz.getMethods()) {
      if (m.getName().equals(payload.method())
          && Type.getMethodDescriptor(m).equals(payload.methodDescriptor())) {
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

  @SuppressWarnings("java:S112")
  private Object runPayload(JobPayload payload) throws Exception {
    validationFacade.validateSecurity(payload);

    log.info(
        "Job "
            + job.getId()
            + " resolving target: "
            + payload.target()
            + "."
            + payload.method()
            + " (static="
            + payload.isStatic()
            + ")");

    Class<?> cls;
    try {
      cls = Class.forName(payload.target());
    } catch (ClassNotFoundException e) {
      log.log(
          Level.SEVERE, "Job " + job.getId() + " target class not found: " + payload.target(), e);
      throw e;
    }

    Method m;
    try {
      m = resolveMethod(cls, payload);
    } catch (NoSuchMethodException e) {
      log.log(
          Level.SEVERE,
          "Job "
              + job.getId()
              + " target method not found: "
              + payload.method()
              + " with descriptor "
              + payload.methodDescriptor()
              + " in class "
              + payload.target(),
          e);
      throw e;
    }

    if (payload.isStatic()) {
      return m.invoke(null, payload.args().toArray());
    }

    Object bean;
    try {
      bean = beanResolver.resolve(cls);
    } catch (Exception e) {
      log.log(
          Level.SEVERE,
          String.format(
              "Failed to resolve bean for instance method %s in class %s",
              payload.method(), payload.target()),
          e);
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
    observabilityFacade.recordJobRetry(job);

    if (currentExecution != null) {
      currentExecution.markFailed(ex);
      observabilityFacade.saveExecution(currentExecution);
    }

    // Consult RetryPolicy for delay; fall back to job-level backoff configuration
    java.time.Duration policyDelay = retryPolicy.getDelay(attempt);
    long backoff =
        policyDelay.isZero()
            ? BackoffPolicyHandler.computeDelay(
                job.getBackoffPolicy(), job.getBackoffParamMs(), attempt)
            : policyDelay.toMillis();
    Instant newScheduledTime = Instant.now().plusMillis(backoff);

    if (jobStore.scheduleJobRetry(job.getId(), ex.toString(), newScheduledTime, attempt)) {
      job.setAttempts(attempt);
      job.setScheduledTime(newScheduledTime);
      job.setLastError(ex.toString());
      job.setStatus(JobStatus.PENDING);

      observabilityFacade.publishEvent(
          new JobRetryingEvent(
              job.getId(),
              job.getBusinessKey(),
              job.getJobType(),
              job.getPriority(),
              job.getPickedBy(),
              ex.toString(),
              attempt,
              newScheduledTime));
      scheduleReadyJobsUpdate(backoff);

      log.warning(
          "Job "
              + job.getId()
              + " retrying in "
              + backoff
              + " ms (attempt "
              + attempt
              + "/"
              + job.getMaxRetries()
              + ") due to: "
              + ex.getClass().getName()
              + ": "
              + ex.getMessage());
    }
  }

  /**
   * Functional interface for future resilience strategy integration (Phase 4f). When non-null,
   * wraps job method invocations with circuit breaker or bulkhead protection.
   */
  @FunctionalInterface
  public interface ResilienceStrategy {
    Object execute(Callable<Object> action) throws Exception;
  }

  private static class ResourceCapacityException extends RuntimeException {

    @Serial private static final long serialVersionUID = 2983760024642099243L;

    ResourceCapacityException(String message) {
      super(message);
    }
  }
}

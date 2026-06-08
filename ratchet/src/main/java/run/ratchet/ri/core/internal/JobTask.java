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
package run.ratchet.ri.core.internal;

import jakarta.inject.Inject;
import java.io.Serial;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import org.jboss.logging.Logger;
import org.objectweb.asm.Type;
import run.ratchet.api.CircuitBreakerProtected;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.SignalDecision;
import run.ratchet.api.event.JobCallbackFailedEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobStartedEvent;
import run.ratchet.api.exception.CircuitBreakerOpenException;
import run.ratchet.api.exception.JobTimeoutException;
import run.ratchet.api.exception.KeyNotFoundException;
import run.ratchet.api.exception.PayloadDecryptionException;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.api.exception.UnsupportedEnvelopeVersionException;
import run.ratchet.ri.core.DefaultJobSchedulerService;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.ri.payload.ArgumentCoercion;
import run.ratchet.spi.BeanResolver;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.ErrorSanitizer;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobLogger;
import run.ratchet.spi.JobLoggerContext;
import run.ratchet.spi.JobLoggerFactory;
import run.ratchet.spi.NodeIdentityProvider;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.spi.ResilienceStrategy;
import run.ratchet.spi.ResultPersistenceStrategy;
import run.ratchet.spi.RetryPolicy;
import run.ratchet.spi.SerializedJobResult;
import run.ratchet.spi.TracingCollector;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

/**
 * Runs a single job via reflection, handling retries, lifecycle events, and post-execution workflow
 * dispatch. Each instance is single-use and discarded after completion.
 */
public class JobTask implements Callable<Void> {

  /**
   * Maximum entries per reflection cache. Bounds memory use across long-running deployments where
   * the set of distinct job target classes/methods could otherwise grow without limit. LRU
   * (access-order) eviction keeps hot entries resident.
   */
  static final int CACHE_MAX_ENTRIES = 1024;

  private static final Logger log = Logger.getLogger(JobTask.class);
  private static final Map<String, Method> METHOD_CACHE = newBoundedCache(CACHE_MAX_ENTRIES);
  private static final Map<String, Class<?>> CLASS_CACHE = newBoundedCache(CACHE_MAX_ENTRIES);
  private static final Map<String, String> SERVICE_NAME_CACHE = newBoundedCache(CACHE_MAX_ENTRIES);
  private static final Object REFLECTION_CACHE_LOCK = new Object();
  private static final int SUCCESS_FINALIZATION_MAX_ATTEMPTS = 5;
  private static final long[] SUCCESS_FINALIZATION_BACKOFF_MS = {25L, 50L, 100L, 200L, 400L};
  private static final long SUCCESS_FINALIZATION_JITTER_MAX_MS = 25L;

  /**
   * Backoff before a job released for being written by a newer Ratchet is eligible again. Long
   * enough that a laggard node does not hot-loop re-claiming an envelope it cannot read, short
   * enough that an upgraded peer drains it promptly during a rolling upgrade.
   */
  private static final Duration UPGRADE_PENDING_BACKOFF = Duration.ofSeconds(30);

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
  private final JobLoggerFactory jobLoggerFactory;
  private final ResultPersistenceStrategy resultPersistenceStrategy;
  private final JobAuthorizationPolicy authorizationPolicy;
  private final PayloadSerializer payloadSerializer;
  private final Clock clock;
  private JobEntity job;
  private JobClaimDto claim;
  private JobExecutionEntity currentExecution;
  private TracingCollector.ExecutionScope currentScope =
      TracingCollector.NoOpExecutionScope.INSTANCE;
  // volatile: this instance is single-use today (allocated fresh per submission
  // in DefaultJobExecutorService.createTask()), but the field outlives no thread
  // hop only by that invariant. Marking it volatile makes the flag correct under
  // any future task reuse and clears SpotBugs AT_STALE_THREAD_WRITE_OF_PRIMITIVE
  // at zero behavioral cost.
  private volatile boolean permitAcquired;

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
    this.jobLoggerFactory = null;
    this.resultPersistenceStrategy = null;
    this.authorizationPolicy = null;
    this.payloadSerializer = null;
    this.clock = null;
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
      ClassPolicy classPolicy,
      JobLoggerFactory jobLoggerFactory,
      ResultPersistenceStrategy resultPersistenceStrategy,
      JobAuthorizationPolicy authorizationPolicy,
      PayloadSerializer payloadSerializer,
      Clock clock) {
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
    this.jobLoggerFactory = jobLoggerFactory;
    this.resultPersistenceStrategy = resultPersistenceStrategy;
    this.authorizationPolicy = authorizationPolicy;
    this.payloadSerializer = payloadSerializer;
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  /**
   * Creates an LRU map bounded to {@code maxEntries}. Access-order ({@code get()}) promotes entries
   * to the most-recently-used position so hot entries survive eviction pressure. Callers must guard
   * all access with {@code REFLECTION_CACHE_LOCK}; access-order reads mutate the map.
   */
  static <K, V> Map<K, V> newBoundedCache(int maxEntries) {
    return new LinkedHashMap<K, V>(16, 0.75f, true) {
      @Serial private static final long serialVersionUID = 1L;

      @Override
      protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxEntries;
      }
    };
  }

  /**
   * Clears all static reflection caches. Must be called on application shutdown to release
   * classloader references and prevent memory leaks in redeployable containers (e.g., WildFly,
   * Payara).
   */
  public static void clearCaches() {
    synchronized (REFLECTION_CACHE_LOCK) {
      SERVICE_NAME_CACHE.clear();
      METHOD_CACHE.clear();
      CLASS_CACHE.clear();
    }
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
    UUID jobId = getJobId();

    JobEntity jobEntity;
    try {
      jobEntity = getJob();
    } catch (Exception e) {
      // Defensive: bind() hasn't run yet, but MDC.remove() of unset keys is a safe no-op.
      UnsupportedEnvelopeVersionException upgradePending = findUpgradePending(e);
      if (isPoison(e)) {
        // Row hydration decrypted a protected surface and the decrypt failed (tampered ciphertext,
        // wrong/retired key, or an uninstalled algorithm). This is non-retryable per the DLQ
        // contract, so dead-letter the claimed RUNNING job rather than silently returning and
        // leaving it to stall until lease recovery.
        deadLetterPoisonedJob(jobId, e);
      } else if (upgradePending != null) {
        // The row carries an envelope version this node cannot read yet — written by a newer
        // Ratchet during a rolling upgrade. It is valid data, not poison, so release the claim back
        // to the pending pool with backoff (preserving the attempt count) so an already-upgraded
        // peer can drain it, rather than dead-lettering valid work or stalling until lease
        // recovery.
        requeueForUpgrade(jobId, upgradePending);
      } else {
        log.errorf(e, "Job %s failed to load entity from database - aborting execution", jobId);
      }
      JobMdcContext.clear();
      return null;
    }

    String nodeId = nodeIdProvider.getNodeId();
    if (claim != null
        && (jobEntity.getStatus() != JobStatus.RUNNING
            || (jobEntity.getPickedBy() != null && !nodeId.equals(jobEntity.getPickedBy())))) {
      log.infof(
          "Job %s claim no longer owned by this node (status=%s, pickedBy=%s) - skipping execution",
          jobId, jobEntity.getStatus(), jobEntity.getPickedBy());
      JobMdcContext.clear();
      return null;
    }
    JobLogger logger =
        jobLoggerFactory.create(
            new JobLoggerContext(
                jobId,
                jobEntity.getPublicJobType(),
                jobEntity.getPriority(),
                nodeId,
                jobEntity.getCallerPrincipal(),
                jobEntity.getParams()));
    JobType jobType = jobEntity.getPublicJobType();
    Serializable deserializedSignalPayload;
    try {
      deserializedSignalPayload = deserializeSignalPayload(jobEntity, jobId);
    } catch (RuntimeException e) {
      UnsupportedEnvelopeVersionException upgradePending = findUpgradePending(e);
      if (upgradePending != null) {
        requeueForUpgrade(jobId, upgradePending);
      } else {
        handleFailureSafely(e);
      }
      JobMdcContext.clear();
      return null;
    }

    JobMdcContext.bindJobContext(
        jobId,
        logger,
        jobEntity.getParams(),
        nodeId,
        jobEntity.getCallerPrincipal(),
        jobType != null ? jobType.name() : null,
        deserializedSignalPayload);

    // The JobContext/MDC bound above must be cleared on every exit. Open the clearing try right
    // after the bind so a throw from the startup observability calls (metrics, execution recording)
    // cannot leak this job's identity onto the pooled worker thread. The inner try owns
    // execution-failure handling; this outer try only guarantees the bound context is cleared.
    try {
      if (jobEntity.getCallerPrincipal() != null) {
        log.debugf("Job %s created by user (present)", jobId);
      } else {
        log.debugf("Job %s created by system (no user context)", jobId);
      }

      observabilityFacade.recordJobStart(jobEntity);

      int attemptNumber = jobEntity.getAttempts() + 1;
      try {
        currentExecution =
            observabilityFacade.startExecution(jobId, attemptNumber, nodeIdProvider.getNodeId());
      } catch (Throwable executionRecordError) {
        // The execution-history write is durable state, not a metric. When it fails before the
        // payload runs, fail the job through normal failure handling so it reaches a terminal
        // state (or a policy-driven retry) instead of stranding in RUNNING until orphan recovery.
        log.errorf(
            executionRecordError,
            "Job %s execution-history start write failed; failing the job",
            jobId);
        handleFailureSafely(executionRecordError);
        return null;
      }

      Instant start = effective().instant();
      observabilityFacade.publishEvent(
          new JobStartedEvent(
              jobEntity.getId(),
              jobEntity.getBusinessKey(),
              jobEntity.getPublicJobType(),
              jobEntity.getPriority(),
              jobEntity.getPickedBy(),
              start));

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

      Object jobResult;
      permitAcquired = false;
      String resilienceServiceName = resolveResilienceServiceName(jobEntity.getPayload());
      try {
        currentScope = observabilityFacade.startExecutionScope(jobEntity);
        if (wasJobCanceledDuringExecution()) {
          handleCanceledDuringExecution(start);
          return null;
        }

        if (!resilienceStrategy.isServiceAvailable(resilienceServiceName)) {
          log.infof(
              "Job %s skipped - circuit breaker OPEN for service: %s",
              jobId, resilienceServiceName);
          rescheduleForCircuitBreaker(jobEntity, resilienceServiceName);
          return null;
        }

        if (!tryAcquireResourcePermit()) {
          return null;
        }

        // Authorization check before breaker scope — denial must not trip the circuit breaker
        if (authorizationPolicy != null) {
          authorizationPolicy.checkExecute(jobEntity.getId(), jobEntity.getCallerPrincipal());
        }
        // Validate before breaker scope — config errors must not trip the circuit breaker
        validationFacade.validateSecurity(jobEntity.getPayload());

        jobResult =
            resilienceStrategy.execute(
                resilienceServiceName, () -> runPayload(jobEntity.getPayload()));

        if (wasJobCanceledDuringExecution()) {
          handleCanceledDuringExecution(start);
        } else {
          handleSuccess(start, jobResult);
        }
      } catch (CircuitBreakerOpenException e) {
        log.infof(
            "Job %s rescheduled - circuit breaker OPEN for service: %s",
            jobId, resilienceServiceName);
        rescheduleForCircuitBreaker(jobEntity, resilienceServiceName, e);
      } catch (Throwable t) {
        handleFailureSafely(t);
      } finally {
        if (permitAcquired) {
          releaseResourcePermit();
        }
        log.infof("Job %s execution complete - cleaning up context", jobId);
        currentScope.close();
      }
    } finally {
      // Must be Throwable, not Exception — Error must still clear MDC. See
      // JobMdcContextThrowableTest
      JobMdcContext.clear();
    }
    return null;
  }

  public void init(JobEntity job) {
    this.job = job;
    this.claim = null;
  }

  public void initFromClaim(JobClaimDto claim) {
    this.claim = claim;
    this.job = null;
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
    synchronized (REFLECTION_CACHE_LOCK) {
      Class<?> cached = CLASS_CACHE.get(className);
      if (cached != null) {
        return cached;
      }
      Class<?> loaded =
          Class.forName(className, true, Thread.currentThread().getContextClassLoader());
      CLASS_CACHE.put(className, loaded);
      return loaded;
    }
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

  private UUID getJobId() {
    return job != null ? job.getId() : claim.id();
  }

  /**
   * Detects payload-decryption poison anywhere in a throwable's cause chain. These are the
   * exceptions {@link run.ratchet.ri.core.DoNotRetryPolicy} treats as non-retryable: the ciphertext
   * cannot be recovered by re-running, so the job must be dead-lettered rather than retried or left
   * stalled. A non-poison load failure (a missing row, a transient store error) falls through to
   * the caller's normal abort, which leaves the claim for lease/orphan recovery.
   */
  private static boolean isPoison(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof PayloadDecryptionException || c instanceof KeyNotFoundException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the {@link UnsupportedEnvelopeVersionException} in a throwable's cause chain, or {@code
   * null}. A future-version envelope is valid data this node cannot read yet (a newer Ratchet wrote
   * it mid-rollout), distinct from poison: it must be requeued for an upgraded peer, not
   * dead-lettered.
   */
  private static UnsupportedEnvelopeVersionException findUpgradePending(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof UnsupportedEnvelopeVersionException u) {
        return u;
      }
    }
    return null;
  }

  /**
   * Moves a job whose payload failed to decrypt during hydration to a terminal FAILED state without
   * retry, and emits the DLQ event. The entity never finished hydrating, so this works from the job
   * id alone: it CAS-transitions RUNNING to FAILED (the same guard {@link #transitionToDlq} uses)
   * and publishes a best-effort {@link JobDlqEvent}. The richer metadata the normal terminal path
   * carries is unavailable here precisely because the row could not be read.
   */
  private void deadLetterPoisonedJob(UUID jobId, Throwable ex) {
    String safeError;
    try {
      safeError = errorSanitizer.sanitize(ex);
    } catch (Throwable sanitizerError) {
      safeError = ex.getClass().getName();
    }
    log.errorf(
        ex,
        "Job %s carries undecryptable payload ciphertext (poison) — moving to FAILED/DLQ without"
            + " retry",
        jobId);
    boolean moved;
    try {
      moved = jobStore.compareAndSwapStatus(jobId, JobStatus.RUNNING, JobStatus.FAILED, safeError);
    } catch (Throwable t) {
      log.errorf(
          t, "Job %s could not be transitioned to FAILED — will require orphan recovery", jobId);
      return;
    }
    if (moved) {
      try {
        observabilityFacade.publishEvent(
            new JobDlqEvent(jobId, null, null, null, nodeIdProvider.getNodeId(), safeError, 1));
      } catch (Throwable eventError) {
        log.warnf(eventError, "Poison DLQ event publish failed for job %s", jobId);
      }
    }
  }

  /**
   * Releases a claimed job whose payload was written by a newer Ratchet (an envelope version this
   * node cannot read yet) back to the pending pool with backoff, so an already-upgraded peer drains
   * it. The attempt count is preserved — this is not a failed attempt, it is a node that has not
   * caught up — and a metric plus a throttled warning surface a node stuck behind the fleet.
   */
  private void requeueForUpgrade(UUID jobId, UnsupportedEnvelopeVersionException ex) {
    observabilityFacade.recordEnvelopeVersionSkew(jobId, ex.version(), ex.maxSupportedVersion());
    log.warnf(
        "Job %s carries encryption envelope version %d but this node reads up to %d — releasing the"
            + " claim for an upgraded peer. Upgrade this node to drain these rows.",
        jobId, ex.version(), ex.maxSupportedVersion());
    Instant newScheduledTime = effective().instant().plus(UPGRADE_PENDING_BACKOFF);
    int attempts = claim != null ? claim.attempts() : 0;
    try {
      jobStore.scheduleJobRetry(jobId, ex.getMessage(), newScheduledTime, attempts);
    } catch (Throwable t) {
      log.errorf(
          t,
          "Job %s could not be released for upgrade — will require lease/orphan recovery",
          jobId);
    }
  }

  private Serializable deserializeSignalPayload(JobEntity jobEntity, UUID jobId) {
    // Decrypt at rest before deserializing; no-op when no cipher is active or the value is
    // plaintext. Let the decryption exception types propagate unwrapped so DoNotRetryPolicy can
    // tell poison (corrupt ciphertext / forgotten key -> DLQ) from a transient key-provider outage
    // (KeyProviderUnavailableException -> retry). Wrapping them in IllegalArgumentException here
    // would force every case onto the non-retryable path and dead-letter a job a retry could have
    // recovered.
    String rawSignalPayload =
        PayloadEncryptor.decryptValue(
            jobEntity.getSignalPayload(), EncryptionTarget.signal(jobEntity.getSignalKey()));

    if (DefaultJobSchedulerService.SIGNAL_PAYLOAD_TYPE_DECISION.equals(
        jobEntity.getSignalPayloadType())) {
      Serializable innerPayload = null;
      if (rawSignalPayload != null && payloadSerializer != null) {
        try {
          Object obj = payloadSerializer.deserialize(rawSignalPayload, Object.class);
          if (obj instanceof Serializable s) {
            innerPayload = s;
          }
        } catch (Exception e) {
          log.warnf(
              "Failed to deserialize signal inner payload for job %s: %s", jobId, e.getMessage());
        }
      }
      String outcomeStr = jobEntity.getSignalOutcome();
      if (outcomeStr != null) {
        SignalDecision.Outcome outcome = SignalDecision.Outcome.valueOf(outcomeStr);
        return new SignalDecision(outcome, innerPayload, jobEntity.getSignalRejectionReason());
      }
    } else if (rawSignalPayload != null && payloadSerializer != null) {
      // Deserialize to Object.class, not Serializable.class: JSON-B cannot instantiate the abstract
      // Serializable target, so the old form always threw and silently left the payload null. This
      // mirrors the SignalDecision inner-payload branch above. The payload round-trips as its
      // JSON-native shape (String / Number / Boolean / List / Map); see signalPayload(Class).
      try {
        Object obj = payloadSerializer.deserialize(rawSignalPayload, Object.class);
        if (obj instanceof Serializable s) {
          return s;
        }
      } catch (Exception e) {
        log.warnf("Failed to deserialize signal payload for job %s: %s", jobId, e.getMessage());
      }
    }
    return null;
  }

  private void handleFailureSafely(Throwable t) {
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
        if (jobStore.compareAndSwapStatus(
            job.getId(), JobStatus.RUNNING, JobStatus.FAILED, safeError)) {
          publishForcedTerminalFailure(t, safeError);
        }
      } catch (Throwable lastResort) {
        log.errorf(
            lastResort,
            "Job %s could not be transitioned to FAILED — will require orphan recovery",
            job.getId());
      }
    }
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
        Instant newScheduledTime = effective().instant().plusMillis(retryDelay);

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
        log.warnf(e, "Permit release error for job %s", job.getId());
      }
    }
  }

  /**
   * Reschedules a job when the circuit breaker is open, without counting it as a retry attempt.
   * Uses the same pattern as resource permit unavailability — the job goes back to PENDING with a
   * delay matching the circuit breaker's typical OPEN-to-HALF_OPEN transition window.
   */
  private void rescheduleForCircuitBreaker(JobEntity jobEntity, String serviceName) {
    rescheduleForCircuitBreaker(
        jobEntity,
        serviceName,
        new CircuitBreakerOpenException("Circuit breaker OPEN for service: " + serviceName));
  }

  private void rescheduleForCircuitBreaker(
      JobEntity jobEntity, String serviceName, CircuitBreakerOpenException rejection) {
    long delayMs = resilienceStrategy.getRetryDelay(serviceName).toMillis();
    Instant newScheduledTime = effective().instant().plusMillis(delayMs);

    if (currentExecution != null) {
      currentExecution.markFailed(rejection);
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

    Instant endTime = effective().instant();
    long executionMs = Duration.between(start, endTime).toMillis();

    job.setExecutionStartTime(start);
    job.setExecutionEndTime(endTime);
    job.setExecutionDurationMs(executionMs);

    if (currentExecution != null) {
      currentExecution.markCanceled();
      observabilityFacade.saveExecution(currentExecution);
    }

    observabilityFacade.recordJobCancellation(job);

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

    // Non-retryable: skip retry count increment
    if (validationFacade.shouldNotRetry(ex)) {
      observabilityFacade.recordJobFailure(job, ex, job.getAttempts());
      currentScope.failure(ex, job.getAttempts());
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
    currentScope.failure(ex, attempt);
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

    transitionToDlq(ex, attempt);
  }

  private void handleSuccess(Instant start, Object jobResult) {
    Instant endTime = effective().instant();
    long executionMs = Duration.between(start, endTime).toMillis();
    long queueMs =
        job.getPickedAt() != null ? Duration.between(job.getPickedAt(), start).toMillis() : 0;

    SerializedJobResult serializedResult =
        resultPersistenceStrategy.serialize(job.getId(), jobResult);
    String resultJson = serializedResult.json();
    String resultType = serializedResult.type();

    SuccessFinalizationState finalizationState =
        finalizeSuccessWithRetry(resultJson, resultType, start, endTime, executionMs, queueMs);
    if (finalizationState == SuccessFinalizationState.TERMINAL_SKIPPED) {
      log.infof("Job %s already in terminal state, skipping success handling", job.getId());
      return;
    }
    if (finalizationState == SuccessFinalizationState.STUCK) {
      return;
    }

    observabilityFacade.recordJobSuccess(job, executionMs);
    currentScope.success(executionMs);

    if (currentExecution != null) {
      currentExecution.markSucceeded();
      observabilityFacade.saveExecution(currentExecution);
    }

    job.setStatus(JobStatus.SUCCEEDED);
    job.setJobResult(
        finalizationState == SuccessFinalizationState.COMPLETED_FULL ? resultJson : null);
    job.setResultType(
        finalizationState == SuccessFinalizationState.COMPLETED_FULL ? resultType : null);
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
            endTime,
            executionMs));

    try {
      lifecycleFacade.handleJobSuccess(job);
    } catch (Exception e) {
      log.warnf(
          e,
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

  private SuccessFinalizationState finalizeSuccessWithRetry(
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      long durationMs,
      long queueWaitMs) {
    for (int attempt = 1; attempt <= SUCCESS_FINALIZATION_MAX_ATTEMPTS; attempt++) {
      try {
        boolean updated =
            jobStore.markJobSucceeded(
                job.getId(), resultJson, resultType, start, end, durationMs, queueWaitMs);
        return updated
            ? SuccessFinalizationState.COMPLETED_FULL
            : SuccessFinalizationState.TERMINAL_SKIPPED;
      } catch (RatchetTransientStoreException e) {
        observabilityFacade.recordSuccessFinalizationRetry(job);
        if (attempt == SUCCESS_FINALIZATION_MAX_ATTEMPTS) {
          log.warnf(
              "Job %s exhausted success finalization retries after transient store conflicts: %s",
              job.getId(), e.getMessage());
          break;
        }

        log.warnf(
            "Job %s transient success finalization failure on attempt %s/%s: %s",
            job.getId(), attempt, SUCCESS_FINALIZATION_MAX_ATTEMPTS, e.getMessage());

        if (!sleepBeforeSuccessFinalizationRetry(attempt)) {
          break;
        }
      }
    }

    try {
      boolean updated =
          jobStore.markJobSucceededMinimal(job.getId(), start, end, durationMs, queueWaitMs);
      if (updated) {
        observabilityFacade.recordSuccessFinalizationMinimal(job);
        log.warnf(
            "Job %s persisted minimal success after transient store finalization conflicts",
            job.getId());
        return SuccessFinalizationState.COMPLETED_MINIMAL;
      }
      return SuccessFinalizationState.TERMINAL_SKIPPED;
    } catch (RatchetTransientStoreException e) {
      observabilityFacade.recordSuccessFinalizationStuck(job);
      log.errorf(
          e,
          "Job %s succeeded but success finalization is stuck after transient store conflicts",
          job.getId());
      return SuccessFinalizationState.STUCK;
    }
  }

  private boolean sleepBeforeSuccessFinalizationRetry(int attempt) {
    long baseDelay =
        SUCCESS_FINALIZATION_BACKOFF_MS[
            Math.min(attempt - 1, SUCCESS_FINALIZATION_BACKOFF_MS.length - 1)];
    long jitter = ThreadLocalRandom.current().nextLong(SUCCESS_FINALIZATION_JITTER_MAX_MS + 1L);
    try {
      Thread.sleep(baseDelay + jitter);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warnf("Job %s finalization retry interrupted", job.getId());
      return false;
    }
  }

  // Matches JobTimeoutException, TimeoutException, and InterruptedException at top level and
  // one cause deep — all three are produced by JobTimeoutHandler cancel paths.
  private void logIfTimeout(Throwable ex) {
    boolean wasTimeout =
        ex instanceof JobTimeoutException
            || ex instanceof TimeoutException
            || ex instanceof InterruptedException
            || ex.getCause() instanceof JobTimeoutException
            || ex.getCause() instanceof TimeoutException
            || ex.getCause() instanceof InterruptedException;
    if (wasTimeout) {
      log.errorf(ex, "Job %s was cancelled due to timeout", job.getId());
    }
  }

  private void moveToDlq(Throwable ex, int attempt) {
    if (transitionToDlq(ex, attempt)) {
      log.errorf(ex, "Job %s moved to DLQ after %s attempts", job.getId(), attempt);
    }
  }

  private boolean transitionToDlq(Throwable ex, int attempt) {
    if (currentExecution != null) {
      currentExecution.markFailed(ex);
      observabilityFacade.saveExecution(currentExecution);
    }

    if (jobStore.compareAndSwapStatus(
        job.getId(), JobStatus.RUNNING, JobStatus.FAILED, errorSanitizer.sanitize(ex))) {
      job.setAttempts(attempt);
      job.setStatus(JobStatus.FAILED);
      publishTerminalFailureEvents(ex, attempt);
      lifecycleFacade.moveToDlq(job, ex);
      handleBatchOrWorkflowPermanentFailure();
      invokeCallback(job.getOnFailurePayload(), "onFailure");
      return true;
    }
    return false;
  }

  private void publishTerminalFailureEvents(Throwable ex, int attempt) {
    String sanitized = errorSanitizer.sanitize(ex);
    Instant timestamp = effective().instant();
    observabilityFacade.publishEvent(
        new JobFailedEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            sanitized,
            attempt));
    observabilityFacade.publishEvent(
        new JobDlqEvent(
            job.getId(),
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            timestamp,
            sanitized,
            attempt));
  }

  private void publishForcedTerminalFailure(Throwable ex, String safeError) {
    job.setStatus(JobStatus.FAILED);
    int attempt = Math.max(1, job.getAttempts());
    try {
      observabilityFacade.publishEvent(
          new JobFailedEvent(
              job.getId(),
              job.getBusinessKey(),
              job.getPublicJobType(),
              job.getPriority(),
              job.getPickedBy(),
              safeError,
              attempt));
      observabilityFacade.publishEvent(
          new JobDlqEvent(
              job.getId(),
              job.getBusinessKey(),
              job.getPublicJobType(),
              job.getPriority(),
              job.getPickedBy(),
              safeError,
              attempt));
    } catch (Throwable eventError) {
      log.warnf(eventError, "Fallback failure event publish failed for job %s", job.getId());
    }
    try {
      lifecycleFacade.moveToDlq(job, ex);
    } catch (Throwable dlqError) {
      log.warnf(dlqError, "Fallback DLQ handling failed for job %s", job.getId());
    }
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
      invokeTargetMethod(method, target, args);
    } catch (Exception e) {
      // Log + metric + event; parent job still succeeds
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
        log.warnf("Callback metric error for job %s: %s", job.getId(), metricEx.getMessage());
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
                effective().instant(),
                type,
                e.getMessage(),
                e.getClass().getName(),
                1));
      } catch (Exception eventEx) {
        log.warnf("Callback event publish error for job %s: %s", job.getId(), eventEx.getMessage());
      }
    }
  }

  private Method resolveMethod(Class<?> clazz, JobPayload payload) throws NoSuchMethodException {
    String cacheKey = clazz.getName() + "#" + payload.method() + ":" + payload.methodDescriptor();
    synchronized (REFLECTION_CACHE_LOCK) {
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
  }

  private String resolveResilienceServiceName(JobPayload payload) {
    String cacheKey = payload.target() + "#" + payload.method() + ":" + payload.methodDescriptor();
    synchronized (REFLECTION_CACHE_LOCK) {
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

    List<Object> args = payload.args() != null ? payload.args() : List.of();
    if (payload.isStatic()) {
      return invokeTargetMethod(m, null, args);
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

    return invokeTargetMethod(m, bean, args);
  }

  private Object invokeTargetMethod(Method method, Object target, List<Object> args)
      throws Exception {
    try {
      return method.invoke(
          target, ArgumentCoercion.coerce(method.getParameterTypes(), args.toArray()));
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw e;
    }
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

    // Consult RetryPolicy for delay; fall back to job-level backoff configuration. The SPI
    // contract requires a non-null Duration; enforce at the boundary so a misbehaving
    // implementation produces a meaningful error instead of an uninformative NPE deeper in.
    Duration policyDelay =
        Objects.requireNonNull(
            retryPolicy.getDelay(attempt),
            "RetryPolicy.getDelay must not return null (attempt=" + attempt + ")");
    long backoff =
        policyDelay.isZero()
            ? BackoffPolicyHandler.computeDelay(
                job.getBackoffPolicy(), job.getBackoffParamMs(), attempt)
            : policyDelay.toMillis();
    Instant timestamp = effective().instant();
    Instant newScheduledTime = timestamp.plusMillis(backoff);
    String sanitizedError = errorSanitizer.sanitize(ex);

    if (jobStore.scheduleJobRetry(job.getId(), sanitizedError, newScheduledTime, attempt)) {
      job.setAttempts(attempt);
      job.setScheduledTime(newScheduledTime);
      job.setLastError(sanitizedError);
      job.setStatus(JobStatus.PENDING);

      observabilityFacade.publishEvent(
          new JobRetryingEvent(
              job.getId(),
              job.getBusinessKey(),
              job.getPublicJobType(),
              job.getPriority(),
              job.getPickedBy(),
              timestamp,
              sanitizedError,
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

  private Clock effective() {
    if (clock == null) {
      throw new IllegalStateException("JobTask clock was not initialized");
    }
    return clock;
  }

  private enum SuccessFinalizationState {
    COMPLETED_FULL,
    COMPLETED_MINIMAL,
    TERMINAL_SKIPPED,
    STUCK
  }

  private static class ResourceCapacityException extends RuntimeException {

    @Serial private static final long serialVersionUID = 2983760024642099243L;

    ResourceCapacityException(String message) {
      super(message);
    }
  }
}

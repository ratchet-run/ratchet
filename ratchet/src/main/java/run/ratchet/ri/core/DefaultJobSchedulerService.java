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
package run.ratchet.ri.core;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.jboss.logging.Logger;
import run.ratchet.api.BatchBuilder;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SignalDecision;
import run.ratchet.api.StreamingBatchBuilder;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.api.event.JobPausedEvent;
import run.ratchet.api.event.JobResumedEvent;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.api.event.JobSignaledEvent;
import run.ratchet.api.event.JobsBulkCancelledEvent;
import run.ratchet.api.event.JobsBulkSignaledEvent;
import run.ratchet.ri.core.internal.InternalEventPublisher;
import run.ratchet.ri.core.internal.JobWakeupService;
import run.ratchet.ri.core.internal.JobWakeupService.AfterCommitRegistrationResult;
import run.ratchet.ri.core.internal.RecurringAnnotationMaintenanceService;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.converter.EncryptionHolder;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.util.EncryptionTarget;
import run.ratchet.store.util.PayloadEncryptor;

/** Core scheduling API implementation. Delegates builder persistence to a CDI-managed service. */
@ApplicationScoped
public class DefaultJobSchedulerService
    implements JobSchedulerService, RecurringAnnotationMaintenanceService {

  // signal_payload_type markers persisted in the VARCHAR(16) column. These two values are the only
  // ones defined: RAW for a payload delivered via deliverSignal(.., Serializable), DECISION for a
  // SignalDecision. JobTask switches on DECISION to rebuild the wrapper; everything else is RAW.
  public static final String SIGNAL_PAYLOAD_TYPE_DECISION = "DECISION";
  static final String SIGNAL_PAYLOAD_TYPE_RAW = "RAW";
  // Used when the caller principal can't be resolved (e.g. no Elytron context in tests).
  // JobSignaledEvent's contract requires non-null signalDeliveredBy.
  private static final String DEFAULT_SIGNAL_DELIVERED_BY = "system";
  private static final Logger log = Logger.getLogger(DefaultJobSchedulerService.class);
  private final InternalEventPublisher eventPublisher;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobPauseStore jobPauseStore;
  private final JobRetryStore jobRetryStore;
  private final JobTerminalStore jobTerminalStore;
  private final JobCrudStore jobCrudStore;
  private final BatchStore batchStore;
  private final TagStore tagStore;
  private final WorkflowConditionStore workflowConditionStore;
  private final RecurringJobStore recurringJobStore;
  private final JobWakeupService wakeupService;
  private final RecurringScheduler recurringScheduler;
  private final JobInvocationResolver jobInvocationResolver;
  private final DefaultJobCreationService jobCreationService;
  private final CallerPrincipalProvider callerPrincipalProvider;
  private final JobAuthorizationPolicy authorizationPolicy;
  private final SignalStore signalStore;
  private final PayloadSerializer payloadSerializer;
  private final MetricsCollector metricsCollector;
  private final Clock clock;

  // Resolved lazily on first use from a Jakarta EE component thread. @Resource field injection
  // fails on Payara when CDI startup observers run on the admin thread (no java:comp/env context).
  private volatile TransactionSynchronizationRegistry txRegistry;

  protected DefaultJobSchedulerService() {
    this.eventPublisher = null;
    this.jobBatchStatusStore = null;
    this.jobPauseStore = null;
    this.jobRetryStore = null;
    this.jobTerminalStore = null;
    this.jobCrudStore = null;
    this.batchStore = null;
    this.tagStore = null;
    this.workflowConditionStore = null;
    this.recurringJobStore = null;
    this.wakeupService = null;
    this.recurringScheduler = null;
    this.jobInvocationResolver = null;
    this.jobCreationService = null;
    this.callerPrincipalProvider = null;
    this.authorizationPolicy = null;
    this.signalStore = null;
    this.payloadSerializer = null;
    this.metricsCollector = null;
    this.clock = null;
  }

  DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      RecurringJobStore recurringJobStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      DefaultJobCreationService jobCreationService,
      CallerPrincipalProvider callerPrincipalProvider,
      JobAuthorizationPolicy authorizationPolicy,
      SignalStore signalStore,
      PayloadSerializer payloadSerializer) {
    this(
        eventPublisher,
        jobBatchStatusStore,
        jobPauseStore,
        jobRetryStore,
        jobTerminalStore,
        jobCrudStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        jobInvocationResolver,
        jobCreationService,
        callerPrincipalProvider,
        authorizationPolicy,
        signalStore,
        payloadSerializer,
        null,
        Clock.systemUTC());
  }

  DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      RecurringJobStore recurringJobStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      DefaultJobCreationService jobCreationService,
      CallerPrincipalProvider callerPrincipalProvider,
      JobAuthorizationPolicy authorizationPolicy,
      SignalStore signalStore,
      PayloadSerializer payloadSerializer,
      MetricsCollector metricsCollector) {
    this(
        eventPublisher,
        jobBatchStatusStore,
        jobPauseStore,
        jobRetryStore,
        jobTerminalStore,
        jobCrudStore,
        batchStore,
        tagStore,
        workflowConditionStore,
        recurringJobStore,
        wakeupService,
        recurringScheduler,
        jobInvocationResolver,
        jobCreationService,
        callerPrincipalProvider,
        authorizationPolicy,
        signalStore,
        payloadSerializer,
        metricsCollector,
        Clock.systemUTC());
  }

  @Inject
  DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      Instance<BatchStore> batchStore,
      TagStore tagStore,
      Instance<WorkflowConditionStore> workflowConditionStore,
      Instance<RecurringJobStore> recurringJobStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      DefaultJobCreationService jobCreationService,
      CallerPrincipalProvider callerPrincipalProvider,
      JobAuthorizationPolicy authorizationPolicy,
      Instance<SignalStore> signalStore,
      PayloadSerializer payloadSerializer,
      MetricsCollector metricsCollector,
      Clock clock) {
    this(
        eventPublisher,
        jobBatchStatusStore,
        jobPauseStore,
        jobRetryStore,
        jobTerminalStore,
        jobCrudStore,
        batchStore.isResolvable() ? batchStore.get() : null,
        tagStore,
        workflowConditionStore.isResolvable() ? workflowConditionStore.get() : null,
        recurringJobStore.isResolvable() ? recurringJobStore.get() : null,
        wakeupService,
        recurringScheduler,
        jobInvocationResolver,
        jobCreationService,
        callerPrincipalProvider,
        authorizationPolicy,
        signalStore.isResolvable() ? signalStore.get() : null,
        payloadSerializer,
        metricsCollector,
        clock);
  }

  DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      RecurringJobStore recurringJobStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler,
      JobInvocationResolver jobInvocationResolver,
      DefaultJobCreationService jobCreationService,
      CallerPrincipalProvider callerPrincipalProvider,
      JobAuthorizationPolicy authorizationPolicy,
      SignalStore signalStore,
      PayloadSerializer payloadSerializer,
      MetricsCollector metricsCollector,
      Clock clock) {
    this.eventPublisher = eventPublisher;
    this.jobBatchStatusStore = jobBatchStatusStore;
    this.jobPauseStore = jobPauseStore;
    this.jobRetryStore = jobRetryStore;
    this.jobTerminalStore = jobTerminalStore;
    this.jobCrudStore = jobCrudStore;
    this.batchStore = batchStore;
    this.tagStore = tagStore;
    this.workflowConditionStore = workflowConditionStore;
    this.recurringJobStore = recurringJobStore;
    this.wakeupService = wakeupService;
    this.recurringScheduler = recurringScheduler;
    this.jobInvocationResolver = jobInvocationResolver;
    this.jobCreationService = jobCreationService;
    this.callerPrincipalProvider = callerPrincipalProvider;
    this.authorizationPolicy = authorizationPolicy;
    this.signalStore = signalStore;
    this.payloadSerializer = payloadSerializer;
    this.metricsCollector = metricsCollector;
    this.clock = clock;
  }

  @Override
  @Transactional
  public boolean cancelJob(UUID jobId) {
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    // Recurring masters do not live in scheduler_job. Fall back to RecurringJobStore so the
    // authorization policy sees the master's captured callerPrincipal instead of a null owner.
    String recurringOwner = null;
    if (job == null && recurringJobStore != null) {
      recurringOwner =
          recurringJobStore.getRecurring(jobId).map(d -> d.callerPrincipal()).orElse(null);
    }
    if (authorizationPolicy != null) {
      // Pre-load to obtain ownerPrincipal. TOCTOU: if the job is deleted between this load
      // and the CAS below, ownerPrincipal will be null — the policy must tolerate null.
      String ownerPrincipal = job != null ? job.getCallerPrincipal() : recurringOwner;
      String currentPrincipal =
          callerPrincipalProvider != null
              ? callerPrincipalProvider.currentPrincipal().orElse(null)
              : null;
      authorizationPolicy.checkCancel(jobId, ownerPrincipal, currentPrincipal);
    }

    // Try PENDING → CANCELED first (most common case)
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.PENDING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled pending job %s", jobId);
      publishCancelledEvent(jobId, JobStatus.PENDING, job);
      return true;
    }

    // Try RUNNING → CANCELED. The scheduler publishes the durable state-transition event here so
    // observers do not lose cancellation if the worker dies before it notices the status flip.
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled running job %s", jobId);
      publishCancelledEvent(jobId, JobStatus.RUNNING, job);
      return true;
    }

    // Try PAUSED → CANCELED
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.PAUSED, JobStatus.CANCELED, null)) {
      log.debugf("Canceled paused job %s", jobId);
      publishCancelledEvent(jobId, JobStatus.PAUSED, job);
      return true;
    }

    // Try WAITING → CANCELED (signal-waiting jobs can be canceled)
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.WAITING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled waiting job %s", jobId);
      if (metricsCollector != null && job != null) {
        metricsCollector.signalCancelled(jobId, job.getPublicJobType(), job.getSignalKey());
      }
      publishCancelledEvent(jobId, JobStatus.WAITING, job);
      return true;
    }

    // Recurring masters live in scheduler_recurring_job, not the executable queue. Fall through to
    // the dedicated SPI so callers holding a recurring master's UUID can cancel it the same way
    // they cancel an executable job. cancelRecurringAndArchive returns false if the id is unknown.
    if (recurringJobStore != null) {
      Optional<RecurringJobDefinition> def = recurringJobStore.getRecurring(jobId);
      if (def.isPresent()
          && recurringJobStore.cancelRecurringAndArchive(
              jobId, RecurringJobStore.ArchiveReason.CANCELED)) {
        log.debugf("Canceled recurring master %s", jobId);
        publishCancelledEventForRecurring(jobId, def.get());
        return true;
      }
    }

    log.debugf("Cannot cancel job %s — already in terminal state or not found", jobId);
    return false;
  }

  @Override
  @Transactional(Transactional.TxType.NOT_SUPPORTED)
  public void addEventListener(Consumer<Object> listener) {
    eventPublisher.addListener(listener);
  }

  @Override
  @Transactional(Transactional.TxType.NOT_SUPPORTED)
  public void removeEventListener(Consumer<Object> listener) {
    eventPublisher.removeListener(listener);
  }

  @Override
  @Transactional
  public int deliverSignal(UUID jobId, Serializable payload) {
    return deliverSignalRaw(jobId, payload);
  }

  @Override
  @Transactional
  public int deliverSignal(UUID jobId, SignalDecision decision) {
    if (decision == null) {
      throw new IllegalArgumentException("decision must not be null");
    }
    return deliverSignalDecision(jobId, decision);
  }

  @Override
  @Transactional
  public int deliverSignal(String signalKey, Serializable payload) {
    return deliverSignalRaw(signalKey, payload);
  }

  @Override
  @Transactional
  public int deliverSignal(String signalKey, SignalDecision decision) {
    if (decision == null) {
      throw new IllegalArgumentException("decision must not be null");
    }
    return deliverSignalDecision(signalKey, decision);
  }

  @Override
  @Transactional(Transactional.TxType.SUPPORTS)
  public JobBuilder enqueue(SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(jobCreationService, task, Duration.ZERO);
  }

  @Override
  @Transactional(Transactional.TxType.SUPPORTS)
  public JobBuilder schedule(Duration delay, SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(jobCreationService, task, delay);
  }

  @Override
  @Transactional(Transactional.TxType.SUPPORTS)
  public BatchBuilder enqueueBatch(String name) {
    return new DefaultBatchBuilder(name, jobCreationService, jobInvocationResolver);
  }

  @Override
  @Transactional(Transactional.TxType.SUPPORTS)
  public <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name) {
    return new DefaultStreamingBatchBuilder<>(name, jobCreationService);
  }

  @Override
  @Transactional(Transactional.TxType.SUPPORTS)
  public RecurringJobBuilder scheduleRecurring(
      String cron, ZoneId zone, SerializableCheckedRunnable task) {
    return new DefaultRecurringJobBuilder(cron, zone, task, jobCreationService);
  }

  @Override
  @Transactional
  public JobHandle replace(
      UUID jobId, Duration delay, SerializableCheckedRunnable newTask, JobOptions opts) {
    // Fail fast if the job doesn't exist — don't create an orphaned replacement below.
    // The entity is loaded here (not discarded) so we can pass ownerPrincipal to the
    // authorization check. The final save() after the CAS block reloads a fresh snapshot
    // whose version is post-CAS. See the block comment on that reload for the stale-version
    // rationale.
    JobEntity existing =
        jobCrudStore
            .findById(jobId)
            .orElseThrow(
                () -> new IllegalArgumentException("Job not found for replacement: " + jobId));
    if (authorizationPolicy != null) {
      String currentPrincipal =
          callerPrincipalProvider != null
              ? callerPrincipalProvider.currentPrincipal().orElse(null)
              : null;
      authorizationPolicy.checkCancel(jobId, existing.getCallerPrincipal(), currentPrincipal);
    }
    if (existing.getSupersededBy() != null) {
      UUID replacementId = existing.getSupersededBy();
      log.infof("Job %s was already replaced by job %s", jobId, replacementId);
      return () -> replacementId;
    }

    JobBuilder builder = DefaultJobBuilder.create(jobCreationService, newTask, delay);
    if (opts != null) {
      builder
          .withPriority(opts.priority())
          .withMaxRetries(opts.maxRetries())
          .withBackoff(opts.backoffPolicy(), opts.backoffParam())
          .withTimeout(Duration.ofSeconds(opts.timeoutSec()));
    }
    JobHandle newHandle = builder.submit();

    JobStatus cancelledFrom = cancelForReplacement(jobId, existing);
    if (cancelledFrom == JobStatus.WAITING && metricsCollector != null) {
      metricsCollector.signalCancelled(jobId, existing.getPublicJobType(), existing.getSignalKey());
    }
    if (cancelledFrom != null) {
      publishCancelledEvent(jobId, cancelledFrom, existing);
    }

    // Reload after CAS: compareAndSwapStatus bumps the optimistic-lock version; writing the
    // pre-CAS entity would throw. The intent is to annotate a terminal CANCELED row with
    // supersededBy as an audit trail.
    JobEntity fresh =
        jobCrudStore
            .findById(jobId)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Job " + jobId + " vanished mid-replace (deleted between load and save)"));
    if (cancelledFrom != null) {
      fresh.setStatus(JobStatus.CANCELED);
    }
    fresh.setSupersededBy(newHandle.id());
    jobCrudStore.save(fresh);

    if (cancelledFrom != null) {
      log.infof("Replaced job %s with new job %s", jobId, newHandle.id());
    } else {
      log.infof(
          "Replaced job %s with new job %s (old job already in terminal state)",
          jobId, newHandle.id());
    }
    return newHandle;
  }

  @Override
  @Transactional
  public boolean pauseJob(UUID jobId) {
    // Recurring masters live in their own table. recurringJobStore is optional in test wiring
    // (the no-arg constructor leaves it null), so guard before the lookup.
    var recurring =
        recurringJobStore != null
            ? recurringJobStore.getRecurring(jobId)
            : Optional.<RecurringJobDefinition>empty();
    if (recurring.isPresent()) {
      if (authorizationPolicy != null) {
        String currentPrincipal =
            callerPrincipalProvider != null
                ? callerPrincipalProvider.currentPrincipal().orElse(null)
                : null;
        authorizationPolicy.checkPause(jobId, recurring.get().callerPrincipal(), currentPrincipal);
      }
      if (recurring.get().paused()) {
        log.debugf("Recurring master %s is already paused", jobId);
        return true;
      }
      if (recurringJobStore.pauseRecurring(jobId)) {
        log.debugf("Paused recurring master %s", jobId);
        publishPausedEventForRecurring(jobId, recurring.get());
        return true;
      }
      log.debugf("Cannot pause recurring master %s — concurrent state transition", jobId);
      return false;
    }

    // Idempotent: already paused is a no-op success
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.debugf("Cannot pause job %s — not found", jobId);
      return false;
    }
    if (authorizationPolicy != null) {
      String currentPrincipal =
          callerPrincipalProvider != null
              ? callerPrincipalProvider.currentPrincipal().orElse(null)
              : null;
      authorizationPolicy.checkPause(jobId, job.getCallerPrincipal(), currentPrincipal);
    }
    JobStatus current = job.getStatus();
    if (current == JobStatus.PAUSED) {
      log.debugf("Job %s is already paused", jobId);
      return true;
    }

    // Executable jobs: try PENDING → PAUSED on hot.
    if (jobPauseStore.transitionToPaused(jobId, JobStatus.PENDING)) {
      log.debugf("Paused pending job %s", jobId);
      publishPausedEvent(jobId, job);
      return true;
    }

    log.debugf(
        "Cannot pause job %s — current status %s is not pausable (only PENDING is eligible)",
        jobId, current);
    return false;
  }

  @Override
  @Transactional
  public boolean resumeJob(UUID jobId) {
    // Recurring masters: dedicated CAS resume on scheduler_recurring_job. recurringJobStore is
    // optional in test wiring; guard before the lookup.
    var recurring =
        recurringJobStore != null
            ? recurringJobStore.getRecurring(jobId)
            : Optional.<RecurringJobDefinition>empty();
    if (recurring.isPresent()) {
      if (authorizationPolicy != null) {
        String currentPrincipal =
            callerPrincipalProvider != null
                ? callerPrincipalProvider.currentPrincipal().orElse(null)
                : null;
        authorizationPolicy.checkResume(jobId, recurring.get().callerPrincipal(), currentPrincipal);
      }
      if (recurringJobStore.resumeRecurring(jobId)) {
        log.debugf("Resumed recurring master %s", jobId);
        publishResumedEventForRecurring(jobId, recurring.get());
        recurringScheduler.kick();
        return true;
      }
      log.debugf("Cannot resume recurring master %s — not paused", jobId);
      return false;
    }

    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (job == null) {
      log.debugf("Cannot resume job %s — not found", jobId);
      return false;
    }
    if (authorizationPolicy != null) {
      String currentPrincipal =
          callerPrincipalProvider != null
              ? callerPrincipalProvider.currentPrincipal().orElse(null)
              : null;
      authorizationPolicy.checkResume(jobId, job.getCallerPrincipal(), currentPrincipal);
    }

    JobStatus target = jobPauseStore.transitionFromPausedAtomic(jobId);
    if (target == null) {
      log.debugf(
          "Cannot resume job %s — not in PAUSED state (current: %s)", jobId, job.getStatus());
      return false;
    }
    log.debugf("Resumed job %s to %s", jobId, target);
    publishResumedEvent(jobId, job);
    if (target == JobStatus.PENDING) {
      recurringScheduler.kick();
    }
    return true;
  }

  @Override
  @Transactional
  public boolean retryJob(UUID jobId) {
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    if (authorizationPolicy != null) {
      // Pre-load to obtain ownerPrincipal. TOCTOU: if the job is deleted between this load
      // and the CAS below, ownerPrincipal will be null — the policy must tolerate null.
      String ownerPrincipal = job != null ? job.getCallerPrincipal() : null;
      String currentPrincipal =
          callerPrincipalProvider != null
              ? callerPrincipalProvider.currentPrincipal().orElse(null)
              : null;
      authorizationPolicy.checkRetry(jobId, ownerPrincipal, currentPrincipal);
    }

    if (jobRetryStore.resetFailedToPending(jobId)) {
      log.debugf("Retried failed job %s — reset to PENDING", jobId);
      publishRetryingEventAndWakeup(jobId, job);
      return true;
    }

    JobStatus current = jobCrudStore.getJobStatus(jobId);
    if (current == null) {
      log.debugf("Cannot retry job %s — not found", jobId);
    } else {
      log.debugf("Cannot retry job %s — not in FAILED state (current: %s)", jobId, current);
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p><strong>Authorization note:</strong> this bulk operation is not subject to per-job {@link
   * JobAuthorizationPolicy} checks. Use {@link #cancelJob(UUID)} for authorization-gated single-job
   * cancellation.
   */
  @Override
  @Transactional
  public int cancelJobsByTag(String tag) {
    int count = jobBatchStatusStore.cancelJobsByTag(tag);
    if (count > 0) {
      publishBulkCancelledEvent(tag, count);
    }
    return count;
  }

  /**
   * {@inheritDoc}
   *
   * <p><strong>Authorization note:</strong> this bulk operation is not subject to per-job {@link
   * JobAuthorizationPolicy} checks. Use {@link #cancelJob(UUID)} for authorization-gated single-job
   * cancellation.
   */
  @Override
  @Transactional
  public int cancelRecurringJobsByTag(String tag) {
    if (recurringJobStore == null) {
      // No RecurringJobStore capability: there are no recurring jobs to cancel.
      return 0;
    }
    int count = recurringJobStore.cancelRecurringJobsByTag(tag);
    if (count > 0) {
      publishBulkCancelledEvent(tag, count);
    }
    return count;
  }

  /**
   * {@inheritDoc}
   *
   * <p><strong>Authorization note:</strong> this bulk operation is not subject to per-job {@link
   * JobAuthorizationPolicy} checks. Use {@link #cancelJob(UUID)} for authorization-gated single-job
   * cancellation.
   */
  @Override
  @Transactional
  public int cancelRecurringJobByBusinessKey(String businessKey) {
    if (recurringJobStore == null) {
      // No RecurringJobStore capability: there are no recurring jobs to cancel.
      return 0;
    }
    return recurringJobStore.cancelRecurringJobByBusinessKey(businessKey) ? 1 : 0;
  }

  @Transactional
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    if (recurringJobStore == null) {
      // No RecurringJobStore capability: no annotation-driven recurring masters were ever created.
      return 0;
    }
    return recurringJobStore.cancelOrphanedRecurringAnnotationJobs(registeredIds, nodeStartTime);
  }

  /**
   * Publishes a single {@link JobsBulkCancelledEvent} after the surrounding transaction commits.
   * Bulk-cancel methods produce one event per call; per-job {@link JobCancelledEvent}s are not
   * fired for jobs cancelled by these methods.
   */
  private void publishBulkCancelledEvent(String tag, int count) {
    JobsBulkCancelledEvent event = new JobsBulkCancelledEvent(tag, count, effective().instant());
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  /**
   * Publishes a {@link JobCancelledEvent} for a successful cancellation CAS. Uses the pre-CAS
   * entity snapshot to populate businessKey / jobType / priority / nodeId on the event so
   * downstream observers (audit logs, monitoring) get the same shape for each cancellable source
   * state.
   */
  private void publishCancelledEvent(UUID jobId, JobStatus previousStatus, JobEntity job) {
    Instant timestamp = effective().instant();
    JobCancelledEvent event =
        job == null
            // Race: job was deleted between CAS and our lookup. Fire a minimal event so observers
            // at least know the cancellation happened.
            ? new JobCancelledEvent(
                jobId, null, null, null, null, timestamp, previousStatus.name(), null)
            : new JobCancelledEvent(
                jobId,
                job.getBusinessKey(),
                job.getPublicJobType(),
                job.getPriority(),
                job.getPickedBy(),
                timestamp,
                previousStatus.name(),
                null);
    // Defer publication until after the surrounding TX commits so a rollback does not produce a
    // spurious CANCELLED event. Falls back to immediate publication when no TX is active.
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  private void publishCancelledEventForRecurring(UUID jobId, RecurringJobDefinition def) {
    JobStatus previousStatus = def.paused() ? JobStatus.PAUSED : JobStatus.PENDING;
    JobCancelledEvent event =
        new JobCancelledEvent(
            jobId,
            def.businessKey(),
            run.ratchet.api.JobType.RECURRING,
            JobPriorityMapper.fromPersistedCode(def.priority()),
            null,
            effective().instant(),
            previousStatus.name(),
            null);
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  private void publishRetryingEventAndWakeup(UUID jobId, JobEntity job) {
    Instant retryAt = effective().instant();
    if (eventPublisher != null) {
      JobRetryingEvent event =
          job == null
              ? new JobRetryingEvent(jobId, null, null, null, null, retryAt, null, 1, retryAt)
              : new JobRetryingEvent(
                  jobId,
                  job.getBusinessKey(),
                  job.getPublicJobType(),
                  job.getPriority(),
                  job.getPickedBy(),
                  retryAt,
                  job.getLastError(),
                  1,
                  retryAt);
      if (registerAfterCommit(() -> eventPublisher.publish(event))
          == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
        eventPublisher.publish(event);
      }
    }

    if (wakeupService == null) {
      return;
    }
    if (job == null) {
      wakeupService.notify(JobPriority.NORMAL, true, null);
    } else {
      wakeupService.notifyIfNeeded(
          job.getJobType(), job.getPriority(), Duration.ZERO, job.getExecutionTarget());
    }
  }

  private JobStatus cancelForReplacement(UUID jobId, JobEntity existing) {
    if (existing.getJobType() == JobExecutionType.RECURRING) {
      JobStatus previousStatus = existing.getStatus();
      return jobTerminalStore.cancelJob(jobId) ? previousStatus : null;
    }
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.PENDING, JobStatus.CANCELED, null)) {
      return JobStatus.PENDING;
    }
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)) {
      return JobStatus.RUNNING;
    }
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.PAUSED, JobStatus.CANCELED, null)) {
      return JobStatus.PAUSED;
    }
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.WAITING, JobStatus.CANCELED, null)) {
      return JobStatus.WAITING;
    }
    return null;
  }

  private void publishPausedEvent(UUID jobId, JobEntity job) {
    JobPausedEvent event =
        new JobPausedEvent(
            jobId,
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            effective().instant());
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  private void publishResumedEvent(UUID jobId, JobEntity job) {
    JobResumedEvent event =
        new JobResumedEvent(
            jobId,
            job.getBusinessKey(),
            job.getPublicJobType(),
            job.getPriority(),
            job.getPickedBy(),
            effective().instant());
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  private void publishPausedEventForRecurring(UUID jobId, RecurringJobDefinition def) {
    JobPausedEvent event =
        new JobPausedEvent(
            jobId,
            def.businessKey(),
            run.ratchet.api.JobType.RECURRING,
            JobPriorityMapper.fromPersistedCode(def.priority()),
            null,
            effective().instant());
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  private void publishResumedEventForRecurring(UUID jobId, RecurringJobDefinition def) {
    JobResumedEvent event =
        new JobResumedEvent(
            jobId,
            def.businessKey(),
            run.ratchet.api.JobType.RECURRING,
            JobPriorityMapper.fromPersistedCode(def.priority()),
            null,
            effective().instant());
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  private TransactionSynchronizationRegistry resolveTxRegistry() {
    TransactionSynchronizationRegistry reg = txRegistry;
    if (reg == null) {
      synchronized (this) {
        reg = txRegistry;
        if (reg == null) {
          try {
            reg = InitialContext.doLookup("java:comp/TransactionSynchronizationRegistry");
            txRegistry = reg;
          } catch (NamingException e) {
            // No component context on this thread (e.g. CDI startup observers on Payara admin
            // thread)
            log.debugf(
                "TransactionSynchronizationRegistry lookup unavailable on this thread; using immediate"
                    + " fallback publication: %s",
                e.getMessage());
          }
        }
      }
    }
    return reg;
  }

  private AfterCommitRegistrationResult registerAfterCommit(Runnable action) {
    return JobWakeupService.registerAfterCommit(
        resolveTxRegistry(),
        action,
        log,
        "After-commit event registration failed; event suppressed: %s");
  }

  private int deliverSignalRaw(UUID jobId, Serializable payload) {
    if (signalStore == null) {
      log.warn("deliverSignal called but no SignalStore is wired — returning 0");
      return 0;
    }
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    String principal =
        callerPrincipalProvider != null
            ? callerPrincipalProvider.currentPrincipal().orElse(null)
            : null;
    if (authorizationPolicy != null) {
      String ownerPrincipal = job != null ? job.getCallerPrincipal() : null;
      authorizationPolicy.checkDeliverSignal(jobId, ownerPrincipal, principal);
    }
    String serializedPayload = serializeSignalPayload(payload, signalKeyOf(job), signalActive(job));
    Instant now = effective().instant();
    String deliveryId = UUID.randomUUID().toString();

    String deliveredBy = principal != null ? principal : DEFAULT_SIGNAL_DELIVERED_BY;
    int unblocked =
        signalStore.deliverSignalById(
            jobId,
            serializedPayload,
            SIGNAL_PAYLOAD_TYPE_RAW,
            SignalDecision.Outcome.APPROVED.name(),
            null,
            deliveredBy,
            now,
            deliveryId);
    if (unblocked > 0) {
      publishSignaledEvent(jobId, job, deliveredBy, now, SignalDecision.Outcome.APPROVED, null);
      log.debugf("Signal delivered to job %s by %s", jobId, deliveredBy);
    }
    return unblocked;
  }

  private int deliverSignalDecision(UUID jobId, SignalDecision decision) {
    if (signalStore == null) {
      log.warn("deliverSignal called but no SignalStore is wired — returning 0");
      return 0;
    }
    JobEntity job = jobCrudStore.findById(jobId).orElse(null);
    String principal =
        callerPrincipalProvider != null
            ? callerPrincipalProvider.currentPrincipal().orElse(null)
            : null;
    if (authorizationPolicy != null) {
      String ownerPrincipal = job != null ? job.getCallerPrincipal() : null;
      authorizationPolicy.checkDeliverSignal(jobId, ownerPrincipal, principal);
    }
    String serializedPayload =
        serializeSignalPayload(decision.payload(), signalKeyOf(job), signalActive(job));
    Instant now = effective().instant();
    String deliveryId = UUID.randomUUID().toString();

    String deliveredBy = principal != null ? principal : DEFAULT_SIGNAL_DELIVERED_BY;
    int unblocked =
        signalStore.deliverSignalById(
            jobId,
            serializedPayload,
            SIGNAL_PAYLOAD_TYPE_DECISION,
            decision.outcome().name(),
            decision.rejectionReason(),
            deliveredBy,
            now,
            deliveryId);
    if (unblocked > 0) {
      publishSignaledEvent(
          jobId, job, deliveredBy, now, decision.outcome(), decision.rejectionReason());
      log.debugf("Signal decision delivered to job %s by %s", jobId, deliveredBy);
    }
    return unblocked;
  }

  private int deliverSignalRaw(String signalKey, Serializable payload) {
    if (signalStore == null) {
      log.warn("deliverSignal called but no SignalStore is wired — returning 0");
      return 0;
    }
    String principal =
        callerPrincipalProvider != null
            ? callerPrincipalProvider.currentPrincipal().orElse(null)
            : null;
    if (authorizationPolicy != null) {
      authorizationPolicy.checkDeliverSignal(signalKey, principal);
    }
    String deliveredBy = principal != null ? principal : DEFAULT_SIGNAL_DELIVERED_BY;
    // Broadcast: one ciphertext lands on every matching row, and the AAD binds the signal key (not
    // a job id), so a single ciphertext decrypts on all of them. Per-job opt-in can't be expressed
    // per row here, so encrypt whenever an engine is configured — that keeps an opted-in waiting
    // job from receiving a plaintext signal payload when the global switch is off.
    String serializedPayload = serializeSignalPayload(payload, signalKey, broadcastSignalActive());
    Instant now = effective().instant();
    String deliveryId = UUID.randomUUID().toString();

    int unblocked =
        signalStore.deliverSignalByKey(
            signalKey,
            serializedPayload,
            SIGNAL_PAYLOAD_TYPE_RAW,
            SignalDecision.Outcome.APPROVED.name(),
            null,
            deliveredBy,
            now,
            deliveryId);
    if (unblocked > 0) {
      publishBulkSignaledEvent(
          signalKey, unblocked, deliveredBy, now, SignalDecision.Outcome.APPROVED, null);
      log.debugf("Signal '%s' broadcast to %s job(s) by %s", signalKey, unblocked, deliveredBy);
    }
    return unblocked;
  }

  private int deliverSignalDecision(String signalKey, SignalDecision decision) {
    if (signalStore == null) {
      log.warn("deliverSignal called but no SignalStore is wired — returning 0");
      return 0;
    }
    String principal =
        callerPrincipalProvider != null
            ? callerPrincipalProvider.currentPrincipal().orElse(null)
            : null;
    if (authorizationPolicy != null) {
      authorizationPolicy.checkDeliverSignal(signalKey, principal);
    }
    String deliveredBy = principal != null ? principal : DEFAULT_SIGNAL_DELIVERED_BY;
    String serializedPayload =
        serializeSignalPayload(decision.payload(), signalKey, broadcastSignalActive());
    Instant now = effective().instant();
    String deliveryId = UUID.randomUUID().toString();

    int unblocked =
        signalStore.deliverSignalByKey(
            signalKey,
            serializedPayload,
            SIGNAL_PAYLOAD_TYPE_DECISION,
            decision.outcome().name(),
            decision.rejectionReason(),
            deliveredBy,
            now,
            deliveryId);
    if (unblocked > 0) {
      publishBulkSignaledEvent(
          signalKey, unblocked, deliveredBy, now, decision.outcome(), decision.rejectionReason());
      log.debugf(
          "Signal decision '%s' broadcast to %s job(s) by %s", signalKey, unblocked, deliveredBy);
    }
    return unblocked;
  }

  private void publishBulkSignaledEvent(
      String signalKey,
      int count,
      String principal,
      Instant timestamp,
      SignalDecision.Outcome outcome,
      String rejectionReason) {
    JobsBulkSignaledEvent event =
        new JobsBulkSignaledEvent(signalKey, count, principal, outcome, rejectionReason, timestamp);
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  private void publishSignaledEvent(
      UUID jobId,
      JobEntity job,
      String principal,
      Instant timestamp,
      SignalDecision.Outcome outcome,
      String rejectionReason) {
    if (metricsCollector != null && job != null) {
      metricsCollector.signalDelivered(jobId, job.getPublicJobType(), job.getSignalKey(), outcome);
    }
    JobSignaledEvent event =
        new JobSignaledEvent(
            jobId,
            job != null ? job.getBusinessKey() : null,
            job != null ? job.getPublicJobType() : null,
            job != null ? job.getPriority() : null,
            null,
            timestamp,
            job != null ? job.getSignalKey() : null,
            principal,
            outcome,
            rejectionReason);
    if (registerAfterCommit(() -> eventPublisher.publish(event))
        == AfterCommitRegistrationResult.NO_ACTIVE_TRANSACTION) {
      eventPublisher.publish(event);
    }
  }

  private static String signalKeyOf(JobEntity job) {
    return job != null ? job.getSignalKey() : null;
  }

  /** Targeted delivery: encrypt iff the global switch is on or the target job opted in. */
  private static boolean signalActive(JobEntity job) {
    return job != null && EncryptionHolder.encryptionActiveFor(job.isEncryptedPayload());
  }

  /**
   * Broadcast delivery: one ciphertext lands on every waiting row matching the key, and the AAD
   * binds the signal key rather than any job id, so the same ciphertext decrypts on all of them.
   * Per-job opt-in cannot be applied per row, so encrypt whenever an engine is configured. This
   * stops an opted-in waiting job from receiving a plaintext signal payload when the global switch
   * is off; over-encrypting a non-opted row is harmless because reads are marker-driven.
   */
  private static boolean broadcastSignalActive() {
    return EncryptionHolder.isEnabled();
  }

  private String serializeSignalPayload(Serializable payload, String signalKey, boolean active) {
    if (payload == null) {
      return null;
    }
    if (payloadSerializer != null) {
      // signal_payload is a TEXT column, so the engine output is stored as a bare token (no JSON
      // envelope needed). Bound to the signal key, not a job id: a broadcast writes one ciphertext
      // to every waiting row matching the key. No-op when inactive.
      return PayloadEncryptor.encryptValue(
          payloadSerializer.serialize(payload), active, EncryptionTarget.signal(signalKey));
    }
    throw new IllegalStateException(
        "Cannot deliver a non-null signal payload without a PayloadSerializer");
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }
}

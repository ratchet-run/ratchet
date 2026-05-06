package run.ratchet.ri.core;

import run.ratchet.api.BatchBuilder;
import run.ratchet.api.JobBuilder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobOptions;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.RecurringJobBuilder;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.api.SignalDecision;
import run.ratchet.api.StreamingBatchBuilder;
import run.ratchet.api.event.JobCancelledEvent;
import run.ratchet.api.event.JobSignaledEvent;
import run.ratchet.ri.payload.DefaultJobInvocationResolver;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.spi.JobInvocationResolver;
import run.ratchet.spi.MetricsCollector;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobBatchStatusStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobPauseStore;
import run.ratchet.store.spi.JobRetryStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import jakarta.annotation.Resource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/** Core scheduling API implementation. Delegates builder persistence to a CDI-managed service. */
@ApplicationScoped
public class DefaultJobSchedulerService
    implements JobSchedulerService, RecurringAnnotationMaintenanceService {

  private static final Logger log = Logger.getLogger(DefaultJobSchedulerService.class);
  static final String SIGNAL_PAYLOAD_TYPE_DECISION = "DECISION";
  static final String SIGNAL_PAYLOAD_TYPE_RAW = "RAW";

  private final InternalEventPublisher eventPublisher;
  private final JobBatchStatusStore jobBatchStatusStore;
  private final JobPauseStore jobPauseStore;
  private final JobRetryStore jobRetryStore;
  private final JobTerminalStore jobTerminalStore;
  private final JobCrudStore jobCrudStore;
  private final BatchStore batchStore;
  private final TagStore tagStore;
  private final WorkflowConditionStore workflowConditionStore;
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

  @Resource private TransactionSynchronizationRegistry txRegistry;

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

  public DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
      JobWakeupService wakeupService,
      RecurringScheduler recurringScheduler) {
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
        wakeupService,
        recurringScheduler,
        new DefaultJobInvocationResolver(),
        new DefaultJobCreationService(
            jobBatchStatusStore,
            jobTerminalStore,
            jobCrudStore,
            batchStore,
            tagStore,
            workflowConditionStore,
            wakeupService,
            recurringScheduler),
        null,
        null,
        null,
        null,
        null,
        Clock.systemUTC());
  }

  public DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
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

  public DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
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
  public DefaultJobSchedulerService(
      InternalEventPublisher eventPublisher,
      JobBatchStatusStore jobBatchStatusStore,
      JobPauseStore jobPauseStore,
      JobRetryStore jobRetryStore,
      JobTerminalStore jobTerminalStore,
      JobCrudStore jobCrudStore,
      BatchStore batchStore,
      TagStore tagStore,
      WorkflowConditionStore workflowConditionStore,
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
    if (authorizationPolicy != null) {
      // Pre-load to obtain ownerPrincipal. TOCTOU: if the job is deleted between this load
      // and the CAS below, ownerPrincipal will be null — the policy must tolerate null.
      String ownerPrincipal = job != null ? job.getCallerPrincipal() : null;
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

    // Try RUNNING → CANCELED (the executor will publish JobCancelledEvent itself when the
    // running task observes the status flip — see JobTask. We do NOT publish here for the
    // RUNNING path to avoid duplicate events.)
    if (jobBatchStatusStore.compareAndSwapStatus(
        jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)) {
      log.debugf("Canceled running job %s", jobId);
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

    log.debugf("Cannot cancel job %s — already in terminal state or not found", jobId);
    return false;
  }

  /**
   * Publishes a {@link JobCancelledEvent} for a job that was cancelled outside the executor (i.e.,
   * from PENDING or PAUSED state). The RUNNING path publishes its own event from within {@code
   * JobTask} when the running task observes the status flip; we skip publication there to avoid
   * duplicate events. Uses the pre-CAS entity snapshot to populate businessKey / jobType / priority /
   * nodeId on the event so downstream observers (audit logs, monitoring) see the same shape they get
   * for running-cancellations.
   */
  private void publishCancelledEvent(UUID jobId, JobStatus previousStatus, JobEntity job) {
    JobCancelledEvent event =
        job == null
            // Race: job was deleted between CAS and our lookup. Fire a minimal event so observers
            // at least know the cancellation happened.
            ? new JobCancelledEvent(jobId, null, null, null, null, previousStatus.name(), null)
            : new JobCancelledEvent(
                jobId,
                job.getBusinessKey(),
                job.getPublicJobType(),
                job.getPriority(),
                job.getPickedBy(),
                previousStatus.name(),
                null);
    // Defer publication until after the surrounding TX commits so a rollback does not produce a
    // spurious CANCELLED event. Falls back to immediate publication when no TX is active.
    if (!registerAfterCommit(() -> eventPublisher.publish(event))) {
      eventPublisher.publish(event);
    }
  }

  private boolean registerAfterCommit(Runnable action) {
    if (txRegistry == null) {
      return false;
    }
    try {
      if (txRegistry.getTransactionStatus() != Status.STATUS_ACTIVE) {
        return false;
      }
      txRegistry.registerInterposedSynchronization(
          new Synchronization() {
            @Override
            public void beforeCompletion() {}

            @Override
            public void afterCompletion(int status) {
              if (status == Status.STATUS_COMMITTED) {
                action.run();
              }
            }
          });
      return true;
    } catch (Exception e) {
      log.warnf(
          "After-commit event registration failed; publishing immediately: %s", e.getMessage());
      return false;
    }
  }

  @Override
  public void addEventListener(Consumer<Object> listener) {
    eventPublisher.addListener(listener);
  }

  @Override
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

  private int deliverSignalRaw(UUID jobId, Serializable payload) {
    if (signalStore == null) {
      log.warn("deliverSignal called but no SignalStore is wired — returning 0");
      return 0;
    }
    String principal =
        callerPrincipalProvider != null
            ? callerPrincipalProvider.currentPrincipal().orElse(null)
            : null;
    String serializedPayload = serializeSignalPayload(payload);
    Instant now = effective().instant();
    String deliveryId = java.util.UUID.randomUUID().toString();

    int unblocked =
        signalStore.deliverSignalById(
            jobId,
            serializedPayload,
            SIGNAL_PAYLOAD_TYPE_RAW,
            SignalDecision.Outcome.APPROVED.name(),
            null,
            principal,
            now,
            deliveryId);
    if (unblocked > 0) {
      JobEntity job = jobCrudStore.findById(jobId).orElse(null);
      publishSignaledEvent(jobId, job, principal, SignalDecision.Outcome.APPROVED, null);
      log.debugf("Signal delivered to job %s by %s", jobId, principal);
    }
    return unblocked;
  }

  private int deliverSignalDecision(UUID jobId, SignalDecision decision) {
    if (signalStore == null) {
      log.warn("deliverSignal called but no SignalStore is wired — returning 0");
      return 0;
    }
    String principal =
        callerPrincipalProvider != null
            ? callerPrincipalProvider.currentPrincipal().orElse(null)
            : null;
    String serializedPayload = serializeSignalPayload(decision);
    Instant now = effective().instant();
    String deliveryId = java.util.UUID.randomUUID().toString();

    int unblocked =
        signalStore.deliverSignalById(
            jobId,
            serializedPayload,
            SIGNAL_PAYLOAD_TYPE_DECISION,
            decision.outcome().name(),
            decision.rejectionReason(),
            principal,
            now,
            deliveryId);
    if (unblocked > 0) {
      JobEntity job = jobCrudStore.findById(jobId).orElse(null);
      publishSignaledEvent(jobId, job, principal, decision.outcome(), decision.rejectionReason());
      log.debugf("Signal decision delivered to job %s by %s", jobId, principal);
    }
    return unblocked;
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

  private int deliverSignalRaw(String signalKey, Serializable payload) {
    if (signalStore == null) {
      log.warn("deliverSignal called but no SignalStore is wired — returning 0");
      return 0;
    }
    String principal =
        callerPrincipalProvider != null
            ? callerPrincipalProvider.currentPrincipal().orElse(null)
            : null;
    String serializedPayload = serializeSignalPayload(payload);
    Instant now = effective().instant();
    String deliveryId = java.util.UUID.randomUUID().toString();

    int unblocked =
        signalStore.deliverSignalByKey(
            signalKey,
            serializedPayload,
            SIGNAL_PAYLOAD_TYPE_RAW,
            SignalDecision.Outcome.APPROVED.name(),
            null,
            principal,
            now,
            deliveryId);
    if (unblocked > 0) {
      publishBulkSignaledEvents(deliveryId, principal, SignalDecision.Outcome.APPROVED, null);
      log.debugf("Signal '%s' broadcast to %s job(s) by %s", signalKey, unblocked, principal);
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
    String serializedPayload = serializeSignalPayload(decision);
    Instant now = effective().instant();
    String deliveryId = java.util.UUID.randomUUID().toString();

    int unblocked =
        signalStore.deliverSignalByKey(
            signalKey,
            serializedPayload,
            SIGNAL_PAYLOAD_TYPE_DECISION,
            decision.outcome().name(),
            decision.rejectionReason(),
            principal,
            now,
            deliveryId);
    if (unblocked > 0) {
      publishBulkSignaledEvents(
          deliveryId, principal, decision.outcome(), decision.rejectionReason());
      log.debugf(
          "Signal decision '%s' broadcast to %s job(s) by %s", signalKey, unblocked, principal);
    }
    return unblocked;
  }

  private void publishBulkSignaledEvents(
      String deliveryId, String principal, SignalDecision.Outcome outcome, String rejectionReason) {
    for (JobEntity job : signalStore.findJobsBySignalDeliveryId(deliveryId)) {
      publishSignaledEvent(job.getId(), job, principal, outcome, rejectionReason);
    }
  }

  private void publishSignaledEvent(
      UUID jobId,
      JobEntity job,
      String principal,
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
            job != null ? job.getSignalKey() : null,
            principal,
            outcome,
            rejectionReason);
    if (!registerAfterCommit(() -> eventPublisher.publish(event))) {
      eventPublisher.publish(event);
    }
  }

  private String serializeSignalPayload(Serializable payload) {
    if (payload == null) {
      return null;
    }
    if (payloadSerializer != null) {
      return payloadSerializer.serialize(payload);
    }
    throw new IllegalStateException(
        "Cannot deliver a non-null signal payload without a PayloadSerializer");
  }

  private Clock effective() {
    return clock != null ? clock : Clock.systemUTC();
  }

  @Override
  public JobBuilder enqueue(SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(jobCreationService, task, Duration.ZERO);
  }

  @Override
  public JobHandle enqueueNow(SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(jobCreationService, task, Duration.ZERO).immediate().submit();
  }

  @Override
  public JobBuilder schedule(Duration delay, SerializableCheckedRunnable task) {
    return DefaultJobBuilder.create(jobCreationService, task, delay);
  }

  @Override
  public BatchBuilder enqueueBatch(String name) {
    return new DefaultBatchBuilder(name, jobCreationService, jobInvocationResolver);
  }

  @Override
  public <T extends Serializable> StreamingBatchBuilder<T> streamingBatch(String name) {
    return new DefaultStreamingBatchBuilder<>(name, jobCreationService);
  }

  @Override
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

    JobBuilder builder = DefaultJobBuilder.create(jobCreationService, newTask, delay);
    if (opts != null) {
      builder
          .withPriority(opts.priority())
          .withMaxRetries(opts.maxRetries())
          .withBackoff(opts.backoffPolicy(), opts.backoffParam())
          .withTimeout(Duration.ofSeconds(opts.timeoutSec()));
    }
    JobHandle newHandle = builder.submit();

    // Cancel the old job using CAS to avoid conflicting with concurrent executors.
    // Try each cancellable state in order; if all fail, the job is already terminal.
    boolean canceled =
        jobBatchStatusStore.compareAndSwapStatus(jobId, JobStatus.PENDING, JobStatus.CANCELED, null)
            || jobBatchStatusStore.compareAndSwapStatus(
                jobId, JobStatus.RUNNING, JobStatus.CANCELED, null)
            || jobBatchStatusStore.compareAndSwapStatus(
                jobId, JobStatus.PAUSED, JobStatus.CANCELED, null);

    // Reload after CAS: compareAndSwapStatus bumps the optimistic-lock version; writing the
    // pre-CAS entity would throw. Not routed through OptimisticLockRetry because the intent is
    // to annotate a terminal CANCELED row with supersededBy as an audit trail.
    JobEntity fresh =
        jobCrudStore
            .findById(jobId)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Job " + jobId + " vanished mid-replace (deleted between load and save)"));
    fresh.setSupersededBy(newHandle.id());
    jobCrudStore.save(fresh);

    if (canceled) {
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

    // Recurring masters use the rec_status shim (cold-only) — no hot row.
    if (job.getJobType() == JobExecutionType.RECURRING) {
      if (jobPauseStore.pauseRecurring(jobId)) {
        log.debugf("Paused recurring master %s", jobId);
        return true;
      }
      log.debugf("Cannot pause recurring master %s — current rec_status %s", jobId, current);
      return false;
    }

    // Executable jobs: try PENDING → PAUSED on hot.
    if (jobPauseStore.transitionToPaused(jobId, JobStatus.PENDING)) {
      log.debugf("Paused pending job %s", jobId);
      return true;
    }

    // Post hot/cold-split: pause-of-FAILED is no longer supported. FAILED is terminal-only and
    // has no hot row, so paused_from_status has nowhere to live. transitionToPaused returns
    // false for terminal expected statuses.
    log.debugf(
        "Cannot pause job %s — current status %s is not pausable (only PENDING is eligible)",
        jobId, current);
    return false;
  }

  @Override
  @Transactional
  public boolean resumeJob(UUID jobId) {
    // Recurring masters: rec_status 'A' → 'P'.
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
    if (job.getJobType() == JobExecutionType.RECURRING) {
      if (jobPauseStore.resumeRecurring(jobId)) {
        log.debugf("Resumed recurring master %s", jobId);
        recurringScheduler.kick();
        return true;
      }
      log.debugf("Cannot resume recurring master %s — not paused", jobId);
      return false;
    }

    JobStatus target = jobPauseStore.transitionFromPausedAtomic(jobId);
    if (target == null) {
      log.debugf(
          "Cannot resume job %s — not in PAUSED state (current: %s)", jobId, job.getStatus());
      return false;
    }
    log.debugf("Resumed job %s to %s", jobId, target);
    if (target == JobStatus.PENDING) {
      recurringScheduler.kick();
    }
    return true;
  }

  @Override
  @Transactional
  public boolean retryJob(UUID jobId) {
    if (authorizationPolicy != null) {
      // Pre-load to obtain ownerPrincipal. TOCTOU: if the job is deleted between this load
      // and the CAS below, ownerPrincipal will be null — the policy must tolerate null.
      JobEntity job = jobCrudStore.findById(jobId).orElse(null);
      String ownerPrincipal = job != null ? job.getCallerPrincipal() : null;
      String currentPrincipal =
          callerPrincipalProvider != null
              ? callerPrincipalProvider.currentPrincipal().orElse(null)
              : null;
      authorizationPolicy.checkRetry(jobId, ownerPrincipal, currentPrincipal);
    }

    if (jobRetryStore.resetFailedToPending(jobId)) {
      log.debugf("Retried failed job %s — reset to PENDING", jobId);
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
  public int cancelRecurringJobsByTag(String tag) {
    return jobBatchStatusStore.cancelRecurringJobsByTag(tag);
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
    return jobBatchStatusStore.cancelRecurringJobByBusinessKey(businessKey);
  }

  @Transactional
  public int cancelOrphanedRecurringAnnotationJobs(
      Set<String> registeredIds, Instant nodeStartTime) {
    return jobBatchStatusStore.cancelOrphanedRecurringAnnotationJobs(registeredIds, nodeStartTime);
  }
}

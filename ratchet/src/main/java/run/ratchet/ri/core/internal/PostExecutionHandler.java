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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.transaction.Transactional.TxType;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.Nullable;
import run.ratchet.api.event.AbstractJobSchedulerEvent;
import run.ratchet.api.event.JobCompletedEvent;
import run.ratchet.api.event.JobFailedEvent;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.ri.core.BatchCompletionTransaction;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.CompletionReads;
import run.ratchet.ri.core.JobTransaction;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.TransactionalCompletion;
import run.ratchet.ri.core.internal.DurableTerminalEvents.PreparedDlqEvents;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.store.dto.BatchProgress;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.JobTerminalStore;
import run.ratchet.store.spi.TransactionalCompletionStore;

/**
 * Routes post-execution lifecycle events (batch progress, workflow scheduling, DLQ) on behalf of
 * {@link JobTask}.
 *
 * <h2>Transaction semantics</h2>
 *
 * <p>The completion entry points {@code completeSuccess}, {@code completeSuccessMinimal}, {@code
 * completeFailure}, {@code moveToDlqAndHandlePermanentFailure}, and {@code handleTimeoutTransition}
 * run outside a transaction. They open one independent {@link TxType#REQUIRES_NEW} transaction
 * through {@link BatchCompletionTransaction}, then run post-commit steps and batch follow-ups on
 * the calling thread after that transaction returns. A durable store commit is reported as
 * committed even if the completion transaction fails afterwards. {@code
 * completeSuccessInCurrentTransaction} is SQL-only and retains its caller's transaction boundary.
 * {@code completeTimeoutFailure} uses {@link TxType#REQUIRED} to join the timeout wrapper's
 * transaction. Other public methods retain the class-level {@code REQUIRES_NEW} boundary. Composite
 * handlers call local helpers directly so their bookkeeping shares one transaction.
 *
 * <p>{@code rollbackOn = Exception.class} covers the {@code ExecutionException} wrapping that
 * {@code ManagedExecutorService} applies to failures from submitted {@code Callable}s. The default
 * rule rolls back only on {@code RuntimeException} and {@code Error}, which would allow a checked
 * exception from a job body to commit the post-execution transaction — the opposite of what we
 * want.
 *
 * <p>If a joined SQL completion transaction fails, its write rolls back and the exception
 * propagates. A store that committed independently cannot be rolled back by that transaction.
 *
 * @see JobTask
 * @see ExecutionObserver
 */
@ApplicationScoped
@Transactional(value = TxType.REQUIRES_NEW, rollbackOn = Exception.class)
public class PostExecutionHandler {

  /**
   * Result of a timeout transition that won its terminal-state compare-and-swap.
   *
   * <p>The ordered events are published before {@code JobDlqEvent} by {@link DeadLetterService} in
   * one after-commit callback on this handler's transaction.
   */
  public record TerminalTimeoutTransition(
      JobEntity job, List<AbstractJobSchedulerEvent> eventsBeforeDlq) {

    public TerminalTimeoutTransition {
      Objects.requireNonNull(job, "job must not be null");
      eventsBeforeDlq = List.copyOf(eventsBeforeDlq);
    }
  }

  private static final int COMMIT_CONFIRMATION_ATTEMPTS = 3;
  private static final long COMMIT_CONFIRMATION_BACKOFF_MS = 25L;

  private static final ThreadLocal<CompletionContext> COMPLETION_CONTEXT = new ThreadLocal<>();

  private static final class CompletionContext {
    private final boolean deferSteps;
    private final List<Runnable> steps = new ArrayList<>();
    private final List<BatchProgress> batchFollowups = new ArrayList<>();
    private UUID jobId;
    private JobCompletionPlan pendingWrite;
    private Runnable pendingNotifications;
    private boolean durable;
    private boolean result;

    private CompletionContext(boolean deferSteps) {
      this.deferSteps = deferSteps;
    }
  }

  /** Keeps publication inside caller-owned transactions and defers independent completion steps. */
  static void deferPostCommit(Runnable step) {
    CompletionContext context = COMPLETION_CONTEXT.get();
    if (context != null && context.deferSteps) context.steps.add(step);
    else step.run();
  }

  private final BatchService batchService;
  private final JobTerminalStore jobTerminalStore;
  private final WorkflowScheduler workflowScheduler;
  private final DurableTerminalEvents durableEvents;

  private final DeadLetterService deadLetterService;
  private final PollerScheduler pollerScheduler;
  private final AfterCommitRegistrar afterCommitRegistrar;
  private final BatchCompletionTransaction completionTransaction;

  protected PostExecutionHandler() {
    this.durableEvents = null;
    this.batchService = null;
    this.jobTerminalStore = null;
    this.workflowScheduler = null;
    this.deadLetterService = null;
    this.pollerScheduler = null;
    this.afterCommitRegistrar = null;
    this.completionTransaction = null;
  }

  public PostExecutionHandler(
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler) {
    this(batchService, workflowScheduler, deadLetterService, pollerScheduler, null);
  }

  /**
   * Test constructor. Without a {@link BatchCompletionTransaction}, completion work runs with no
   * transaction boundary, so containers must use the {@code @Inject} constructor.
   */
  public PostExecutionHandler(
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler,
      JobTerminalStore jobTerminalStore) {
    this(
        batchService,
        workflowScheduler,
        deadLetterService,
        pollerScheduler,
        jobTerminalStore,
        new JakartaAfterCommitRegistrar(),
        null);
  }

  public PostExecutionHandler(
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler,
      JobTerminalStore jobTerminalStore,
      AfterCommitRegistrar afterCommitRegistrar,
      BatchCompletionTransaction completionTransaction) {
    this(
        batchService,
        workflowScheduler,
        deadLetterService,
        pollerScheduler,
        jobTerminalStore,
        afterCommitRegistrar,
        completionTransaction,
        null);
  }

  @Inject
  public PostExecutionHandler(
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler,
      JobTerminalStore jobTerminalStore,
      AfterCommitRegistrar afterCommitRegistrar,
      BatchCompletionTransaction completionTransaction,
      DurableTerminalEvents durableEvents) {
    this.jobTerminalStore = jobTerminalStore;
    this.batchService = batchService;
    this.workflowScheduler = workflowScheduler;
    this.deadLetterService = deadLetterService;
    this.pollerScheduler = pollerScheduler;
    this.afterCommitRegistrar = afterCommitRegistrar;
    this.completionTransaction = completionTransaction;
    this.durableEvents = durableEvents;
  }

  @Transactional(value = TxType.NOT_SUPPORTED)
  public boolean completeSuccess(
      JobEntity source,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      long durationMs,
      long queueWaitMs,
      @Nullable Integer expectedAttempts) {
    return inCompletionTransaction(
        () -> {
          JobEntity completed =
              successSnapshot(source, resultJson, resultType, start, end, durationMs, queueWaitMs);
          return complete(completed, JobStatus.RUNNING, false, null, List.of(), expectedAttempts);
        });
  }

  @Transactional(value = TxType.MANDATORY, rollbackOn = Exception.class)
  public TransactionalCompletion completeSuccessInCurrentTransaction(
      JobEntity source,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      long durationMs,
      long queueWaitMs,
      @Nullable Integer expectedAttempts) {
    if (!JobTransaction.active()) {
      throw new IllegalStateException("Job completion requires an active JobTransaction carrier");
    }
    TransactionalCompletionStore store =
        jobTerminalStore instanceof JobStore jobs
            ? jobs.capability(TransactionalCompletionStore.class).orElse(null)
            : null;
    if (store == null) {
      throw new IllegalStateException("Job store does not support transactional completion");
    }
    store.requireJoinedTransaction();
    CompletionContext previous = COMPLETION_CONTEXT.get();
    CompletionContext context = new CompletionContext(false);
    COMPLETION_CONTEXT.set(context);
    try {
      JobEntity completed =
          successSnapshot(source, resultJson, resultType, start, end, durationMs, queueWaitMs);
      if (!complete(
          completed,
          JobStatus.RUNNING,
          false,
          null,
          List.of(),
          CompletionReads.transactional(store),
          expectedAttempts)) {
        throw new CompletionRejectedException();
      }
      return new TransactionalCompletion(context.batchFollowups);
    } finally {
      if (previous == null) COMPLETION_CONTEXT.remove();
      else COMPLETION_CONTEXT.set(previous);
    }
  }

  @Transactional(TxType.NOT_SUPPORTED)
  public void runFollowups(TransactionalCompletion completion) {
    completion.followups().forEach(this::runBatchFollowup);
  }

  private JobEntity successSnapshot(
      JobEntity source,
      String resultJson,
      String resultType,
      Instant start,
      Instant end,
      long durationMs,
      long queueWaitMs) {
    JobEntity completed = completionSnapshot(source, JobStatus.SUCCEEDED);
    completed.setJobResult(resultJson);
    completed.setResultType(resultType);
    completed.setExecutionStartTime(start);
    completed.setExecutionEndTime(end);
    completed.setExecutionDurationMs(durationMs);
    completed.setQueueWaitMs(queueWaitMs);
    completed.setLastError(null);
    return completed;
  }

  @Transactional(value = TxType.NOT_SUPPORTED)
  public boolean completeSuccessMinimal(
      JobEntity source,
      Instant start,
      Instant end,
      long durationMs,
      long queueWaitMs,
      @Nullable Integer expectedAttempts) {
    return completeSuccess(
        source, null, null, start, end, durationMs, queueWaitMs, expectedAttempts);
  }

  @Transactional(value = TxType.NOT_SUPPORTED)
  public boolean completeFailure(
      JobEntity source,
      JobStatus expectedStatus,
      boolean cancelChain,
      @Nullable Integer expectedAttempts) {
    return inCompletionTransaction(
        () -> completeFailureBody(source, expectedStatus, cancelChain, expectedAttempts));
  }

  private boolean completeFailureBody(
      JobEntity source,
      JobStatus expectedStatus,
      boolean cancelChain,
      @Nullable Integer expectedAttempts) {
    JobEntity completed = completionSnapshot(source, JobStatus.FAILED);
    String error = completed.getLastError() != null ? completed.getLastError() : "Job failed";
    completed.setLastError(error);
    return complete(
        completed,
        expectedStatus,
        cancelChain,
        null,
        List.of(
            new JobFailedEvent(
                completed.getId(),
                completed.getBusinessKey(),
                completed.getRecurringMasterId(),
                completed.getPublicJobType(),
                completed.getPriority(),
                completed.getPickedBy(),
                error,
                completed.getAttempts())),
        expectedAttempts);
  }

  @Transactional(value = TxType.REQUIRED, rollbackOn = Exception.class)
  public boolean completeTimeoutFailure(
      JobEntity source,
      JobStatus expectedStatus,
      boolean cancelChain,
      Throwable cause,
      List<AbstractJobSchedulerEvent> eventsBeforeDlq) {
    return complete(
        completionSnapshot(source, JobStatus.FAILED),
        expectedStatus,
        cancelChain,
        cause,
        eventsBeforeDlq,
        null);
  }

  private boolean complete(
      JobEntity completed,
      JobStatus expectedStatus,
      boolean cancelChain,
      Throwable cause,
      List<AbstractJobSchedulerEvent> failureEvents,
      @Nullable Integer expectedAttempts) {
    return complete(
        completed, expectedStatus, cancelChain, cause, failureEvents, null, expectedAttempts);
  }

  private boolean complete(
      JobEntity completed,
      JobStatus expectedStatus,
      boolean cancelChain,
      Throwable cause,
      List<AbstractJobSchedulerEvent> failureEvents,
      CompletionReads reads,
      @Nullable Integer expectedAttempts) {
    WorkflowCompletionPlan workflow =
        completed.getJobType() == JobExecutionType.BATCH_CHILD
            ? WorkflowCompletionPlan.empty()
            : reads == null
                ? workflowScheduler.planCompletion(completed, cancelChain)
                : workflowScheduler.planCompletion(completed, cancelChain, null, reads);
    DurableTerminalEvents durable = durableEvents;
    boolean durableEnabled = durable != null && durable.enabled();
    List<AbstractJobSchedulerEvent> events = new ArrayList<>();
    PreparedDlqEvents preparedDlq = null;
    if (durableEnabled) {
      if (completed.getStatus() == JobStatus.SUCCEEDED) {
        events.add(
            new JobCompletedEvent(
                completed.getId(),
                completed.getBusinessKey(),
                completed.getRecurringMasterId(),
                completed.getPublicJobType(),
                completed.getPriority(),
                completed.getPickedBy(),
                completed.getExecutionEndTime(),
                completed.getExecutionDurationMs()));
      } else {
        events.addAll(failureEvents);
        preparedDlq =
            durable.prepareDlqEvents(events, deadLetterService.prepareDlqEvent(completed, cause));
        events = preparedDlq.events();
      }
      if (preparedDlq == null) events = durable.prepare(events);
    }
    JobCompletionPlan plan =
        completionPlan(completed, expectedStatus, null, workflow, expectedAttempts);
    if (durableEnabled) plan = durable.withEvents(plan, events);
    List<AbstractJobSchedulerEvent> outboxPublication = durableEnabled ? events : null;
    PreparedDlqEvents plannedDlq = preparedDlq;
    CompletionContext context = COMPLETION_CONTEXT.get();
    if (context != null
        && context.deferSteps
        && plan.expectedClaimSeq() != null
        && plan.terminalStatus() != JobStatus.CANCELED) {
      plan = plan.withCommitToken(UUID.randomUUID());
      // Record the identity before the store call, which can itself lose its commit reply.
      context.pendingWrite = plan;
      context.jobId = plan.jobId();
      context.pendingNotifications =
          () -> {
            registerCompletionNotifications(
                completed, cause, failureEvents, outboxPublication, plannedDlq, true);
            deferPostCommit(() -> workflowScheduler.publishCompletion(workflow));
            deferPostCommit(() -> wakeupIfNewWorkAvailable(workflow.newWorkAvailable()));
          };
    }
    JobCompletionResult result = jobTerminalStore.commitCompletion(plan);
    if (!result.committed()) {
      if (context != null) context.pendingWrite = null;
      if (expectedStatus == JobStatus.RUNNING && completed.getClaimSeq() != null)
        Logger.getLogger(PostExecutionHandler.class)
            .warnf(
                "Rejected owner write for job %s, stale claimSeq %s, node %s",
                completed.getId(), completed.getClaimSeq(), completed.getPickedBy());
      return false;
    }
    if (context != null) {
      context.jobId = completed.getId();
      context.durable |= result.durable();
      context.result = true;
      context.pendingNotifications = null;
    }
    registerCompletionNotifications(
        completed, cause, failureEvents, outboxPublication, plannedDlq, false);
    deferPostCommit(() -> workflowScheduler.publishCompletion(workflow));
    if (result.batchProgress() != null) {
      // Child terminal state and counters are already one durable unit. Parent completion is
      // independently recoverable if this following step fails (notably on Mongo).
      // The committed connection stays enlisted through afterCompletion callbacks on JTA servers
      // and Spring. REQUIRES_NEW there needs a second connection, so a burst can exhaust the pool.
      // Collect follow-ups until the completion transaction has returned and released its
      // connection.
      if (context != null) {
        context.batchFollowups.add(result.batchProgress());
      } else {
        Runnable followup =
            ExecutorClassLoader.preserveContext(() -> runBatchFollowup(result.batchProgress()));
        if (afterCommitRegistrar.registerAfterCommit(followup)
            == AfterCommitRegistrar.Result.NO_ACTIVE_TRANSACTION) {
          followup.run();
        }
      }
    }
    deferPostCommit(() -> wakeupIfNewWorkAvailable(workflow.newWorkAvailable()));
    return true;
  }

  private void registerCompletionNotifications(
      JobEntity completed,
      Throwable cause,
      List<AbstractJobSchedulerEvent> failureEvents,
      @Nullable List<AbstractJobSchedulerEvent> outboxPublication,
      @Nullable PreparedDlqEvents plannedDlq,
      boolean recovered) {
    if (completed.getStatus() == JobStatus.SUCCEEDED) {
      if (outboxPublication != null) {
        deferPostCommit(() -> durableEvents.publishAfterCommit(outboxPublication));
      } else {
        deferPostCommit(
            () ->
                workflowScheduler.publishTerminalEvent(
                    new JobCompletedEvent(
                        completed.getId(),
                        completed.getBusinessKey(),
                        completed.getRecurringMasterId(),
                        completed.getPublicJobType(),
                        completed.getPriority(),
                        completed.getPickedBy(),
                        completed.getExecutionEndTime(),
                        completed.getExecutionDurationMs())));
      }
    } else if (recovered) {
      deferPostCommit(
          () -> {
            var dlq =
                plannedDlq != null
                    ? plannedDlq
                    : new PreparedDlqEvents(
                        failureEvents, deadLetterService.prepareDlqEvent(completed, cause));
            deadLetterService.recordPreparedDlqTransitionAfterCommit(completed, dlq);
          });
    } else if (plannedDlq != null) {
      deadLetterService.recordDlqTransitionInCurrentTransaction(completed, plannedDlq);
    } else {
      deadLetterService.recordDlqTransitionInCurrentTransaction(completed, cause, failureEvents);
    }
  }

  /**
   * Runs post-commit steps after the completion transaction returns, then batch follow-ups. A
   * durable store commit stays committed if the completion transaction fails afterwards.
   * Caller-owned SQL completion does not use this boundary.
   */
  private boolean inCompletionTransaction(Supplier<Boolean> work) {
    CompletionContext previous = COMPLETION_CONTEXT.get();
    CompletionContext context = new CompletionContext(true);
    COMPLETION_CONTEXT.set(context);
    Supplier<Boolean> recordedWork =
        () -> {
          context.result = work.get();
          return context.result;
        };
    try {
      context.result =
          completionTransaction != null
              ? completionTransaction.complete(recordedWork)
              : recordedWork.get();
    } catch (RuntimeException failure) {
      if (!context.durable && !resolveCommit(context.pendingWrite, failure)) throw failure;
      if (context.pendingNotifications != null) context.pendingNotifications.run();
      context.result = true;
      Logger.getLogger(PostExecutionHandler.class)
          .warnf(
              failure,
              "Job %s: the store committed; the completion transaction failed afterwards",
              context.jobId);
    } finally {
      if (previous == null) COMPLETION_CONTEXT.remove();
      else COMPLETION_CONTEXT.set(previous);
    }
    for (Runnable step : context.steps) {
      try {
        step.run();
      } catch (RuntimeException failure) {
        Logger.getLogger(PostExecutionHandler.class)
            .warnf(
                failure,
                "Job %s completion committed but a post-commit step failed",
                context.jobId);
      }
    }
    for (BatchProgress progress : context.batchFollowups) {
      runBatchFollowup(progress, context.jobId);
    }
    return context.result;
  }

  private boolean resolveCommit(@Nullable JobCompletionPlan plan, RuntimeException writeFailure) {
    if (plan == null) return false;
    boolean interrupted = Thread.interrupted();
    try {
      // A failed read carries no outcome. Keep the original token and pending notifications
      // through a short retry window before returning the write to its caller.
      for (int attempt = 0; attempt < COMMIT_CONFIRMATION_ATTEMPTS; attempt++) {
        try {
          return jobTerminalStore.wasCompletionCommitted(
              plan.jobId(), plan.terminalStatus(), plan.expectedClaimSeq(), plan.commitToken());
        } catch (RuntimeException readFailure) {
          if (readFailure != writeFailure) writeFailure.addSuppressed(readFailure);
        }
        if (attempt + 1 == COMMIT_CONFIRMATION_ATTEMPTS) break;
        try {
          Thread.sleep(COMMIT_CONFIRMATION_BACKOFF_MS << attempt);
        } catch (InterruptedException interruptedDuringRetry) {
          interrupted = true;
          writeFailure.addSuppressed(interruptedDuringRetry);
          break;
        }
      }
      String message =
          "Unresolved terminal commit for job %s, claim %s, status %s, token %s"
              .formatted(
                  plan.jobId(), plan.expectedClaimSeq(), plan.terminalStatus(), plan.commitToken());
      var unresolved = new RatchetTransientStoreException(message, writeFailure);
      // Record the uncertainty even when the caller subsequently retries the fenced transition.
      Logger.getLogger(PostExecutionHandler.class).error(message, unresolved);
      throw unresolved;
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  private void runBatchFollowup(BatchProgress progress) {
    runBatchFollowup(progress, progress.batchId());
  }

  private void runBatchFollowup(BatchProgress progress, UUID completedJobId) {
    Logger log = Logger.getLogger(PostExecutionHandler.class);
    try {
      wakeupIfNewWorkAvailable(batchService.afterChildCompletion(progress));
    } catch (RuntimeException failure) {
      log.warnf(
          failure,
          "Job %s batch %s completion deferred to recovery",
          completedJobId,
          progress.batchId());
    }
  }

  public static JobCompletionPlan completionPlan(
      JobEntity completed,
      JobStatus expectedStatus,
      JobCompletionPlan.BatchCompletion batchCompletion,
      WorkflowCompletionPlan workflow,
      @Nullable Integer expectedAttempts) {
    Long expectedClaimSeq = expectedStatus == JobStatus.RUNNING ? completed.getClaimSeq() : null;
    return new JobCompletionPlan(
        completed.getId(),
        expectedStatus,
        completed.getStatus(),
        completed.getJobResult(),
        completed.getResultType(),
        completed.getLastError(),
        completed.getAttempts(),
        completed.getExecutionStartTime(),
        completed.getExecutionEndTime(),
        completed.getExecutionDurationMs(),
        completed.getQueueWaitMs(),
        completed.getJobType() == JobExecutionType.BATCH_CHILD ? completed.getDependsOn() : null,
        batchCompletion,
        workflow.dependencies(),
        expectedClaimSeq,
        expectedAttempts,
        List.of());
  }

  static JobEntity completionSnapshot(JobEntity source, JobStatus status) {
    JobEntity snapshot = new JobEntity();
    snapshot.setId(source.getId());
    snapshot.setClaimSeq(source.getClaimSeq());
    snapshot.setStatus(status);
    snapshot.setJobType(source.getJobType());
    snapshot.setPriority(source.getPriority());
    snapshot.setDependsOn(source.getDependsOn());
    snapshot.setBusinessKey(source.getBusinessKey());
    snapshot.setRecurringMasterId(source.getRecurringMasterId());
    snapshot.setPickedBy(source.getPickedBy());
    snapshot.setAttempts(source.getAttempts());
    snapshot.setLastError(source.getLastError());
    snapshot.setExecutionStartTime(source.getExecutionStartTime());
    snapshot.setExecutionEndTime(source.getExecutionEndTime());
    snapshot.setExecutionDurationMs(source.getExecutionDurationMs());
    snapshot.setQueueWaitMs(source.getQueueWaitMs());
    return snapshot;
  }

  /**
   * Atomically moves a still-RUNNING job to the DLQ and applies its batch/workflow failure
   * bookkeeping through the store's atomic completion operation. SQL stores join the helper's
   * {@code REQUIRES_NEW} transaction; Mongo commits its own session transaction. Ordered events are
   * registered only after the store reports a successful transition.
   *
   * @return {@code true} when this call won the terminal transition, or {@code false} when the job
   *     had already left RUNNING
   */
  @Transactional(value = TxType.NOT_SUPPORTED)
  public boolean moveToDlqAndHandlePermanentFailure(
      JobEntity job, Throwable ex, @Nullable Integer expectedAttempts) {
    return inCompletionTransaction(
        () -> {
          requireFailureRoutingMetadata(job);
          job.setLastError(deadLetterService.sanitizeForCompletion(ex));
          if (!completeFailureBody(job, JobStatus.RUNNING, false, expectedAttempts)) {
            return false;
          }
          job.setStatus(JobStatus.FAILED);
          return true;
        });
  }

  /**
   * Runs a timeout-owned state transition and its terminal lifecycle work in one independent
   * transaction.
   *
   * <p>The callback must perform the complete timeout transition, including the retry-attempt
   * update and construction of its ordered terminal events. The callback invokes {@link
   * #completeTimeoutFailure}, which commits terminal bookkeeping and registers timeout, failure,
   * DLQ, and dependent events in order. Returning a terminal transition means that operation won
   * the terminal-state compare-and-swap. An empty result means the job was retried or a competing
   * path already changed it; this wrapper never repeats terminal bookkeeping.
   *
   * @param ex timeout failure used for DLQ routing
   * @param cancelChainOnFailure whether a terminal signal timeout must cancel its chain
   * @param transition complete timeout state transition and event-registration callback
   * @return {@code true} when the callback applied the terminal transition
   */
  @Transactional(value = TxType.NOT_SUPPORTED)
  public boolean handleTimeoutTransition(
      Throwable ex,
      boolean cancelChainOnFailure,
      Supplier<Optional<TerminalTimeoutTransition>> transition) {
    return inCompletionTransaction(
        () -> {
          Optional<TerminalTimeoutTransition> terminalTransition = transition.get();
          if (terminalTransition.isEmpty()) {
            return false;
          }

          TerminalTimeoutTransition outcome = terminalTransition.orElseThrow();
          JobEntity job = outcome.job();
          requireFailureRoutingMetadata(job);
          // The callback's atomic completion already registered timeout/failure/DLQ events and
          // downstream events in order, in this transaction.
          return true;
        });
  }

  private static void requireFailureRoutingMetadata(JobEntity job) {
    Objects.requireNonNull(job, "job must not be null");
    Objects.requireNonNull(job.getJobType(), "job type must not be null");
  }

  private void wakeupIfNewWorkAvailable(boolean newWorkAvailable) {
    if (newWorkAvailable) {
      pollerScheduler.wakeup();
    }
  }
}

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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.api.event.AbstractJobSchedulerEvent;
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.store.dto.JobCompletionPlan;
import run.ratchet.store.dto.JobCompletionResult;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobTerminalStore;

/**
 * Routes post-execution lifecycle events (batch progress, workflow scheduling, DLQ) on behalf of
 * {@link JobTask}.
 *
 * <h2>Transaction semantics</h2>
 *
 * <p>External calls to public methods run in their own transaction ({@link TxType#REQUIRES_NEW})
 * with rollback on any exception ({@code rollbackOn = Exception.class}). Composite handlers such as
 * {@link #handlePermanentFailure(JobEntity, Throwable)} call local helper methods directly, so
 * those helper calls intentionally share the composite handler's transaction instead of re-entering
 * the CDI proxy.
 *
 * <p>A {@code ManagedExecutorService} task does not inherit the submitting transaction. Jakarta
 * Concurrency 3.0 §3.1.8 requires the task to run outside that transaction and suspends any
 * transaction already associated with the executing thread. The {@code REQUIRES_NEW} attribute is
 * still load bearing: it gives each composite lifecycle operation one explicit, independent
 * transaction. It also protects transactional non-executor callers and recovery paths by suspending
 * their caller transaction, so the lifecycle transition, event registration, and downstream
 * bookkeeping commit or roll back together instead of following the caller's outcome.
 *
 * <p>{@code rollbackOn = Exception.class} covers the {@code ExecutionException} wrapping that
 * {@code ManagedExecutorService} applies to failures from submitted {@code Callable}s. The default
 * rule rolls back only on {@code RuntimeException} and {@code Error}, which would allow a checked
 * exception from a job body to commit the post-execution transaction — the opposite of what we
 * want.
 *
 * <p><b>Failure-recovery note:</b> if the {@code REQUIRES_NEW} commit itself fails (DB blip during
 * the completion ack), the job is left in {@code RUNNING}. Orphan reset on startup / periodic
 * stale-RUNNING detection is the recovery path.
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

  private final BatchService batchService;
  private final JobTerminalStore jobTerminalStore;
  private final WorkflowScheduler workflowScheduler;
  private final DeadLetterService deadLetterService;
  private final PollerScheduler pollerScheduler;
  private AfterCommitRegistrar afterCommitRegistrar = new JakartaAfterCommitRegistrar();

  protected PostExecutionHandler() {
    this.batchService = null;
    this.jobTerminalStore = null;
    this.workflowScheduler = null;
    this.deadLetterService = null;
    this.pollerScheduler = null;
  }

  public PostExecutionHandler(
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler) {
    this(batchService, workflowScheduler, deadLetterService, pollerScheduler, null);
  }

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
        new JakartaAfterCommitRegistrar());
  }

  @Inject
  public PostExecutionHandler(
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler,
      JobTerminalStore jobTerminalStore,
      AfterCommitRegistrar afterCommitRegistrar) {
    this.jobTerminalStore = jobTerminalStore;
    this.batchService = batchService;
    this.workflowScheduler = workflowScheduler;
    this.deadLetterService = deadLetterService;
    this.pollerScheduler = pollerScheduler;
    this.afterCommitRegistrar = afterCommitRegistrar;
  }

  public boolean completeSuccess(
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
    return complete(completed, JobStatus.RUNNING, false, null, List.of());
  }

  public boolean completeSuccessMinimal(
      JobEntity source, Instant start, Instant end, long durationMs, long queueWaitMs) {
    return completeSuccess(source, null, null, start, end, durationMs, queueWaitMs);
  }

  public boolean completeFailure(JobEntity source, JobStatus expectedStatus, boolean cancelChain) {
    JobEntity completed = completionSnapshot(source, JobStatus.FAILED);
    String error = completed.getLastError() != null ? completed.getLastError() : "Job failed";
    completed.setLastError(error);
    return complete(
        completed,
        expectedStatus,
        cancelChain,
        null,
        List.of(
            new run.ratchet.api.event.JobFailedEvent(
                completed.getId(),
                completed.getBusinessKey(),
                completed.getRecurringMasterId(),
                completed.getPublicJobType(),
                completed.getPriority(),
                completed.getPickedBy(),
                error,
                completed.getAttempts())));
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
        eventsBeforeDlq);
  }

  private boolean complete(
      JobEntity completed,
      JobStatus expectedStatus,
      boolean cancelChain,
      Throwable cause,
      List<AbstractJobSchedulerEvent> failureEvents) {
    WorkflowCompletionPlan workflow =
        completed.getJobType() == JobExecutionType.BATCH_CHILD
            ? WorkflowCompletionPlan.empty()
            : workflowScheduler.planCompletion(completed, cancelChain);
    JobCompletionResult result =
        jobTerminalStore.commitCompletion(
            completionPlan(completed, expectedStatus, null, workflow));
    if (!result.committed()) {
      return false;
    }
    if (completed.getStatus() == JobStatus.SUCCEEDED) {
      workflowScheduler.publishTerminalEvent(
          new run.ratchet.api.event.JobCompletedEvent(
              completed.getId(),
              completed.getBusinessKey(),
              completed.getRecurringMasterId(),
              completed.getPublicJobType(),
              completed.getPriority(),
              completed.getPickedBy(),
              completed.getExecutionEndTime(),
              completed.getExecutionDurationMs()));
    } else {
      deadLetterService.recordDlqTransitionInCurrentTransaction(completed, cause, failureEvents);
    }
    workflowScheduler.publishCompletion(workflow);
    wakeupIfNewWorkAvailable(workflow.newWorkAvailable());
    if (result.batchProgress() != null) {
      // Child terminal state and counters are already one durable unit. Parent completion is
      // independently recoverable if this following step fails (notably on Mongo).
      Logger log = Logger.getLogger(PostExecutionHandler.class);
      Runnable followup =
          () -> {
            try {
              wakeupIfNewWorkAvailable(batchService.afterChildCompletion(result.batchProgress()));
            } catch (RuntimeException failure) {
              log.warnf(
                  failure,
                  "Batch %s completion deferred to recovery",
                  result.batchProgress().batchId());
            }
          };
      if (afterCommitRegistrar.registerAfterCommit(followup)
          == AfterCommitRegistrar.Result.NO_ACTIVE_TRANSACTION) {
        followup.run();
      }
    }
    return true;
  }

  public static JobCompletionPlan completionPlan(
      JobEntity completed,
      JobStatus expectedStatus,
      JobCompletionPlan.BatchCompletion batchCompletion,
      WorkflowCompletionPlan workflow) {
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
        workflow.dependencies());
  }

  private static JobEntity completionSnapshot(JobEntity source, JobStatus status) {
    JobEntity snapshot = new JobEntity();
    snapshot.setId(source.getId());
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

  public boolean markBatchChildFailed(JobEntity job) {
    return batchService.markChildFailed(job);
  }

  public boolean markBatchChildSucceeded(JobEntity job) {
    return batchService.markChildSucceeded(job);
  }

  public void cancelChain(JobEntity job) {
    workflowScheduler.cancelChain(job);
  }

  public boolean scheduleNext(JobEntity job) {
    return workflowScheduler.scheduleNext(job);
  }

  public void moveToDlq(JobEntity job, Throwable ex) {
    deadLetterService.recordDlqTransition(job, ex);
  }

  /**
   * Atomically moves a still-RUNNING job to the DLQ and applies its batch/workflow failure
   * bookkeeping through the store's atomic completion operation. SQL stores join this method's
   * {@code REQUIRES_NEW} transaction; Mongo commits its own session transaction. Ordered events are
   * registered only after the store reports a successful transition.
   *
   * @return {@code true} when this call won the terminal transition, or {@code false} when the job
   *     had already left RUNNING
   */
  public boolean moveToDlqAndHandlePermanentFailure(JobEntity job, Throwable ex) {
    requireFailureRoutingMetadata(job);
    job.setLastError(deadLetterService.sanitizeForCompletion(ex));
    if (!completeFailure(job, JobStatus.RUNNING, false)) {
      return false;
    }
    job.setStatus(JobStatus.FAILED);
    return true;
  }

  public void handleJobSuccess(JobEntity job) {
    boolean newWorkAvailable =
        switch (job.getJobType()) {
          case BATCH_CHILD -> markBatchChildSucceeded(job);
          case SINGLE, CHAIN_STEP, WORKFLOW_BRANCH -> scheduleNext(job);
          default -> false;
        };
    wakeupIfNewWorkAvailable(newWorkAvailable);
  }

  public void handlePermanentFailure(JobEntity job, Throwable ex) {
    requireFailureRoutingMetadata(job);
    moveToDlq(job, ex);
    wakeupIfNewWorkAvailable(applyPermanentFailureBookkeeping(job));
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
  public boolean handleTimeoutTransition(
      Throwable ex,
      boolean cancelChainOnFailure,
      Supplier<Optional<TerminalTimeoutTransition>> transition) {
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
  }

  private boolean applyPermanentFailureBookkeeping(JobEntity job) {
    return switch (job.getJobType()) {
      case BATCH_CHILD -> markBatchChildFailed(job);
      case SINGLE, CHAIN_STEP, WORKFLOW_BRANCH -> scheduleNext(job);
      default -> false;
    };
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

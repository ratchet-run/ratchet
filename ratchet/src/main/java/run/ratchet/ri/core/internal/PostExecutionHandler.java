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
import run.ratchet.ri.core.BatchService;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.store.entity.JobEntity;

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
 * <p>The {@code REQUIRES_NEW} attribute is load bearing: {@link JobTask} is submitted to a {@code
 * ManagedExecutorService} which, per Jakarta Concurrency 3.0 §3.4.4, propagates JTA context from
 * the submitting thread by default. Without {@code REQUIRES_NEW} the post-execution commit would
 * join that inherited transaction, and any rollback of the outer transaction would silently discard
 * the job-completion ack — the job would be re-executed after it had already succeeded.
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

  private final BatchService batchService;
  private final WorkflowScheduler workflowScheduler;
  private final DeadLetterService deadLetterService;
  private final PollerScheduler pollerScheduler;

  protected PostExecutionHandler() {
    this.batchService = null;
    this.workflowScheduler = null;
    this.deadLetterService = null;
    this.pollerScheduler = null;
  }

  @Inject
  public PostExecutionHandler(
      BatchService batchService,
      WorkflowScheduler workflowScheduler,
      DeadLetterService deadLetterService,
      PollerScheduler pollerScheduler) {
    this.batchService = batchService;
    this.workflowScheduler = workflowScheduler;
    this.deadLetterService = deadLetterService;
    this.pollerScheduler = pollerScheduler;
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
    deadLetterService.moveToDlq(job, ex);
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
    boolean newWorkAvailable =
        switch (job.getJobType()) {
          case BATCH_CHILD -> markBatchChildFailed(job);
          case SINGLE, CHAIN_STEP, WORKFLOW_BRANCH -> {
            moveToDlq(job, ex);
            yield scheduleNext(job);
          }
          default -> {
            moveToDlq(job, ex);
            yield false;
          }
        };
    wakeupIfNewWorkAvailable(newWorkAvailable);
  }

  private void wakeupIfNewWorkAvailable(boolean newWorkAvailable) {
    if (newWorkAvailable) {
      pollerScheduler.wakeup();
    }
  }
}

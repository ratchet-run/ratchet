package run.ratchet.ri.core;

import run.ratchet.store.entity.JobEntity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

/**
 * Routes post-execution lifecycle events (batch progress, workflow scheduling, DLQ) on behalf of
 * {@link JobTask}.
 *
 * @see JobTask
 * @see ExecutionObserver
 */
@ApplicationScoped
@Transactional
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

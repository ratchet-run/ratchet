package run.ratchet.ri.core;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;

@ExtendWith(MockitoExtension.class)
class PostExecutionHandlerTest {

  @Mock private BatchService batchService;
  @Mock private WorkflowScheduler workflowScheduler;
  @Mock private DeadLetterService deadLetterService;
  @Mock private PollerScheduler pollerScheduler;

  private PostExecutionHandler handler;

  private static JobEntity job(JobExecutionType jobType) {
    JobEntity job = new JobEntity();
    job.setJobType(jobType);
    return job;
  }

  @BeforeEach
  void setUp() {
    handler =
        new PostExecutionHandler(
            batchService, workflowScheduler, deadLetterService, pollerScheduler);
  }

  @Test
  void handleJobSuccess_withoutDownstreamWork_doesNotWakePoller() {
    JobEntity job = job(JobExecutionType.SINGLE);
    when(workflowScheduler.scheduleNext(job)).thenReturn(false);

    handler.handleJobSuccess(job);

    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handleJobSuccess_withDownstreamWork_wakesPoller() {
    JobEntity job = job(JobExecutionType.CHAIN_STEP);
    when(workflowScheduler.scheduleNext(job)).thenReturn(true);

    handler.handleJobSuccess(job);

    verify(pollerScheduler).wakeup();
  }

  @Test
  void handleBatchChildSuccess_withoutCompletedBatch_doesNotWakePoller() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    when(batchService.markChildSucceeded(job)).thenReturn(false);

    handler.handleJobSuccess(job);

    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handlePermanentFailure_batchChildWithoutCompletedBatch_doesNotWakePollerOrDlq() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    RuntimeException failure = new RuntimeException("boom");
    when(batchService.markChildFailed(job)).thenReturn(false);

    handler.handlePermanentFailure(job, failure);

    verify(batchService).markChildFailed(job);
    verify(deadLetterService, never()).moveToDlq(job, failure);
    verify(workflowScheduler, never()).scheduleNext(job);
    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handlePermanentFailure_batchChildCompletesBatch_wakesPoller() {
    JobEntity job = job(JobExecutionType.BATCH_CHILD);
    RuntimeException failure = new RuntimeException("boom");
    when(batchService.markChildFailed(job)).thenReturn(true);

    handler.handlePermanentFailure(job, failure);

    verify(batchService).markChildFailed(job);
    verify(deadLetterService, never()).moveToDlq(job, failure);
    verify(pollerScheduler).wakeup();
  }

  @Test
  void handlePermanentFailure_singleMovesToDlqAndWithoutDownstreamWorkDoesNotWakePoller() {
    JobEntity job = job(JobExecutionType.SINGLE);
    RuntimeException failure = new RuntimeException("boom");
    when(workflowScheduler.scheduleNext(job)).thenReturn(false);

    handler.handlePermanentFailure(job, failure);

    verify(deadLetterService).moveToDlq(job, failure);
    verify(workflowScheduler).scheduleNext(job);
    verify(pollerScheduler, never()).wakeup();
  }

  @Test
  void handlePermanentFailure_chainStepMovesToDlqAndWithDownstreamWorkWakesPoller() {
    JobEntity job = job(JobExecutionType.CHAIN_STEP);
    RuntimeException failure = new RuntimeException("boom");
    when(workflowScheduler.scheduleNext(job)).thenReturn(true);

    handler.handlePermanentFailure(job, failure);

    verify(deadLetterService).moveToDlq(job, failure);
    verify(workflowScheduler).scheduleNext(job);
    verify(pollerScheduler).wakeup();
  }

  @Test
  void handlePermanentFailure_recurringMovesToDlqWithoutSchedulingNext() {
    JobEntity job = job(JobExecutionType.RECURRING);
    RuntimeException failure = new RuntimeException("boom");

    handler.handlePermanentFailure(job, failure);

    verify(deadLetterService).moveToDlq(job, failure);
    verify(workflowScheduler, never()).scheduleNext(job);
    verify(pollerScheduler, never()).wakeup();
  }
}

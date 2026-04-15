package run.ratchet.ri.core;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostExecutionHandlerTest {

  @Mock private BatchService batchService;
  @Mock private WorkflowScheduler workflowScheduler;
  @Mock private DeadLetterService deadLetterService;
  @Mock private PollerScheduler pollerScheduler;

  private PostExecutionHandler handler;

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

  private static JobEntity job(JobExecutionType jobType) {
    JobEntity job = new JobEntity();
    job.setJobType(jobType);
    return job;
  }
}

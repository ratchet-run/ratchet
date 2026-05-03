package run.ratchet.testsuite.dlq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.event.JobDlqEvent;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailOnceJob;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.observer.EventCapture;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Validates Dead Letter Queue behavior when jobs exhaust all retry attempts. */
class DeadLetterQueueIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private EventCapture eventCapture;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(FailingJob.class, FailOnceJob.class, TestJobService.class, EventCapture.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetState() {
    FailingJob.resetCount();
    FailOnceJob.reset();
    eventCapture.clear();
  }

  @Test
  void failingJob_afterExhaustingRetries_shouldMoveToDlqAndFireEvent() throws InterruptedException {
    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .withMaxRetries(2)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
            .submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    boolean received = eventCapture.awaitEvent(JobDlqEvent.class, Duration.ofSeconds(10));
    assertTrue(received, "Should have received JobDlqEvent after retries exhausted");

    var dlqEvents = eventCapture.getEvents(JobDlqEvent.class);
    assertFalse(dlqEvents.isEmpty(), "Should have at least one DLQ event");

    JobDlqEvent dlqEvent = dlqEvents.get(0);
    assertNotNull(dlqEvent.getErrorMessage(), "DLQ event should have an error message");
    assertTrue(dlqEvent.getRetryAttempt() >= 1, "DLQ event retry attempt should be >= 1");

    assertTrue(
        FailingJob.getAttemptCount() >= 3,
        "Job should have been attempted at least 3 times (1 initial + 2 retries)");

    var job = jobCrudStore.findById(handle.id());
    assertTrue(job.isPresent(), "Job should still exist in store");
    assertNotNull(job.get().getLastError(), "Job should have lastError populated");
  }

  @Test
  void dlqJob_afterManualRetry_shouldReExecuteAndSucceed() {
    JobHandle handle = jobService.enqueue(FailOnceJob::execute).withMaxRetries(0).submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    var failedJob = jobCrudStore.findById(handle.id());
    assertTrue(failedJob.isPresent(), "Failed job should exist");
    assertEquals(JobStatus.FAILED, failedJob.get().getStatus(), "Job should be FAILED");
    assertNotNull(failedJob.get().getLastError(), "Job should have lastError");

    boolean retried = jobService.retryJob(handle.id());
    assertTrue(retried, "retryJob should return true for a FAILED job");

    // Job should re-execute and succeed (FailOnceJob only fails once)
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    // Verify the job was cleaned up
    var succeededJob = jobCrudStore.findById(handle.id());
    assertTrue(succeededJob.isPresent(), "Job should still exist after retry");
    assertEquals(JobStatus.SUCCEEDED, succeededJob.get().getStatus(), "Job should be SUCCEEDED");
    assertNull(
        succeededJob.get().getLastError(), "lastError should be cleared after successful retry");
    assertEquals(
        0, succeededJob.get().getAttempts(), "Attempts should be reset before re-execution");
  }

  @Test
  void failedJob_pauseShouldBeRejected() {
    // FAILED is terminal-only (no hot row), so paused_from_status has nowhere to live.
    // Pause-of-FAILED is not supported; the job remains FAILED and pauseJob returns false.
    JobHandle handle = jobService.enqueue(FailOnceJob::execute).withMaxRetries(0).submit();

    JobAssertions.assertJobFailed(jobCrudStore, handle);
    assertFalse(
        jobService.pauseJob(handle.id()), "pauseJob must reject FAILED jobs in terminal state");
    JobAssertions.assertJobStatus(jobCrudStore, handle, JobStatus.FAILED);
  }
}

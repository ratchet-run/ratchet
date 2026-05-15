package run.ratchet.testsuite.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.event.JobRetryingEvent;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.observer.EventCapture;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates retry behavior: failure → retry with backoff, configurable retry count. */
class JobRetryIT extends BaseRatchetIT {

  // The gap assertion measures scheduledTime − event.timestamp. Emission-lag between scheduling
  // the retry and publishing the event eats into the gap, so the backoff must be large enough
  // that a slow CI runner (GC pause, DB write latency) can't push the gap below the floor. 100ms
  // was too tight — observed −42ms gap on a CI runner, i.e. ~142ms of emission lag. 500ms with a
  // 150ms tolerance gives ~350ms of headroom while still catching real backoff regressions.
  private static final Duration FIXED_BACKOFF = Duration.ofMillis(500);
  private static final Duration BACKOFF_TOLERANCE = Duration.ofMillis(150);

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private EventCapture eventCapture;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(FailingJob.class, TestJobService.class, EventCapture.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJobs() {
    FailingJob.resetCount();
    eventCapture.clear();
  }

  @Test
  void failingJob_withRetries_shouldRetryAndFail() {
    JobHandle handle =
        jobService
            .enqueue(FailingJob::execute)
            .withMaxRetries(2)
            .withBackoff(BackoffPolicy.FIXED, FIXED_BACKOFF)
            .submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    // Should have been attempted 3 times (1 initial + 2 retries)
    assertEquals(3, FailingJob.getAttemptCount());

    List<JobRetryingEvent> retryEvents = eventCapture.getEvents(JobRetryingEvent.class);
    assertEquals(2, retryEvents.size(), "Expected one retry event per configured retry");
    retryEvents.forEach(
        event -> {
          Duration gap = Duration.between(event.getTimestamp(), event.getScheduledTime());
          // Floor: at least FIXED_BACKOFF minus tolerance.
          assertTrue(
              gap.compareTo(FIXED_BACKOFF.minus(BACKOFF_TOLERANCE)) >= 0,
              "Retry scheduled sooner than the configured backoff: gap=" + gap);
          // Ceiling: no more than FIXED_BACKOFF plus tolerance. Catches regressions that schedule
          // retries arbitrarily far in the future (e.g., misconfigured backoff multipliers).
          assertTrue(
              gap.compareTo(FIXED_BACKOFF.plus(BACKOFF_TOLERANCE)) <= 0,
              "Retry scheduled later than the configured backoff: gap=" + gap);
        });
  }

  @Test
  void failingJob_withNoRetries_shouldFailImmediately() {
    JobHandle handle = jobService.enqueue(FailingJob::execute).submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);
  }
}

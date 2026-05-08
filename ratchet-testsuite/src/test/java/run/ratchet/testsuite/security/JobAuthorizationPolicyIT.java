package run.ratchet.testsuite.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.FailOnceJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.StubJobAuthorizationPolicy;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Verifies that the {@link JobAuthorizationPolicy} SPI is wired into the CDI graph and invoked at
 * the correct job lifecycle points.
 *
 * <p>Uses {@link StubJobAuthorizationPolicy} ({@code @Alternative @Priority(1)}) to count
 * invocations without blocking execution, proving the framework calls through to the policy at
 * creation and at execution time.
 */
class JobAuthorizationPolicyIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;
  @Inject private JobCrudStore jobCrudStore;
  @Inject private JobQueryService jobQueryService;
  @Inject private JobAuthorizationPolicy authorizationPolicy;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            StubJobAuthorizationPolicy.class,
            FailOnceJob.class,
            SimpleJob.class,
            TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    StubJobAuthorizationPolicy.resetAll();
    FailOnceJob.reset();
    SimpleJob.resetCount();
  }

  @Test
  void authorizationPolicy_isInjectedAsCorrectAlternativeType() {
    assertNotNull(authorizationPolicy, "JobAuthorizationPolicy must be injectable as a CDI bean");
    assertInstanceOf(
        StubJobAuthorizationPolicy.class,
        authorizationPolicy,
        "Injected policy must be the @Alternative StubJobAuthorizationPolicy");
  }

  @Test
  void checkCreate_isCalledOncePerJobSubmission() {
    assertEquals(
        0, StubJobAuthorizationPolicy.getCreateCount(), "Pre-condition: no create calls yet");

    jobService.enqueueNow(SimpleJob::execute);

    assertEquals(
        1,
        StubJobAuthorizationPolicy.getCreateCount(),
        "checkCreate must be called exactly once for a single job submission");
  }

  @Test
  void checkExecute_isCalledOnceDuringJobExecution() {
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    assertEquals(
        1,
        StubJobAuthorizationPolicy.getExecuteCount(),
        "checkExecute must be called exactly once when the job runs");
  }

  @Test
  void checkCreate_thenCheckExecute_eachCalledOnce_forSingleJob() {
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    assertEquals(
        1, StubJobAuthorizationPolicy.getCreateCount(), "checkCreate called once at submission");
    assertEquals(
        1, StubJobAuthorizationPolicy.getExecuteCount(), "checkExecute called once at execution");
  }

  @Test
  void checkCancel_isCalledWhenJobIsCancelled() {
    JobHandle handle = jobService.enqueue(SimpleJob::execute).withMaxRetries(0).submit();

    jobService.cancelJob(handle.id());

    assertEquals(
        1,
        StubJobAuthorizationPolicy.getCancelCount(),
        "checkCancel must be called when cancelJob is invoked");
  }

  @Test
  void checkPause_isCalledWhenJobIsPaused() {
    JobHandle handle = jobService.schedule(Duration.ofMinutes(5), SimpleJob::execute).submit();

    assertTrue(jobService.pauseJob(handle.id()), "pauseJob should pause a pending job");

    assertEquals(
        1,
        StubJobAuthorizationPolicy.getPauseCount(),
        "checkPause must be called when pauseJob is invoked");
  }

  @Test
  void checkResume_isCalledWhenJobIsResumed() {
    JobHandle handle = jobService.schedule(Duration.ofMinutes(5), SimpleJob::execute).submit();
    assertTrue(jobService.pauseJob(handle.id()), "pauseJob should prepare a paused job");
    StubJobAuthorizationPolicy.resetAll();

    assertTrue(jobService.resumeJob(handle.id()), "resumeJob should resume a paused job");

    assertEquals(
        1,
        StubJobAuthorizationPolicy.getResumeCount(),
        "checkResume must be called when resumeJob is invoked");
  }

  @Test
  void checkRetry_isCalledWhenRetryIsRequested() {
    assertEquals(
        0, StubJobAuthorizationPolicy.getRetryCount(), "Pre-condition: no retry calls yet");

    jobService.retryJob(UUID.randomUUID());

    assertEquals(
        1,
        StubJobAuthorizationPolicy.getRetryCount(),
        "checkRetry must be called before retryJob evaluates job state");
  }

  @Test
  void checkRead_isCalledForSingleJobDetail() {
    JobHandle handle = jobService.schedule(Duration.ofMinutes(5), SimpleJob::execute).submit();

    assertTrue(jobQueryService.getJobDetail(handle.id()).isPresent());

    assertEquals(
        1, StubJobAuthorizationPolicy.getReadCount(), "checkRead must be called for getJobDetail");
  }

  @Test
  void filterForPrincipal_isCalledForJobListQueries() {
    jobService.schedule(Duration.ofMinutes(5), SimpleJob::execute).submit();

    jobQueryService.findJobs(JobFilter.builder().build(), 10, 0);

    assertEquals(
        1,
        StubJobAuthorizationPolicy.getFilterCount(),
        "filterForPrincipal must be called for findJobs");
  }

  @Test
  void checkCreate_isCalledForEachJobInBatch() {
    int batchSize = 3;
    jobService
        .enqueueBatch("test-batch")
        .forEach(List.of("a", "b", "c"), item -> SimpleJob.execute())
        .submit();

    // Batch parent + 3 children = 4 checkCreate calls
    assertTrue(
        StubJobAuthorizationPolicy.getCreateCount() >= batchSize,
        "checkCreate must be called for the batch parent and each child job");
  }
}

package run.ratchet.testsuite.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.spi.JobAuthorizationPolicy;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.StubJobAuthorizationPolicy;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
  @Inject private JobAuthorizationPolicy authorizationPolicy;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(StubJobAuthorizationPolicy.class, SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCounts() {
    StubJobAuthorizationPolicy.resetAll();
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

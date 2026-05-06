package run.ratchet.testsuite.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.MdcCapturingJob;
import run.ratchet.testsuite.app.StubCallerPrincipalProvider;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Verifies that JobMdcContext populates the stable MDC keys during job execution. */
class LoggingMdcIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(MdcCapturingJob.class, StubCallerPrincipalProvider.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCapture() {
    MdcCapturingJob.reset();
  }

  @Test
  void mdcKeysArePopulatedDuringJobExecution() {
    JobHandle handle = jobService.enqueueNow(MdcCapturingJob::execute);

    assertNotNull(handle);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    Map<String, Object> captured = MdcCapturingJob.getCapturedMdc();
    assertNotNull(captured, "MdcCapturingJob did not run — no MDC snapshot captured");

    // jobId is always populated.
    Object jobIdValue = captured.get("jobId");
    assertNotNull(jobIdValue, "jobId MDC key missing during job execution. Captured: " + captured);
    assertEquals(
        String.valueOf(handle.id()),
        String.valueOf(jobIdValue),
        "jobId MDC value should match the submitted job's ID");

    // node is populated from NodeIdentityProvider; we don't pin a specific value but it must be
    // non-null.
    assertTrue(
        captured.containsKey("node") && captured.get("node") != null,
        "node MDC key missing or null during job execution. Captured: " + captured);

    // jobType is populated from JobEntity.getPublicJobType().name(); single jobs = "SINGLE".
    assertEquals(
        "SINGLE",
        String.valueOf(captured.get("jobType")),
        "jobType MDC key should be SINGLE for a directly-enqueued job. Captured: " + captured);

    // jobCreator is populated from CallerPrincipalProvider (callerPrincipal) at enqueue time.
    // StubCallerPrincipalProvider is in this deployment and returns "it-caller".
    assertEquals(
        StubCallerPrincipalProvider.STUB_PRINCIPAL,
        String.valueOf(captured.get("jobCreator")),
        "jobCreator MDC key should match the CallerPrincipalProvider value. Captured: " + captured);
  }
}

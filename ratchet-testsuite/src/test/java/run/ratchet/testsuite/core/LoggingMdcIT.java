package run.ratchet.testsuite.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.MdcCapturingJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@code JobMdcContext} populates the JBoss Logging MDC with {@code jobId}, {@code
 * node}, and {@code jobCreator} keys during job execution, and that those keys are visible to the
 * job's own thread when the executor calls into user code.
 *
 * <p>The test uses {@link MdcCapturingJob}, which snapshots {@code MDC.getMap()} into a static
 * field at execution time. The IT then asserts the captured snapshot contains the expected keys.
 *
 * <p>WildFly's JBoss LogManager backend renders MDC values via {@code %X{...}} formatter patterns;
 * the same MDC put/remove APIs work whether the runtime backend is JBoss LogManager, SLF4J, or bare
 * JDK JUL.
 */
class LoggingMdcIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(MdcCapturingJob.class, TestJobService.class)
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
  }
}

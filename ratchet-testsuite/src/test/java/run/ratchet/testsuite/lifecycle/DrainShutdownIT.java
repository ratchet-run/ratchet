package run.ratchet.testsuite.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.ri.core.DrainController;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Validates that drain mode correctly blocks and resumes job claims in a real WildFly deployment.
 * See the Ratchet public-readiness plan, fix 2.
 *
 * <p>Two tests exercise the drain mechanism end-to-end:
 *
 * <ol>
 *   <li>Draining blocks new claims — with drain engaged, an immediately-ready job must remain
 *       PENDING instead of being claimed by the poller.
 *   <li>Disabling drain resumes claims — after {@code setDraining(false)}, a new job must be picked
 *       up and completed normally.
 * </ol>
 *
 * <p>The <i>ordering</i> guarantee (drain is engaged BEFORE poller.stop() during shutdown) is
 * verified separately by {@code RatchetLifecycleShutdownTest} in the ratchet unit test suite
 * using Mockito InOrder, which avoids CDI proxy complications that prevent reflective invocation of
 * package-private {@code @PreDestroy} methods in Arquillian deployments.
 */
class DrainShutdownIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;
  @Inject private JobCrudStore jobCrudStore;
  @Inject private DrainController drainController;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJobCounter() {
    SimpleJob.resetCount();
  }

  @Test
  void drainModeBlocksNewJobClaims() {
    // Engage drain mode BEFORE enqueueing. Poller.pollOnce() short-circuits on isDraining()
    // and never claims.
    drainController.setDraining(true);
    try {
      JobHandle handle = jobService.enqueue(SimpleJob::execute).immediate().submit();
      assertNotNull(handle);

      // Give the poller several cycles to run. With drain engaged, the job must stay PENDING.
      // arquillian.xml caps POLLER_MAX_DELAY_MS=2000 and POLLER_DEEP_IDLE_DELAY_MS=1000, so a
      // 6-second wait covers at least 3 full worst-case poll cycles — well past the point where
      // a functioning drain check should have either blocked every cycle or let one through.
      // If this test ever starts flaking, raise this wait rather than lowering it; the goal is
      // high-confidence evidence that the poller was actively running and refused to claim.
      try {
        Thread.sleep(6000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      JobStatus status = jobCrudStore.getJobStatus(handle.id());
      assertEquals(
          JobStatus.PENDING,
          status,
          "Job must remain PENDING while drain mode is active (poller must not claim)");
      assertEquals(
          0,
          SimpleJob.getInvocationCount(),
          "SimpleJob.execute() must not have run — drain mode blocks claims");
    } finally {
      // Always restore drain mode so other tests (or subsequent @BeforeEach) see normal state.
      drainController.setDraining(false);
    }
  }

  @Test
  void disablingDrainModeResumesJobClaims() {
    // Drain should be off (either never engaged, or reset by the other test's finally block).
    assertFalse(drainController.isDraining(), "Drain should be off at start of this test");

    JobHandle handle = jobService.enqueue(SimpleJob::execute).immediate().submit();
    assertNotNull(handle);

    // Poller should pick it up and execute it normally. assertJobCompleted already blocks via
    // Awaitility until the row is SUCCEEDED, which happens only after SimpleJob.execute() has
    // run and incremented the counter — so a separate counter wait would be redundant.
    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertTrue(
        SimpleJob.getInvocationCount() >= 1,
        "SimpleJob.execute() must have run by the time the job is SUCCEEDED");
  }
}

package run.ratchet.testsuite.lifecycle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.DrainController;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Validates that drain mode blocks and resumes job claims in a real WildFly deployment. Ordering
 * (drain engaged before poller.stop()) is verified separately by {@code
 * RatchetLifecycleShutdownTest} using Mockito InOrder.
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

      // Drain engaged before enqueue — poller must not claim while draining.
      // 6s wait covers >= 3 poll cycles (POLLER_MAX_DELAY_MS=2000).
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

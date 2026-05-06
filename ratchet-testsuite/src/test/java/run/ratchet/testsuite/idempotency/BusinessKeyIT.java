package run.ratchet.testsuite.idempotency;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.SlowJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates business key deduplication: duplicate active jobs with the same key are rejected. */
class BusinessKeyIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, SlowJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetJobs() {
    SimpleJob.resetCount();
    SlowJob.reset();
  }

  @Test
  void duplicateBusinessKey_whileActive_shouldBeRejected() {
    // Submit a slow job so it stays active
    SlowJob.setSleepMs(10_000);
    JobHandle first = jobService.enqueue(SlowJob::execute).withBusinessKey("user-123").submit();

    assertNotNull(first);

    // Second submission with same business key while first is active should fail
    assertThrows(
        Exception.class,
        () -> jobService.enqueue(SimpleJob::execute).withBusinessKey("user-123").submit(),
        "Should reject duplicate business key while first job is active");
  }

  @Test
  void businessKey_afterCompletion_shouldAllowResubmission() {
    JobHandle first = jobService.enqueue(SimpleJob::execute).withBusinessKey("user-456").submit();

    JobAssertions.assertJobCompleted(jobCrudStore, first);

    // After first completes, same business key should work
    JobHandle second = jobService.enqueue(SimpleJob::execute).withBusinessKey("user-456").submit();

    assertNotNull(second);
    JobAssertions.assertJobCompleted(jobCrudStore, second);
  }
}

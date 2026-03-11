package run.ratchet.testsuite.core;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SlowJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Validates timeout behavior: a job exceeding its timeout should transition to FAILED. */
class JobTimeoutIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SlowJob.class, TestJobService.class)
        .addTestInfrastructure()
        .addBeansXml()
        .addPersistenceXml(dbType)
        .addDataSource()
        .build();
  }

  @BeforeEach
  void resetJobs() {
    SlowJob.reset();
  }

  @Test
  void slowJob_exceedingTimeout_shouldFail() {
    // SlowJob sleeps 60s by default; timeout set to 1s
    JobHandle handle =
        jobService.enqueue(SlowJob::execute).withTimeout(Duration.ofSeconds(1)).submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);
  }

  @Test
  void slowJob_withinTimeout_shouldSucceed() {
    SlowJob.setSleepMs(100);

    JobHandle handle =
        jobService.enqueue(SlowJob::execute).withTimeout(Duration.ofSeconds(30)).submit();

    assertNotNull(handle);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);
  }
}

package run.ratchet.testsuite.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobHandle;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
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

/** Validates the complete job lifecycle: submit → execute → complete state transitions. */
class JobLifecycleIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

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
  void resetJobs() {
    SimpleJob.resetCount();
  }

  @Test
  void enqueueNow_shouldExecuteAndComplete() {
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);

    assertNotNull(handle);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);
    assertEquals(1, SimpleJob.getInvocationCount());
  }

  @Test
  void enqueue_withSubmit_shouldExecuteAndComplete() {
    JobHandle handle = jobService.enqueue(SimpleJob::execute).submit();

    assertNotNull(handle);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);
  }

  @Test
  void enqueue_shouldStartInPendingStatus() {
    JobHandle handle = jobService.schedule(Duration.ofMinutes(5), SimpleJob::execute).submit();

    assertNotNull(handle);
    var job = jobCrudStore.findById(handle.id());
    assertTrue(job.isPresent());
    assertEquals(JobStatus.PENDING, job.get().getStatus());
  }
}

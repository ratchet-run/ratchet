package run.ratchet.testsuite.resource;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.ri.core.ResourcePermitService;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.ResourceTestJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates resource permit concurrency limiting for jobs tagged with a shared resource. */
class ResourcePermitIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private ResourcePermitService resourcePermitService;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(ResourceTestJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetState() {
    ResourceTestJob.reset();
  }

  @Test
  void jobsWithinPermitLimit_shouldAllComplete() {
    resourcePermitService.configureResource("test-res", 3, 1000, "test");

    List<JobHandle> handles = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      handles.add(jobService.enqueue(ResourceTestJob::execute).withResource("test-res").submit());
    }

    // Wait for all jobs to complete
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .until(() -> ResourceTestJob.getCompletedCount() >= 3);

    for (JobHandle handle : handles) {
      JobAssertions.assertJobCompleted(jobCrudStore, handle);
    }

    assertTrue(ResourceTestJob.getMaxConcurrentSeen() >= 1, "At least one job should have run");
  }

  @Test
  void jobsExceedingPermitLimit_shouldBeConcurrencyLimited() {
    resourcePermitService.configureResource("limited-res", 2, 500, "test");

    List<JobHandle> handles = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      handles.add(
          jobService.enqueue(ResourceTestJob::execute).withResource("limited-res").submit());
    }

    // Wait for all jobs to complete — longer timeout since jobs get rescheduled
    await()
        .atMost(Duration.ofSeconds(60))
        .pollInterval(Duration.ofMillis(500))
        .until(() -> ResourceTestJob.getCompletedCount() >= 5);

    for (JobHandle handle : handles) {
      JobAssertions.assertJobCompleted(jobCrudStore, handle);
    }

    // Critical invariant: never more than 2 concurrent
    assertTrue(
        ResourceTestJob.getMaxConcurrentSeen() <= 2,
        "Max concurrent should be <= 2 (permit limit) but was "
            + ResourceTestJob.getMaxConcurrentSeen());
  }
}

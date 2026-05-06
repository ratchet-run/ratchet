package run.ratchet.testsuite.archiving;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestDataManipulator;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates job archiving moves completed jobs to the archive table based on retention policy. */
class JobArchivingIT extends BaseRatchetIT {

  private static final CronParser CRON_PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
  @Inject private TestJobService jobService;
  @Inject private JobCrudStore jobCrudStore;
  @Inject private ArchiveStore archiveStore;
  @Inject private JobArchivingService archivingService;
  @Inject private TestDataManipulator dataManipulator;

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
  void resetState() {
    SimpleJob.resetCount();
  }

  @Test
  void completedJobs_olderThanRetention_shouldBeArchived() throws Exception {
    List<JobHandle> handles = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      handles.add(jobService.enqueueNow(SimpleJob::execute));
    }
    for (JobHandle handle : handles) {
      JobAssertions.assertJobCompleted(jobCrudStore, handle);
    }

    List<UUID> jobIds = handles.stream().map(JobHandle::id).toList();

    Instant past = Instant.now().minus(3, ChronoUnit.DAYS);
    for (UUID id : jobIds) {
      dataManipulator.setJobUpdatedAt(id, past);
    }

    // Configure and trigger archiving with 1-day retention
    archivingService.init(true, 1, 100, CRON_PARSER.parse("0 0 2 * * ?"));
    archivingService.triggerArchiving().get(30, TimeUnit.SECONDS);

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              var archived = archiveStore.findArchivedJobs(null, null, null, null, 100);
              assertTrue(
                  archived.size() >= 3,
                  "Expected at least 3 archived jobs but found " + archived.size());
            });

    // Verify jobs removed from active table
    for (UUID id : jobIds) {
      assertTrue(
          jobCrudStore.findById(id).isEmpty(),
          "Job " + id + " should be removed from active table");
    }

    // Verify archive entries reference original job IDs
    var archived = archiveStore.findArchivedJobs(null, null, null, null, 100);
    List<UUID> archivedOriginalIds = archived.stream().map(a -> a.getOriginalJobId()).toList();
    for (UUID id : jobIds) {
      assertTrue(archivedOriginalIds.contains(id), "Archive should contain original job ID " + id);
    }
  }

  @Test
  void activeAndRecentJobs_shouldNotBeArchived() throws Exception {
    // Submit and wait for 2 jobs — don't backdate, they're within retention
    JobHandle handle1 = jobService.enqueueNow(SimpleJob::execute);
    JobHandle handle2 = jobService.enqueueNow(SimpleJob::execute);
    JobAssertions.assertJobCompleted(jobCrudStore, handle1);
    JobAssertions.assertJobCompleted(jobCrudStore, handle2);

    // Configure and trigger archiving with 1-day retention
    archivingService.init(true, 1, 100, CRON_PARSER.parse("0 0 2 * * ?"));
    archivingService.triggerArchiving().get(30, TimeUnit.SECONDS);

    // Wait sufficient time to confirm no archiving occurs
    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> {
              var archived2 = archiveStore.findArchivedJobs(null, null, null, null, 100);
              assertTrue(archived2.isEmpty(), "Recent jobs should not be archived");
            });

    // Verify original jobs still in active table
    assertTrue(
        jobCrudStore.findById(handle1.id()).isPresent(), "Job 1 should still be in active table");
    assertTrue(
        jobCrudStore.findById(handle2.id()).isPresent(), "Job 2 should still be in active table");
  }
}

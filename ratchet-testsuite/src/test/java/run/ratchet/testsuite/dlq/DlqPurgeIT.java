package run.ratchet.testsuite.dlq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobPriority;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobBulkStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestDataManipulator;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;

/** Verifies DLQ purge semantics across store implementations. */
class DlqPurgeIT extends BaseRatchetIT {

  @Inject private JobCrudStore jobCrudStore;

  @Inject private JobBulkStore jobBulkStore;

  @Inject private TestDataManipulator dataManipulator;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void deleteDlqOlderThan_shouldPurgeFailedJobsRegardlessOfExecutionType() {
    JobEntity staleFailed = persistFailedJob();
    JobEntity freshFailed = persistFailedJob();

    Instant cutoff = Instant.now().minus(Duration.ofDays(2));
    dataManipulator.setJobUpdatedAt(staleFailed.getId(), cutoff.minus(Duration.ofHours(1)));
    dataManipulator.setJobUpdatedAt(freshFailed.getId(), cutoff.plus(Duration.ofHours(1)));

    int deleted = jobBulkStore.deleteDlqOlderThan(cutoff);

    assertEquals(1, deleted);
    assertFalse(jobCrudStore.findById(staleFailed.getId()).isPresent());
    assertTrue(jobCrudStore.findById(freshFailed.getId()).isPresent());
  }

  private JobEntity persistFailedJob() {
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.SINGLE);
    job.setStatus(JobStatus.FAILED);
    job.setPriority(JobPriority.NORMAL);
    job.setScheduledTime(Instant.now().minusSeconds(5));
    job.setPayload(JobPayloadFactory.noop());
    job.setIdempotencyKey(UUID.randomUUID().toString());
    job.setAttempts(0);
    job.setMaxRetries(0);
    job.setLastError("boom");
    return jobCrudStore.save(job);
  }
}

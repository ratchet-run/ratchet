package run.ratchet.testsuite.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import run.ratchet.api.JobPriority;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;

/** Validates store claim ordering and executable-type filtering. */
class JobPriorityIT extends BaseRatchetIT {

  @Inject private JobCrudStore jobCrudStore;

  @Inject private JobClaimStore jobClaimStore;

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
  void claimNextBatch_shouldPreferHigherPriorityAndIgnoreUnsupportedExecutionTypes() {
    Instant due = Instant.now().minusSeconds(5);
    JobEntity low = persistJob(JobExecutionType.SINGLE, JobPriority.LOW, due, null);
    JobEntity high = persistJob(JobExecutionType.SINGLE, JobPriority.HIGH, due, null);
    persistJob(JobExecutionType.DLQ_ALERT, JobPriority.CRITICAL, due, null);
    persistJob(JobExecutionType.WORKFLOW_JOIN, JobPriority.CRITICAL, due, null);

    List<JobClaimDto> claims =
        jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "priority-it-node");

    assertEquals(List.of(high.getId(), low.getId()), claims.stream().map(JobClaimDto::id).toList());
    assertEquals(
        List.of(JobExecutionType.SINGLE, JobExecutionType.SINGLE),
        claims.stream().map(JobClaimDto::jobType).toList());
  }

  @Test
  void claimNextBatch_shouldBoostLongWaitingLowPriorityJobs() {
    Instant oldDue = Instant.now().minusSeconds(60L * 45);
    Instant newDue = Instant.now().minusSeconds(30);
    JobEntity boostedLow = persistJob(JobExecutionType.SINGLE, JobPriority.LOWEST, oldDue, null);
    JobEntity normal = persistJob(JobExecutionType.SINGLE, JobPriority.NORMAL, newDue, null);

    List<JobClaimDto> claims =
        jobClaimStore.claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "priority-it-node");

    assertEquals(
        List.of(boostedLow.getId(), normal.getId()), claims.stream().map(JobClaimDto::id).toList());
  }

  @Test
  void claimDueRecurring_shouldPreferHigherPriorityRecurringMasters() {
    Instant nextFire = Instant.now().minusSeconds(5);
    JobEntity lowRecurring =
        persistJob(JobExecutionType.RECURRING, JobPriority.LOW, Instant.now(), nextFire);
    JobEntity highRecurring =
        persistJob(JobExecutionType.RECURRING, JobPriority.HIGH, Instant.now(), nextFire);

    List<JobEntity> claims = jobClaimStore.claimDueRecurring(10, "priority-it-node");

    assertEquals(
        List.of(highRecurring.getId(), lowRecurring.getId()),
        claims.stream().map(JobEntity::getId).toList());
  }

  private JobEntity persistJob(
      JobExecutionType jobType, JobPriority priority, Instant scheduledTime, Instant nextFire) {
    JobEntity job = new JobEntity();
    job.setJobType(jobType);
    job.setStatus(JobStatus.PENDING);
    job.setPriority(priority);
    job.setScheduledTime(scheduledTime);
    job.setNextFire(nextFire);
    job.setPayload(JobPayloadFactory.noop());
    job.setIdempotencyKey(UUID.randomUUID().toString());
    return jobCrudStore.save(job);
  }
}

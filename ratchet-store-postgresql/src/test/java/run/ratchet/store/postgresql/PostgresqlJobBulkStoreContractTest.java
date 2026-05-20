package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobBulkStoreContract;

class PostgresqlJobBulkStoreContractTest extends AbstractJobBulkStoreContract {

  private final PostgresqlTestFixture fixture = new PostgresqlTestFixture();

  @Override
  public JobStore store() {
    return fixture.store();
  }

  @Override
  public JobEntity newPendingJob() {
    return fixture.newPendingJob();
  }

  @Override
  public JobEntity newBatchParentJob() {
    return fixture.newBatchParentJob();
  }

  @Override
  public void cleanupStore() {
    fixture.cleanupStore();
  }

  @Test
  @org.junit.jupiter.api.Disabled(
      "Superseded by AbstractRecurringJobStoreContract after CP2 — recurring rows live in"
          + " scheduler_recurring_job, not scheduler_job.")
  void cancelOrphanedRecurringAnnotationJobs_cancelsMultipleRowsAndReservations() {
    JobEntity orphan1 = recurringJob("recurring-orphan-1");
    orphan1 = persist(orphan1);

    JobEntity orphan2 = recurringJob("recurring-orphan-2");
    orphan2 = persist(orphan2);

    JobEntity registered = recurringJob("recurring-registered");
    registered = persist(registered);

    int count =
        store()
            .cancelOrphanedRecurringAnnotationJobs(
                Set.of("recurring-registered"), Instant.now().plusSeconds(60));

    assertEquals(2, count);
    assertEquals(JobStatus.CANCELED, store().getJobStatus(orphan1.getId()));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(orphan2.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(registered.getId()));
    assertFalse(store().findActiveByBusinessKey("recurring-orphan-1").isPresent());
    assertFalse(store().findActiveByBusinessKey("recurring-orphan-2").isPresent());
    assertTrue(store().findActiveByBusinessKey("recurring-registered").isPresent());
  }

  private JobEntity recurringJob(String businessKey) {
    JobEntity job = newPendingJob();
    job.setJobType(JobExecutionType.RECURRING);
    job.setBusinessKey(businessKey);
    return job;
  }
}

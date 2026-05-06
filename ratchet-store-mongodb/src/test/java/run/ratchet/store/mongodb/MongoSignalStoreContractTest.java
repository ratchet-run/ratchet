package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractSignalContractTest;

class MongoSignalStoreContractTest extends AbstractSignalContractTest {

  private final MongoTestFixture fixture = new MongoTestFixture();

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
  void deliverSignalByIdSetsUpdatedAtToDeliveredAt() {
    JobEntity job = newPendingJob();
    job.setStatus(JobStatus.WAITING);
    job.setSignalKey("mongo-delivered-at");
    job.setSignalTimeout(Instant.parse("2026-05-05T13:00:00Z"));
    JobEntity saved = persist(job);
    Instant deliveredAt = Instant.parse("2026-05-05T12:00:00Z");

    int delivered =
        store()
            .deliverSignalById(
                saved.getId(),
                null,
                null,
                "APPROVED",
                null,
                "admin",
                deliveredAt,
                "mongo-delivery");

    assertEquals(1, delivered);
    assertEquals(deliveredAt, store().findById(saved.getId()).orElseThrow().getUpdatedAt());
  }
}

package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractArchiveStoreContract;

/** MongoDB contract test for {@code ArchiveStore} operations. */
class MongoArchiveStoreContractTest extends AbstractArchiveStoreContract {

  private static final Instant ARCHIVE_NOW = Instant.parse("2026-05-09T12:34:56Z");
  private static final MongoTestFixture fixture = new MongoTestFixture();

  @AfterAll
  static void closeFixture() {
    fixture.close();
  }

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
  void archiveJob_usesConfiguredClockForArchivedAt() {
    var job = persist(newPendingJob());
    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceeded(job.getId(), null, null, ARCHIVE_NOW, ARCHIVE_NOW, 100L, 50L);
    var completed = store().findById(job.getId()).orElseThrow();

    var archived =
        fixture
            .archiveOperations(Clock.fixed(ARCHIVE_NOW, ZoneOffset.UTC))
            .archiveJob(completed, "test", "tck");

    assertEquals(ARCHIVE_NOW, archived.getArchivedAt());
  }
}

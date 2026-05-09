package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.MongoWriteException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.JobEntity;

class IdempotencyIT extends BaseDocumentStoreIT {

  @Test
  void duplicateIdempotencyKey_isRejected() {
    String key = "unique-key-123";

    JobEntity job1 = newPendingJob();
    job1.setIdempotencyKey(key);
    store().save(job1);

    JobEntity job2 = newPendingJob();
    job2.setIdempotencyKey(key);

    assertThrows(MongoWriteException.class, () -> store().save(job2));
  }

  @Test
  void duplicateIdempotencyKey_returnsExistingJobForIdempotentSubmitPath() {
    String key = "webhook-delivery-12345";

    JobEntity original = newPendingJob();
    original.setIdempotencyKey(key);
    original = store().save(original);

    JobEntity duplicateAttempt = newPendingJob();
    duplicateAttempt.setIdempotencyKey(key);

    Optional<JobEntity> existing =
        store().findByIdempotencyKey(duplicateAttempt.getIdempotencyKey());

    assertTrue(existing.isPresent());
    assertEquals(original.getId(), existing.get().getId());
    assertEquals(key, existing.get().getIdempotencyKey());
  }

  @Test
  void businessKey_uniqueAmongActiveJobs() {
    String bizKey = "order-12345";

    JobEntity job1 = newPendingJob();
    job1.setBusinessKey(bizKey);
    store().save(job1);

    JobEntity job2 = newPendingJob();
    job2.setBusinessKey(bizKey);

    RatchetTransientStoreException ex =
        assertThrows(RatchetTransientStoreException.class, () -> store().save(job2));
    assertTrue(ex.getMessage().contains("business key"));
    assertTrue(causeMessage(ex).contains("idx_job_active_business_key"));
  }

  @Test
  void businessKey_allowsReuseAfterTerminal() {
    String bizKey = "order-67890";

    JobEntity job1 = newPendingJob();
    job1.setBusinessKey(bizKey);
    job1 = store().save(job1);
    store().claimNextBatch(1, "node-1");
    store().compareAndSwapStatus(job1.getId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, null);

    JobEntity job2 = newPendingJob();
    job2.setBusinessKey(bizKey);
    JobEntity saved = store().save(job2);
    assertNotNull(saved.getId());
    assertEquals(bizKey, saved.getBusinessKey());
  }

  @Test
  void findByIdempotencyKey() {
    String key = "find-me-key";
    JobEntity job = newPendingJob();
    job.setIdempotencyKey(key);
    store().save(job);

    Optional<JobEntity> found = store().findByIdempotencyKey(key);
    assertTrue(found.isPresent());
    assertEquals(key, found.get().getIdempotencyKey());
  }

  private static String causeMessage(Throwable throwable) {
    Throwable current = throwable;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return current.getMessage();
  }
}

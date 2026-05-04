package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.MongoWriteException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.api.JobStatus;
import java.util.Optional;
import org.junit.jupiter.api.Test;

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
  void businessKey_uniqueAmongActiveJobs() {
    String bizKey = "order-12345";

    JobEntity job1 = newPendingJob();
    job1.setBusinessKey(bizKey);
    store().save(job1);

    JobEntity job2 = newPendingJob();
    job2.setBusinessKey(bizKey);

    assertThrows(MongoWriteException.class, () -> store().save(job2));
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
}

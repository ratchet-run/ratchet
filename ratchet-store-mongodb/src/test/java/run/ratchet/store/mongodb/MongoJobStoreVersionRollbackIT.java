package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for the in-memory version rollback on stale-write in {@link MongoJobStore}.
 *
 * <p>Without the rollback, a caller that catches {@link RatchetOptimisticLockException} and reuses
 * the same entity instance for a follow-up operation would carry a phantom version bump that was
 * never persisted. The next concurrent writer to land that version would be silently overwritten on
 * the next save, defeating the optimistic-lock guarantee that this whole code path exists to
 * provide.
 */
class MongoJobStoreVersionRollbackIT extends BaseDocumentStoreIT {

  @Test
  void staleWrite_restoresEntityVersionToPrePersistValue() {
    JobEntity initial = store().save(newPendingJob());
    long id = initial.getId();
    int initialVersion = initial.getVersion();

    // Simulate a concurrent writer by mutating the row out from under our snapshot.
    JobEntity concurrentMutation = store().findById(id).orElseThrow();
    concurrentMutation.setStatus(JobStatus.RUNNING);
    store().save(concurrentMutation);

    // Our snapshot still holds version == initialVersion. save() must throw and restore the
    // in-memory version to the pre-call value so the caller's entity reflects reality.
    assertEquals(initialVersion, initial.getVersion(), "precondition: in-memory version unchanged");
    initial.setStatus(JobStatus.CANCELED);
    assertThrows(RatchetOptimisticLockException.class, () -> store().save(initial));
    assertEquals(
        initialVersion,
        initial.getVersion(),
        "stale-write must not leave a phantom version bump on the caller's entity");
  }
}

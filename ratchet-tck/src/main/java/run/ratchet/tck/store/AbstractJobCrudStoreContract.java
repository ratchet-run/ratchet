package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code JobCrudStore}. */
public abstract class AbstractJobCrudStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupCrudFixture() {
    cleanupStore();
  }

  @Test
  void saveAndFindById_roundTripsPersistedJob() {
    var saved = persist(newPendingJob());

    var reloaded = store().findById(saved.getId());

    assertTrue(reloaded.isPresent(), "Persisted job should be reloadable by ID");
    assertEquals(saved.getId(), reloaded.get().getId());
  }

  @Test
  void findByIds_returnsEveryRequestedRow() {
    var first = persist(newPendingJob());
    var second = persist(newPendingJob());

    var jobs = store().findByIds(List.of(first.getId(), second.getId()));

    assertEquals(2, jobs.size(), "findByIds should return both persisted jobs");
  }

  @Test
  void findByIdempotencyKey_returnsMatchingJob() {
    var saved = persist(newPendingJob());

    var reloaded = store().findByIdempotencyKey(saved.getIdempotencyKey());

    assertTrue(reloaded.isPresent(), "Persisted job should be reloadable by idempotency key");
    assertEquals(saved.getId(), reloaded.get().getId());
  }
}

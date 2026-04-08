package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
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

  /**
   * Two threads load the same job, mutate their respective in-memory copies, and race to save.
   * Exactly one save must surface a stale-write / optimistic-lock failure recognised by the
   * fixture's {@link JobStoreContractFixture#isStaleWriteException(Throwable)} predicate; the other
   * must succeed.
   *
   * <p>Both snapshots are loaded <i>before</i> the {@link ConcurrentTestRunner} starts the racing
   * tasks — if either thread reloaded after the race started, fast hardware would let the winner
   * complete its entire save cycle before the loser even read the row, and both saves would succeed
   * against a broken store. Pre-loading the snapshots is what forces the conflict.
   *
   * <p>This contract is deliberately type-agnostic: it does not know whether a store throws {@code
   * RatchetOptimisticLockException}, {@code jakarta.persistence.OptimisticLockException}, or a
   * driver-specific type. Stores declare what counts as a stale-write observation via the fixture
   * predicate, so the TCK stays stable across API evolutions.
   */
  @Test
  void save_concurrentMutation_oneThreadObservesStaleWrite() {
    JobEntity initial = persist(newPendingJob());
    long id = initial.getId();

    // Pre-load both snapshots BEFORE the latch release. Both hold the same version.
    JobEntity snapshotA = store().findById(id).orElseThrow();
    JobEntity snapshotB = store().findById(id).orElseThrow();

    List<Throwable> failures =
        ConcurrentTestRunner.runAll(
            Duration.ofSeconds(10),
            () -> {
              snapshotA.setStatus(JobStatus.RUNNING);
              store().save(snapshotA);
            },
            () -> {
              snapshotB.setStatus(JobStatus.CANCELED);
              store().save(snapshotB);
            });

    long staleWriteCount =
        failures.stream().filter(t -> t != null && isStaleWriteException(t)).count();
    long otherFailureCount =
        failures.stream().filter(t -> t != null && !isStaleWriteException(t)).count();

    assertEquals(
        0L,
        otherFailureCount,
        "no thread should fail with a non-stale-write exception; got " + failures);
    assertEquals(
        1L,
        staleWriteCount,
        "exactly one thread must observe a stale-write failure; got " + failures);
  }
}

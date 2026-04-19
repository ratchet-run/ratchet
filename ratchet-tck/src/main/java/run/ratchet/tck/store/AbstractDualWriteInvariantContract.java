package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Contract tests for stores that split live queue state from cold metadata into separate physical
 * structures (e.g. the MySQL hot/cold split with {@code scheduler_job_queue} and {@code
 * scheduler_business_key_reservation}).
 *
 * <p>The assertions are SPI-only and pass on any conforming store, but they are designed to expose
 * dual-write defects: bkres leaks at terminal, hot rows surviving terminal, count drift between the
 * live queue and the metadata table, and stale ownership of business keys after lifecycle
 * transitions.
 */
public abstract class AbstractDualWriteInvariantContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupDualWriteFixture() {
    cleanupStore();
  }

  @Test
  void enqueue_thenSucceed_releasesBusinessKey() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));

    store().compareAndSwapStatus(first.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    assertTrue(
        store().markJobSucceededMinimal(first.getId(), Instant.now(), Instant.now(), 0L, 0L),
        "markJobSucceededMinimal should succeed for a RUNNING job");

    assertEquals(JobStatus.SUCCEEDED, store().getJobStatus(first.getId()));
    assertTrue(
        store().findActiveByBusinessKey(bk).isEmpty(),
        "Business key should be released after terminal SUCCEEDED");

    JobEntity second = persist(jobWithBusinessKey(bk));
    assertNotNull(second.getId(), "Re-enqueue with the same business key should succeed");
    assertEquals(JobStatus.PENDING, store().getJobStatus(second.getId()));
  }

  @Test
  void enqueue_thenFailTerminal_releasesBusinessKey() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));
    store().compareAndSwapStatus(first.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    assertTrue(
        store().markJobFailedTerminal(first.getId(), "permanent error", 3),
        "markJobFailedTerminal should succeed for a RUNNING job");

    assertEquals(JobStatus.FAILED, store().getJobStatus(first.getId()));
    assertTrue(
        store().findActiveByBusinessKey(bk).isEmpty(),
        "Business key should be released after terminal FAILED");

    persist(jobWithBusinessKey(bk));
  }

  @Test
  void enqueue_thenCancel_releasesBusinessKey() {
    String bk = uniqueBusinessKey();
    JobEntity first = persist(jobWithBusinessKey(bk));

    assertTrue(store().cancelJob(first.getId()), "cancelJob should succeed for a PENDING job");

    assertEquals(JobStatus.CANCELED, store().getJobStatus(first.getId()));
    assertTrue(
        store().findActiveByBusinessKey(bk).isEmpty(),
        "Business key should be released after terminal CANCELED");

    persist(jobWithBusinessKey(bk));
  }

  @Test
  void resetFailedToPending_keepsBusinessKeyReserved() {
    String bk = uniqueBusinessKey();
    JobEntity job = persist(jobWithBusinessKey(bk));
    long id = job.getId();
    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobFailedTerminal(id, "transient", 1);

    assertTrue(store().resetFailedToPending(id), "resetFailedToPending should succeed");

    assertEquals(JobStatus.PENDING, store().getJobStatus(id));
    var active = store().findActiveByBusinessKey(bk);
    assertTrue(active.isPresent(), "Business key should be re-reserved after reset");
    assertEquals(id, active.get().getId());

    try {
      persist(jobWithBusinessKey(bk));
      fail("Re-enqueue while business key is held by reset job should fail");
    } catch (RuntimeException expected) {
      // expected — duplicate active business key
    }
  }

  @Test
  void terminalTransition_clearsLiveStatusFromCounts() {
    String bk = uniqueBusinessKey();
    JobEntity job = persist(jobWithBusinessKey(bk));

    long pendingBefore = store().countJobsByStatus(JobStatus.PENDING);
    long succeededBefore = store().countJobsByStatus(JobStatus.SUCCEEDED);

    store().compareAndSwapStatus(job.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceededMinimal(job.getId(), Instant.now(), Instant.now(), 0L, 0L);

    long pendingAfter = store().countJobsByStatus(JobStatus.PENDING);
    long succeededAfter = store().countJobsByStatus(JobStatus.SUCCEEDED);

    assertEquals(
        pendingBefore - 1,
        pendingAfter,
        "PENDING count should decrease by one after terminal SUCCEEDED");
    assertEquals(
        succeededBefore + 1,
        succeededAfter,
        "SUCCEEDED count should increase by one after terminal SUCCEEDED");
  }

  @Test
  void terminalTransition_makesJobUnclaimable() {
    JobEntity job = persist(newPendingJob());
    long id = job.getId();
    store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null);
    store().markJobSucceededMinimal(id, Instant.now(), Instant.now(), 0L, 0L);

    assertFalse(
        store().tryPickUpJob(id, "node-rebound"),
        "tryPickUpJob must not pick up a terminal job (no live row should remain)");
    assertEquals(JobStatus.SUCCEEDED, store().getJobStatus(id));
  }

  @Test
  void cancelBeforeClaim_releasesAllOwnership() {
    String bk = uniqueBusinessKey();
    JobEntity job = persist(jobWithBusinessKey(bk));
    long id = job.getId();

    assertTrue(store().cancelJob(id));

    assertEquals(JobStatus.CANCELED, store().getJobStatus(id));
    assertFalse(
        store().tryPickUpJob(id, "node-late"),
        "tryPickUpJob must not pick up a CANCELED job after enqueue cancel");
    assertTrue(
        store().findActiveByBusinessKey(bk).isEmpty(),
        "cancel before claim must free the business key");
  }

  @Test
  void duplicateBusinessKey_failsWhilePeerIsLive() {
    String bk = uniqueBusinessKey();
    persist(jobWithBusinessKey(bk));

    try {
      persist(jobWithBusinessKey(bk));
      fail("Duplicate enqueue with active business key should throw");
    } catch (RuntimeException expected) {
      // expected
    }
  }

  @Test
  void concurrentDuplicateEnqueue_atMostOneSucceeds() {
    String bk = uniqueBusinessKey();
    AtomicInteger successCount = new AtomicInteger();

    ConcurrentTestRunner.runAll(
        Duration.ofSeconds(15),
        () -> tryEnqueue(bk, successCount),
        () -> tryEnqueue(bk, successCount),
        () -> tryEnqueue(bk, successCount));

    assertEquals(
        1,
        successCount.get(),
        "Exactly one concurrent enqueue with the same business key should succeed");
    var active = store().findActiveByBusinessKey(bk);
    assertTrue(active.isPresent(), "Surviving job should own the business key");
  }

  private void tryEnqueue(String bk, AtomicInteger counter) {
    try {
      persist(jobWithBusinessKey(bk));
      counter.incrementAndGet();
    } catch (RuntimeException ignored) {
      // expected for losers
    }
  }

  private JobEntity jobWithBusinessKey(String bk) {
    JobEntity job = newPendingJob();
    job.setBusinessKey(bk);
    return job;
  }

  private static String uniqueBusinessKey() {
    return "dual-write-" + UUID.randomUUID();
  }
}

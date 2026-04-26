package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobStatus;
import run.ratchet.tck.util.ConcurrentTestRunner;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code JobBatchStatusStore}. */
public abstract class AbstractJobBatchStatusStoreContract implements JobStoreContractFixture {

  @AfterEach
  void cleanupBatchStatusFixture() {
    cleanupStore();
  }

  @Test
  void compareAndSwapStatus_updatesExpectedState() {
    var saved = persist(newPendingJob());

    boolean updated =
        store().compareAndSwapStatus(saved.getId(), JobStatus.PENDING, JobStatus.RUNNING, null);

    assertTrue(updated, "Pending job should transition to RUNNING");
    assertEquals(JobStatus.RUNNING, store().getJobStatus(saved.getId()));
  }

  @Test
  void compareAndSwapStatus_failsOnStatusMismatch() {
    var saved = persist(newPendingJob());

    boolean updated =
        store().compareAndSwapStatus(saved.getId(), JobStatus.RUNNING, JobStatus.CANCELED, null);

    assertFalse(updated, "CAS from wrong expected status should return false");
    assertEquals(
        JobStatus.PENDING,
        store().getJobStatus(saved.getId()),
        "Status should remain PENDING after failed CAS");
  }

  @Test
  void compareAndSwapStatus_concurrent_atMostOneSucceeds() {
    var saved = persist(newPendingJob());
    long id = saved.getId();

    AtomicInteger successCount = new AtomicInteger();

    ConcurrentTestRunner.runAll(
        Duration.ofSeconds(10),
        () -> {
          if (store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null)) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null)) {
            successCount.incrementAndGet();
          }
        },
        () -> {
          if (store().compareAndSwapStatus(id, JobStatus.PENDING, JobStatus.RUNNING, null)) {
            successCount.incrementAndGet();
          }
        });

    assertTrue(
        successCount.get() <= 1, "at most one CAS should succeed; got " + successCount.get());
    assertEquals(JobStatus.RUNNING, store().getJobStatus(id), "Job should be RUNNING after CAS");
  }

  @Test
  void tryPickUpJob_setsStatusAndPickedBy() {
    var saved = persist(newPendingJob());

    boolean picked = store().tryPickUpJob(saved.getId(), "node-1");

    assertTrue(picked, "tryPickUpJob should succeed on a PENDING job");
    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.RUNNING, reloaded.getStatus());
    assertEquals("node-1", reloaded.getPickedBy());
  }

  @Test
  void tryPickUpJob_failsOnAlreadyRunning() {
    var saved = persist(newPendingJob());
    store().tryPickUpJob(saved.getId(), "node-1");

    boolean secondPick = store().tryPickUpJob(saved.getId(), "node-2");

    assertFalse(secondPick, "tryPickUpJob should fail on an already-running job");
  }
}

package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Base contract tests for {@code JobBatchStatusStore}. */
public abstract class AbstractJobBatchStatusStoreContract implements JobStoreContractFixture {

  @BeforeEach
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
    UUID id = saved.getId();

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

  @Test
  void updateJobStatus_updatesLiveStatusAndError() {
    var saved = persist(newPendingJob());

    store().updateJobStatus(saved.getId(), JobStatus.PAUSED, "manual pause");

    var reloaded = store().findById(saved.getId()).orElseThrow();
    assertEquals(JobStatus.PAUSED, reloaded.getStatus());
    assertEquals("manual pause", reloaded.getLastError());
  }

  @Test
  void resetRunningJob_reclaimsMatchingNodeOnly() {
    var matching = runningJob("node-1");
    var wrongNode = runningJob("node-2");

    boolean reset = store().resetRunningJob(matching.getId(), "node-1");
    boolean wrongNodeReset = store().resetRunningJob(wrongNode.getId(), "node-1");

    assertTrue(reset, "Matching RUNNING row should be reset");
    assertFalse(wrongNodeReset, "RUNNING row owned by another node should not be reset");

    var resetJob = store().findById(matching.getId()).orElseThrow();
    assertEquals(JobStatus.PENDING, resetJob.getStatus());
    assertNull(resetJob.getPickedBy());
    assertNull(resetJob.getPickedAt());

    var preserved = store().findById(wrongNode.getId()).orElseThrow();
    assertEquals(JobStatus.RUNNING, preserved.getStatus());
    assertEquals("node-2", preserved.getPickedBy());
  }

  @Test
  void resetRunningJobs_reclaimsAllRowsForNode() {
    var first = runningJob("node-1");
    var second = runningJob("node-1");
    var otherNode = runningJob("node-2");

    int reset = store().resetRunningJobs("node-1");

    assertEquals(2, reset, "Only rows owned by the requested node should be reset");
    assertEquals(JobStatus.PENDING, store().getJobStatus(first.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(second.getId()));
    assertEquals(JobStatus.RUNNING, store().getJobStatus(otherNode.getId()));
  }

  @Test
  void cancelRecurringJobsByTag_cancelsOnlyTaggedRecurringJobs() {
    var tagged = recurringJob("recurring-tag", "recurring-key-1");
    var secondTagged = recurringJob("recurring-tag", "recurring-key-2");
    var untagged = recurringJob("other-tag", "recurring-key-3");
    var oneShot = persist(newPendingJob("recurring-tag"));

    int canceled = store().cancelRecurringJobsByTag("recurring-tag");

    assertEquals(2, canceled);
    assertEquals(JobStatus.CANCELED, store().getJobStatus(tagged.getId()));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(secondTagged.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(untagged.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(oneShot.getId()));
  }

  @Test
  void cancelRecurringJobByBusinessKey_cancelsMatchingRecurringJob() {
    var matching = recurringJob("tag-a", "business-key-target");
    var other = recurringJob("tag-b", "business-key-other");

    int canceled = store().cancelRecurringJobByBusinessKey("business-key-target");

    assertEquals(1, canceled);
    assertEquals(JobStatus.CANCELED, store().getJobStatus(matching.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(other.getId()));
  }

  @Test
  void cancelRecurringJobsByBusinessKeys_cancelsMatchingRecurringJobsInBulk() {
    var first = recurringJob("tag-a", "business-key-first");
    var second = recurringJob("tag-b", "business-key-second");
    var other = recurringJob("tag-c", "business-key-other");
    var oneShot = persist(newPendingJob("tag-a"));

    int canceled =
        store()
            .cancelRecurringJobsByBusinessKeys(Set.of("business-key-first", "business-key-second"));

    assertEquals(2, canceled);
    assertEquals(JobStatus.CANCELED, store().getJobStatus(first.getId()));
    assertEquals(JobStatus.CANCELED, store().getJobStatus(second.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(other.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(oneShot.getId()));
  }

  @Test
  void cancelOrphanedRecurringAnnotationJobs_cancelsOnlyOldUnregisteredRecurringJobs() {
    var orphan = recurringJob("tag-a", "annotation-missing");
    var registered = recurringJob("tag-b", "annotation-registered");
    var noBusinessKey = recurringJob("tag-c", null);

    int canceled =
        store()
            .cancelOrphanedRecurringAnnotationJobs(
                Set.of("annotation-registered"), Instant.now().plusSeconds(60));

    assertEquals(1, canceled);
    assertEquals(JobStatus.CANCELED, store().getJobStatus(orphan.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(registered.getId()));
    assertEquals(JobStatus.PENDING, store().getJobStatus(noBusinessKey.getId()));
  }

  private JobEntity runningJob(String nodeId) {
    var job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), nodeId));
    return store().findById(job.getId()).orElseThrow();
  }

  private JobEntity recurringJob(String tag, String businessKey) {
    JobEntity job = newPendingJob(tag);
    job.setJobType(JobExecutionType.RECURRING);
    job.setBusinessKey(businessKey);
    job.setCronExpr("0 * * * *");
    job.setNextFire(Instant.now().plusSeconds(60));
    return persist(job);
  }
}

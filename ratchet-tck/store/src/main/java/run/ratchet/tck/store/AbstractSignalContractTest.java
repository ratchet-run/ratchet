package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * TCK contracts for {@link run.ratchet.store.spi.SignalStore} (composed into {@link
 * run.ratchet.store.spi.JobStore}).
 *
 * <p>Contracts are store-layer only — no RI or CDI involvement. The RI timeout scanner and event
 * publication are tested separately in ratchet unit tests.
 */
public abstract class AbstractSignalContractTest implements JobStoreContractFixture {

  @AfterEach
  void cleanup() {
    cleanupStore();
  }

  @Test
  void createSignalWaitingJob_roundTripsSignalFields() {
    JobEntity job = newWaitingJob("approval", Instant.now().plusSeconds(3600));
    JobEntity saved = persist(job);

    JobEntity reloaded = store().findById(saved.getId()).orElseThrow();

    assertEquals(JobStatus.WAITING, reloaded.getStatus());
    assertEquals("approval", reloaded.getSignalKey());
    assertNotNull(reloaded.getSignalTimeout());
  }

  @Test
  void deliverSignalById_transitionsWaitingToPending() {
    JobEntity job = newWaitingJob("gate-1", Instant.now().plusSeconds(600));
    JobEntity saved = persist(job);
    UUID jobId = saved.getId();

    int count = store().deliverSignalById(jobId, "{\"approved\":true}", "admin", Instant.now());

    assertEquals(1, count, "exactly one job should be unblocked");
    JobEntity reloaded = store().findById(jobId).orElseThrow();
    assertEquals(JobStatus.PENDING, reloaded.getStatus());
  }

  @Test
  void deliverSignalById_roundTripsPayloadMetadataForExecutionHydration() {
    JobEntity job = newWaitingJob("gate-payload", Instant.now().plusSeconds(600));
    JobEntity saved = persist(job);
    UUID jobId = saved.getId();
    Instant deliveredAt = Instant.now();

    int count = store().deliverSignalById(jobId, "{\"approved\":true}", "admin", deliveredAt);

    assertEquals(1, count);
    JobEntity reloaded = store().findById(jobId).orElseThrow();
    assertEquals(JobStatus.PENDING, reloaded.getStatus());
    assertEquals("{\"approved\":true}", reloaded.getSignalPayload());
    assertEquals("admin", reloaded.getSignalDeliveredBy());
    assertNotNull(reloaded.getSignalDeliveredAt());
  }

  @Test
  void deliverSignalById_roundTripsDecisionMetadata() {
    JobEntity job = newWaitingJob("gate-decision", Instant.now().plusSeconds(600));
    JobEntity saved = persist(job);
    UUID jobId = saved.getId();
    Instant deliveredAt = Instant.now();

    int count =
        store()
            .deliverSignalById(
                jobId,
                "{\"outcome\":\"REJECTED\"}",
                "DECISION",
                "REJECTED",
                "policy denied",
                "admin",
                deliveredAt,
                "delivery-id-1");

    assertEquals(1, count);
    JobEntity reloaded = store().findById(jobId).orElseThrow();
    assertEquals(JobStatus.PENDING, reloaded.getStatus());
    assertEquals("{\"outcome\":\"REJECTED\"}", reloaded.getSignalPayload());
    assertEquals("DECISION", reloaded.getSignalPayloadType());
    assertEquals("REJECTED", reloaded.getSignalOutcome());
    assertEquals("policy denied", reloaded.getSignalRejectionReason());
    assertEquals("admin", reloaded.getSignalDeliveredBy());
    assertNotNull(reloaded.getSignalDeliveredAt());
    assertEquals("delivery-id-1", reloaded.getSignalDeliveryId());
  }

  @Test
  void deliverSignalById_idempotent_returnZeroOnNonWaiting() {
    JobEntity job = newWaitingJob("gate-2", Instant.now().plusSeconds(600));
    JobEntity saved = persist(job);
    UUID jobId = saved.getId();

    store().deliverSignalById(jobId, null, null, Instant.now());
    int second = store().deliverSignalById(jobId, null, null, Instant.now());

    assertEquals(0, second, "second delivery to a non-WAITING job must return 0");
  }

  @Test
  void deliverSignalById_missingJob_returnsZero() {
    int count = store().deliverSignalById(UUID.randomUUID(), null, null, Instant.now());
    assertEquals(0, count);
  }

  @Test
  void deliverSignalByKey_unblocksAllMatchingWaitingJobs() {
    String key = "approval-broadcast";
    JobEntity j1 = persist(newWaitingJob(key, Instant.now().plusSeconds(600)));
    JobEntity j2 = persist(newWaitingJob(key, Instant.now().plusSeconds(600)));
    JobEntity other = persist(newWaitingJob("other-key", Instant.now().plusSeconds(600)));

    int count = store().deliverSignalByKey(key, "{\"ok\":true}", "system", Instant.now());

    assertEquals(2, count, "both jobs with matching key should be unblocked");

    assertEquals(JobStatus.PENDING, store().findById(j1.getId()).orElseThrow().getStatus());
    assertEquals(JobStatus.PENDING, store().findById(j2.getId()).orElseThrow().getStatus());
    assertEquals(JobStatus.WAITING, store().findById(other.getId()).orElseThrow().getStatus());
  }

  @Test
  void deliverSignalByKey_recordsSharedDeliveryIdAndFindsDeliveredJobs() {
    String key = "approval-broadcast-decision";
    JobEntity j1 = persist(newWaitingJob(key, Instant.now().plusSeconds(600)));
    JobEntity j2 = persist(newWaitingJob(key, Instant.now().plusSeconds(600)));
    persist(newWaitingJob("other-broadcast-decision", Instant.now().plusSeconds(600)));

    int count =
        store()
            .deliverSignalByKey(
                key,
                "{\"approved\":true}",
                "DECISION",
                "APPROVED",
                null,
                "system",
                Instant.now(),
                "delivery-id-2");

    assertEquals(2, count, "both jobs with matching key should be unblocked");

    List<UUID> deliveredIds =
        store().findJobsBySignalDeliveryId("delivery-id-2").stream().map(JobEntity::getId).toList();
    assertTrue(deliveredIds.contains(j1.getId()));
    assertTrue(deliveredIds.contains(j2.getId()));

    JobEntity reloaded = store().findById(j1.getId()).orElseThrow();
    assertEquals("DECISION", reloaded.getSignalPayloadType());
    assertEquals("APPROVED", reloaded.getSignalOutcome());
    assertEquals("delivery-id-2", reloaded.getSignalDeliveryId());
  }

  @Test
  void deliverSignalByKey_noMatch_returnsZero() {
    int count = store().deliverSignalByKey("no-such-key", null, null, Instant.now());
    assertEquals(0, count);
  }

  @Test
  void findTimedOutSignalJobs_returnsJobsPastDeadline() {
    Instant pastDeadline = Instant.now().minusSeconds(10);
    Instant futureDeadline = Instant.now().plusSeconds(3600);

    JobEntity expired = persist(newWaitingJob("expired-key", pastDeadline));
    persist(newWaitingJob("future-key", futureDeadline));

    List<JobEntity> timedOut = store().findTimedOutSignalJobs(Instant.now());

    assertTrue(
        timedOut.stream().anyMatch(j -> j.getId().equals(expired.getId())),
        "expired WAITING job must appear in timeout scan");
    assertFalse(
        timedOut.stream().anyMatch(j -> j.getStatus() != JobStatus.WAITING),
        "only WAITING jobs should be returned");
  }

  @Test
  void findTimedOutSignalJobs_honorsLimit() {
    persist(newWaitingJob("expired-limit-1", Instant.now().minusSeconds(30)));
    persist(newWaitingJob("expired-limit-2", Instant.now().minusSeconds(20)));
    persist(newWaitingJob("expired-limit-3", Instant.now().minusSeconds(10)));

    List<JobEntity> timedOut = store().findTimedOutSignalJobs(Instant.now(), 2);

    assertEquals(2, timedOut.size(), "timeout scan must honor the requested batch limit");
  }

  @Test
  void findTimedOutSignalJobs_hydratesRetryBackoffMetadata() {
    JobEntity expired = newWaitingJob("expired-backoff", Instant.now().minusSeconds(10));
    expired.setBackoffPolicy(BackoffPolicy.FIXED);
    expired.setBackoffParamMs(1234);
    JobEntity saved = persist(expired);

    JobEntity timedOut =
        store().findTimedOutSignalJobs(Instant.now()).stream()
            .filter(j -> j.getId().equals(saved.getId()))
            .findFirst()
            .orElseThrow();

    assertEquals(BackoffPolicy.FIXED, timedOut.getBackoffPolicy());
    assertEquals(1234, timedOut.getBackoffParamMs());
  }

  @Test
  void findTimedOutSignalJobs_excludesFutureDeadlines() {
    Instant futureDeadline = Instant.now().plusSeconds(3600);
    persist(newWaitingJob("future", futureDeadline));

    List<JobEntity> timedOut = store().findTimedOutSignalJobs(Instant.now());

    assertFalse(
        timedOut.stream().anyMatch(j -> "future".equals(j.getSignalKey())),
        "job with future deadline must not appear in timeout scan");
  }

  @Test
  void findTimedOutSignalJobs_excludesDeliveredJobs() {
    Instant pastDeadline = Instant.now().minusSeconds(10);
    JobEntity job = persist(newWaitingJob("delivered", pastDeadline));
    store().deliverSignalById(job.getId(), null, null, Instant.now());

    List<JobEntity> timedOut = store().findTimedOutSignalJobs(Instant.now());

    assertFalse(
        timedOut.stream().anyMatch(j -> j.getId().equals(job.getId())),
        "already-delivered (PENDING) job must not appear in timeout scan");
  }

  @Test
  void compareAndSwapStatus_waitingToCanceledSucceeds() {
    JobEntity job = persist(newWaitingJob("cancel-waiting", Instant.now().plusSeconds(600)));

    boolean canceled =
        store().compareAndSwapStatus(job.getId(), JobStatus.WAITING, JobStatus.CANCELED, null);

    assertTrue(canceled, "WAITING jobs must be cancellable via CAS");
    assertEquals(JobStatus.CANCELED, store().findById(job.getId()).orElseThrow().getStatus());
  }

  @Test
  void compareAndSwapStatus_waitingToFailedSucceeds() {
    JobEntity job = persist(newWaitingJob("fail-waiting", Instant.now().plusSeconds(600)));

    boolean failed =
        store().compareAndSwapStatus(job.getId(), JobStatus.WAITING, JobStatus.FAILED, "timeout");

    assertTrue(failed, "WAITING jobs must be fail-able via CAS");
    JobEntity reloaded = store().findById(job.getId()).orElseThrow();
    assertEquals(JobStatus.FAILED, reloaded.getStatus());
    assertEquals("timeout", reloaded.getLastError());
  }

  private JobEntity newWaitingJob(String signalKey, Instant signalTimeout) {
    JobEntity job = newPendingJob();
    job.setStatus(JobStatus.WAITING);
    job.setSignalKey(signalKey);
    job.setSignalTimeout(signalTimeout);
    return job;
  }
}

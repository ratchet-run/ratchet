package run.ratchet.store.spi;

import run.ratchet.api.Incubating;
import run.ratchet.api.SignalDecision;
import run.ratchet.store.entity.JobEntity;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Store operations for signal-waiting jobs.
 *
 * <p>Signal delivery uses atomic bulk updates ({@link #deliverSignalByKey}) rather than a
 * fetch-then-CAS-per-row pattern to eliminate TOCTOU races where concurrent deliveries would
 * double-unblock the same WAITING set.
 */
@Incubating
public interface SignalStore {

  /**
   * Returns all WAITING jobs whose {@code signalTimeout} is at or before {@code now}. Used by the
   * timeout scanner to transition expired jobs to FAILED.
   */
  List<JobEntity> findTimedOutSignalJobs(Instant now);

  /**
   * Atomically delivers a signal to the specific WAITING job identified by {@code jobId},
   * transitioning it to PENDING and recording delivery metadata.
   *
   * @param jobId the target job
   * @param payload serialized signal payload (JSON); may be null
   * @param deliveredBy principal or node that delivered the signal; may be null
   * @param deliveredAt timestamp of delivery
   * @return 1 if the job was transitioned from WAITING to PENDING, 0 otherwise
   */
  default int deliverSignalById(
      UUID jobId, String payload, String deliveredBy, Instant deliveredAt) {
    return deliverSignalById(
        jobId,
        payload,
        null,
        SignalDecision.Outcome.APPROVED.name(),
        null,
        deliveredBy,
        deliveredAt,
        UUID.randomUUID().toString());
  }

  int deliverSignalById(
      UUID jobId,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId);

  /**
   * Atomically delivers a signal to ALL WAITING jobs whose {@code signalKey} matches, in a single
   * bulk update. Implementations MUST use a single UPDATE … WHERE statement (SQL stores) or a
   * {@code updateMany} within a session transaction (MongoDB) to prevent duplicate-delivery races.
   *
   * @param signalKey the named signal key to match
   * @param payload serialized signal payload (JSON); may be null
   * @param deliveredBy principal or node that delivered the signal; may be null
   * @param deliveredAt timestamp of delivery
   * @return the number of jobs transitioned from WAITING to PENDING
   */
  default int deliverSignalByKey(
      String signalKey, String payload, String deliveredBy, Instant deliveredAt) {
    return deliverSignalByKey(
        signalKey,
        payload,
        null,
        SignalDecision.Outcome.APPROVED.name(),
        null,
        deliveredBy,
        deliveredAt,
        UUID.randomUUID().toString());
  }

  int deliverSignalByKey(
      String signalKey,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId);

  /**
   * Returns jobs updated by a signal delivery token. Used for per-job events after bulk delivery.
   */
  default List<JobEntity> findJobsBySignalDeliveryId(String deliveryId) {
    return List.of();
  }
}

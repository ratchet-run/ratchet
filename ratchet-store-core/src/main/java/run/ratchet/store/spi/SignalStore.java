package run.ratchet.store.spi;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import run.ratchet.api.Incubating;
import run.ratchet.store.entity.JobEntity;

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
  default List<JobEntity> findTimedOutSignalJobs(Instant now) {
    return findTimedOutSignalJobs(now, Integer.MAX_VALUE);
  }

  /**
   * Returns at most {@code limit} WAITING jobs whose {@code signalTimeout} is at or before {@code
   * now}. Implementations should return a deterministic oldest-first slice when the store can order
   * efficiently.
   *
   * @param now current time used for deadline comparison
   * @param limit maximum number of jobs to return; must be positive
   * @return expired WAITING jobs, never null
   *     <p>Transaction attribute: {@code SUPPORTS}.
   * @throws RuntimeException when the store cannot read timed-out signal jobs
   */
  List<JobEntity> findTimedOutSignalJobs(Instant now, int limit);

  /**
   * Delivers a signal to one WAITING job.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. The WAITING-to-PENDING update and delivery
   * metadata write must be atomic. Implementations must throw if the update cannot be completed; a
   * normal return value means the delivery transaction committed for that many rows.
   *
   * @throws RuntimeException when the store cannot complete the delivery atomically
   */
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
   * Delivers a signal to all WAITING jobs with the same signal key.
   *
   * <p><b>Transaction attribute:</b> {@code REQUIRED}. Implementations must use one atomic bulk
   * update, or the store's closest equivalent, so concurrent deliveries cannot unblock the same
   * jobs twice. Implementations must throw if the update cannot be completed; a normal return value
   * means the delivery transaction committed for that many rows.
   *
   * @throws RuntimeException when the store cannot complete the delivery atomically
   */
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
   *
   * <p><b>Transaction attribute:</b> {@code SUPPORTS}. This method reads the result of a completed
   * delivery and must not start its own write transaction.
   *
   * @return an empty list if {@code deliveryId} is unknown; never null
   * @throws RuntimeException when the store cannot read delivery results
   */
  List<JobEntity> findJobsBySignalDeliveryId(String deliveryId);
}

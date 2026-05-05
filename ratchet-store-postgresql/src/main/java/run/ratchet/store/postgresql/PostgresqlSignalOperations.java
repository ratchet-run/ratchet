package run.ratchet.store.postgresql;

import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.SignalStore;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * PostgreSQL implementation of {@link SignalStore}.
 *
 * <p>All operations target {@code scheduler_job_queue} — the live-state table in the hot/cold split
 * — because WAITING is a non-terminal status that lives there.
 */
final class PostgresqlSignalOperations implements SignalStore {

  private static final Logger log = Logger.getLogger(PostgresqlSignalOperations.class);

  private final PostgresqlStoreContext ctx;

  PostgresqlSignalOperations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> findTimedOutSignalJobs(Instant now) {
    // language=PostgreSQL
    String sql =
        """
        SELECT q.job_id, q.signal_key, q.signal_timeout, q.status,
               c.job_type, c.priority, c.max_retries, c.business_key,
               q.signal_payload, q.signal_payload_type, q.signal_outcome,
               q.signal_rejection_reason, q.signal_delivered_at,
               q.signal_delivered_by, q.signal_delivery_id
        FROM scheduler_job_queue q
        JOIN scheduler_job c ON c.job_id = q.job_id
        WHERE q.status = 'WAITING'
          AND q.signal_timeout IS NOT NULL
          AND q.signal_timeout <= ?
        """;
    List<Object[]> rows =
        ctx.em().createNativeQuery(sql).setParameter(1, Timestamp.from(now)).getResultList();

    List<JobEntity> result = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      JobEntity job = new JobEntity();
      job.setId(toUuid(row[0]));
      job.setSignalKey((String) row[1]);
      job.setSignalTimeout(toInstant(row[2]));
      job.setStatus(JobStatus.WAITING);
      job.setJobType(row[4] != null ? JobExecutionType.valueOf((String) row[4]) : null);
      job.setPriority(
          row[5] != null ? JobPriority.values()[((Number) row[5]).intValue()] : JobPriority.NORMAL);
      job.setMaxRetries(row[6] != null ? ((Number) row[6]).intValue() : 0);
      job.setBusinessKey((String) row[7]);
      job.setSignalPayload((String) row[8]);
      job.setSignalPayloadType((String) row[9]);
      job.setSignalOutcome((String) row[10]);
      job.setSignalRejectionReason((String) row[11]);
      job.setSignalDeliveredAt(toInstant(row[12]));
      job.setSignalDeliveredBy((String) row[13]);
      job.setSignalDeliveryId((String) row[14]);
      job.setBackoffPolicy(BackoffPolicy.NONE);
      result.add(job);
    }
    return result;
  }

  @Override
  public int deliverSignalById(
      UUID jobId,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING',
            signal_payload = ?,
            signal_payload_type = ?,
            signal_outcome = ?,
            signal_rejection_reason = ?,
            signal_delivered_at = ?,
            signal_delivered_by = ?,
            signal_delivery_id = ?,
            updated_at = NOW()
        WHERE job_id = ? AND status = 'WAITING'
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, payload)
            .setParameter(2, payloadType)
            .setParameter(3, outcome)
            .setParameter(4, rejectionReason)
            .setParameter(5, deliveredAt != null ? Timestamp.from(deliveredAt) : null)
            .setParameter(6, deliveredBy)
            .setParameter(7, deliveryId)
            .setParameter(8, jobId)
            .executeUpdate();
    log.debugf("deliverSignalById(%s): %s row(s) updated", jobId, updated);
    return updated;
  }

  @Override
  public int deliverSignalByKey(
      String signalKey,
      String payload,
      String payloadType,
      String outcome,
      String rejectionReason,
      String deliveredBy,
      Instant deliveredAt,
      String deliveryId) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING',
            signal_payload = ?,
            signal_payload_type = ?,
            signal_outcome = ?,
            signal_rejection_reason = ?,
            signal_delivered_at = ?,
            signal_delivered_by = ?,
            signal_delivery_id = ?,
            updated_at = NOW()
        WHERE signal_key = ? AND status = 'WAITING'
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, payload)
            .setParameter(2, payloadType)
            .setParameter(3, outcome)
            .setParameter(4, rejectionReason)
            .setParameter(5, deliveredAt != null ? Timestamp.from(deliveredAt) : null)
            .setParameter(6, deliveredBy)
            .setParameter(7, deliveryId)
            .setParameter(8, signalKey)
            .executeUpdate();
    log.debugf("deliverSignalByKey('%s'): %s row(s) updated", signalKey, updated);
    return updated;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> findJobsBySignalDeliveryId(String deliveryId) {
    if (deliveryId == null || deliveryId.isBlank()) {
      return List.of();
    }
    String sql =
        "SELECT "
            + PostgresqlJobRowMapper.hydrationSelect()
            + " FROM scheduler_job c LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id"
            + " WHERE q.signal_delivery_id = ?";
    List<Object[]> rows =
        ctx.em().createNativeQuery(sql).setParameter(1, deliveryId).getResultList();
    return PostgresqlJobRowMapper.hydrateRows(rows);
  }

  private static UUID toUuid(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof UUID uuid) {
      return uuid;
    }
    return UUID.fromString(value.toString());
  }

  private static Instant toInstant(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Timestamp ts) {
      return ts.toInstant();
    }
    return null;
  }
}

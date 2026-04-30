package run.ratchet.store.postgresql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

final class PostgresqlJobRecurringAndResetOperations {

  private final PostgresqlStoreContext ctx;
  private final PostgresqlBusinessKeyReservations reservations;

  PostgresqlJobRecurringAndResetOperations(
      PostgresqlStoreContext ctx, PostgresqlBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  boolean resetRunningJob(UUID id, String nodeId) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL,
            updated_at = statement_timestamp()
        WHERE job_id = ? AND status = 'RUNNING' AND picked_by = ?
        """;
    int updated =
        ctx.em().createNativeQuery(sql).setParameter(1, id).setParameter(2, nodeId).executeUpdate();
    return updated > 0;
  }

  int resetRunningJobs(String nodeId) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL,
            updated_at = statement_timestamp()
        WHERE status = 'RUNNING' AND picked_by = ?
        """;
    return ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate();
  }

  @SuppressWarnings("unchecked")
  int cancelRecurringJobsByTag(String tag) {
    // language=PostgreSQL
    String sql =
        """
        SELECT j.job_id FROM scheduler_job j
        JOIN scheduler_job_tag t ON j.job_id = t.job_id
        WHERE t.tag = ? AND j.job_type = 'RECURRING'
          AND j.rec_status IS NOT NULL AND j.terminal_status IS NULL
        """;
    List<?> ids = ctx.em().createNativeQuery(sql).setParameter(1, tag).getResultList();
    return cancelRecurringByIds(ids);
  }

  @SuppressWarnings("unchecked")
  int cancelRecurringJobByBusinessKey(String businessKey) {
    // language=PostgreSQL
    String sql =
        """
        SELECT job_id FROM scheduler_job
        WHERE business_key = ? AND job_type = 'RECURRING'
          AND rec_status IS NOT NULL AND terminal_status IS NULL
        """;
    List<?> ids = ctx.em().createNativeQuery(sql).setParameter(1, businessKey).getResultList();
    return cancelRecurringByIds(ids);
  }

  int cancelOrphanedRecurringAnnotationJobs(Set<String> registeredIds, Instant nodeStartTime) {
    if (registeredIds.isEmpty()) {
      return 0;
    }
    List<String> idsList = new ArrayList<>(registeredIds);
    String placeholders = String.join(",", Collections.nCopies(idsList.size(), "?"));
    // language=PostgreSQL
    String sql =
        """
        SELECT job_id FROM scheduler_job
        WHERE job_type = 'RECURRING'
          AND rec_status IS NOT NULL AND terminal_status IS NULL
          AND created_at < ? AND business_key IS NOT NULL
          AND business_key NOT IN (%s)
        """
            .formatted(placeholders);
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    query.setParameter(parameter++, Timestamp.from(nodeStartTime));
    for (String registeredId : idsList) {
      query.setParameter(parameter++, registeredId);
    }
    @SuppressWarnings("unchecked")
    List<?> ids = query.getResultList();
    return cancelRecurringByIds(ids);
  }

  boolean pauseRecurring(UUID id) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job SET rec_status = 'A'
        WHERE job_id = ? AND job_type = 'RECURRING'
          AND rec_status = 'P' AND terminal_status IS NULL
        """;
    int updated = ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate();
    return updated > 0;
  }

  boolean resumeRecurring(UUID id) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job SET rec_status = 'P'
        WHERE job_id = ? AND job_type = 'RECURRING'
          AND rec_status = 'A' AND terminal_status IS NULL
        """;
    int updated = ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate();
    return updated > 0;
  }

  private int cancelRecurringByIds(List<?> idRows) {
    if (idRows.isEmpty()) {
      return 0;
    }
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job SET rec_status = NULL, terminal_status = 'CANCELED',
            terminated_at = statement_timestamp()
        WHERE job_id = ? AND job_type = 'RECURRING'
          AND rec_status IS NOT NULL AND terminal_status IS NULL
        """;
    int total = 0;
    for (Object n : idRows) {
      UUID id = PostgresqlJobRowMapper.uuidOrNull(n);
      int updated = ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate();
      if (updated > 0) {
        reservations.deleteReservationByOwner(id);
        total += updated;
      }
    }
    return total;
  }
}

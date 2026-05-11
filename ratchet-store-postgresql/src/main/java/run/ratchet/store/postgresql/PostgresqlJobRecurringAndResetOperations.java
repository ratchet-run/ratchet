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
    return ctx.timedStoreOperation(
            "reset_running_job",
            () ->
                ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, id)
                    .setParameter(2, nodeId)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
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
    return ctx.timedStoreOperation(
        "reset_running_jobs",
        () -> ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate(),
        updated -> updated > 0 ? "updated" : "miss");
  }

  int cancelRecurringJobsByTag(String tag) {
    // language=PostgreSQL
    String coldSql =
        """
        UPDATE scheduler_job j
        SET rec_status = NULL,
            terminal_status = 'CANCELED',
            terminated_at = statement_timestamp()
        FROM scheduler_job_tag t
        WHERE t.job_id = j.job_id
          AND t.tag = ?
          AND j.job_type = 'RECURRING'
          AND j.rec_status IS NOT NULL
          AND j.terminal_status IS NULL
        """;
    int cancelled =
        ctx.timedStoreOperation(
            "cancel_recurring_by_tag",
            () -> ctx.em().createNativeQuery(coldSql).setParameter(1, tag).executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss");
    if (cancelled == 0) {
      return 0;
    }
    // language=PostgreSQL
    String reservationsSql =
        """
        DELETE FROM scheduler_business_key_reservation r
        USING scheduler_job j, scheduler_job_tag t
        WHERE r.owner_job_id = j.job_id
          AND t.job_id = j.job_id
          AND t.tag = ?
          AND j.job_type = 'RECURRING'
          AND j.terminal_status = 'CANCELED'
        """;
    ctx.em().createNativeQuery(reservationsSql).setParameter(1, tag).executeUpdate();
    return cancelled;
  }

  int cancelJobsByTag(String tag) {
    // Cold UPDATE drives off the hot row's status so RUNNING jobs are skipped (their cold row is
    // not flipped to terminal until the executor finishes them). Rowcount is the return value.
    // language=PostgreSQL
    String coldSql =
        """
        UPDATE scheduler_job j
        SET terminal_status = 'CANCELED',
            terminated_at = statement_timestamp()
        FROM scheduler_job_tag t, scheduler_job_queue q
        WHERE t.job_id = j.job_id
          AND q.job_id = j.job_id
          AND t.tag = ?
          AND j.job_type <> 'RECURRING'
          AND j.terminal_status IS NULL
          AND q.status IN ('PENDING','PAUSED','WAITING')
        """;
    int cancelled =
        ctx.timedStoreOperation(
            "cancel_jobs_by_tag",
            () -> ctx.em().createNativeQuery(coldSql).setParameter(1, tag).executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss");
    if (cancelled == 0) {
      return 0;
    }
    // Hot DELETE drives off the cold rows we just flipped — guarantees the hot housekeeping
    // matches exactly what the cold UPDATE counted.
    // language=PostgreSQL
    String hotSql =
        """
        DELETE FROM scheduler_job_queue q
        USING scheduler_job j, scheduler_job_tag t
        WHERE q.job_id = j.job_id
          AND t.job_id = j.job_id
          AND t.tag = ?
          AND j.job_type <> 'RECURRING'
          AND j.terminal_status = 'CANCELED'
          AND q.status IN ('PENDING','PAUSED','WAITING')
        """;
    ctx.em().createNativeQuery(hotSql).setParameter(1, tag).executeUpdate();
    // Reservations housekeeping.
    // language=PostgreSQL
    String reservationsSql =
        """
        DELETE FROM scheduler_business_key_reservation r
        USING scheduler_job j, scheduler_job_tag t
        WHERE r.owner_job_id = j.job_id
          AND t.job_id = j.job_id
          AND t.tag = ?
          AND j.job_type <> 'RECURRING'
          AND j.terminal_status = 'CANCELED'
        """;
    ctx.em().createNativeQuery(reservationsSql).setParameter(1, tag).executeUpdate();
    return cancelled;
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

  int cancelRecurringJobsByBusinessKeys(Set<String> businessKeys) {
    if (businessKeys.isEmpty()) {
      return 0;
    }
    List<String> keysList = new ArrayList<>(businessKeys);
    String placeholders = String.join(",", Collections.nCopies(keysList.size(), "?"));
    // language=PostgreSQL
    String sql =
        """
        SELECT job_id FROM scheduler_job
        WHERE business_key IN (%s) AND job_type = 'RECURRING'
          AND rec_status IS NOT NULL AND terminal_status IS NULL
        """
            .formatted(placeholders);
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (String businessKey : keysList) {
      query.setParameter(parameter++, businessKey);
    }
    @SuppressWarnings("unchecked")
    List<?> ids = query.getResultList();
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
    return ctx.timedStoreOperation(
            "pause_recurring",
            () -> ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  boolean resumeRecurring(UUID id) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job SET rec_status = 'P'
        WHERE job_id = ? AND job_type = 'RECURRING'
          AND rec_status = 'A' AND terminal_status IS NULL
        """;
    return ctx.timedStoreOperation(
            "resume_recurring",
            () -> ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  private int cancelRecurringByIds(List<?> idRows) {
    if (idRows.isEmpty()) {
      return 0;
    }
    List<UUID> ids = new ArrayList<>(idRows.size());
    for (Object row : idRows) {
      UUID id = PostgresqlJobRowMapper.uuidOrNull(row);
      if (id != null) {
        ids.add(id);
      }
    }
    if (ids.isEmpty()) {
      return 0;
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job SET rec_status = NULL, terminal_status = 'CANCELED',
            terminated_at = statement_timestamp()
        WHERE job_id IN (%s) AND job_type = 'RECURRING'
          AND rec_status IS NOT NULL AND terminal_status IS NULL
        RETURNING job_id
        """
            .formatted(placeholders);
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (UUID id : ids) {
      query.setParameter(parameter++, id);
    }
    @SuppressWarnings("unchecked")
    List<?> cancelledRows = query.getResultList();
    if (cancelledRows.isEmpty()) {
      return 0;
    }
    List<UUID> cancelledIds = new ArrayList<>(cancelledRows.size());
    for (Object row : cancelledRows) {
      UUID id = PostgresqlJobRowMapper.uuidOrNull(row);
      if (id != null) {
        cancelledIds.add(id);
      }
    }
    reservations.deleteReservationsByOwners(cancelledIds);
    return cancelledIds.size();
  }
}

package run.ratchet.store.postgresql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class PostgresqlJobDeleteOperations {

  private final PostgresqlStoreContext ctx;
  private final PostgresqlBusinessKeyReservations reservations;

  PostgresqlJobDeleteOperations(
      PostgresqlStoreContext ctx, PostgresqlBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  void delete(long id) {
    reservations.deleteReservationByOwner(id);
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_job WHERE job_id = ?";
    ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate();
  }

  int deleteJobsByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    reservations.deleteReservationsByOwners(ids);
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")";
    Query jobDelete = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (Long id : ids) {
      jobDelete.setParameter(parameter++, id);
    }
    return jobDelete.executeUpdate();
  }

  int deleteDlqOlderThan(Instant cutoff) {
    // language=PostgreSQL
    String selectSql =
        """
        SELECT job_id FROM scheduler_job
        WHERE terminal_status = 'FAILED' AND total_attempts >= max_retries
          AND terminated_at < ?
        """;
    @SuppressWarnings("unchecked")
    List<Number> idRows =
        ctx.em()
            .createNativeQuery(selectSql)
            .setParameter(1, Timestamp.from(cutoff))
            .getResultList();
    if (idRows.isEmpty()) {
      return 0;
    }
    List<Long> ids = new ArrayList<>(idRows.size());
    for (Number n : idRows) {
      ids.add(n.longValue());
    }
    reservations.deleteReservationsByOwners(idRows);
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=PostgreSQL
    String deleteSql = "DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")";
    Query jobDelete = ctx.em().createNativeQuery(deleteSql);
    int parameter = 1;
    for (Long id : ids) {
      jobDelete.setParameter(parameter++, id);
    }
    return jobDelete.executeUpdate();
  }

  int resetOrphanJobs(Duration grace) {
    long graceSeconds = grace.toSeconds();
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING',
            picked_by = NULL, picked_at = NULL,
            updated_at = statement_timestamp()
        WHERE status = 'RUNNING'
          AND picked_by NOT IN (
            SELECT node_id FROM scheduler_node
            WHERE heartbeat_ts > statement_timestamp() - ? * interval '1 second'
          )
          AND extract(epoch from (statement_timestamp() - picked_at))::bigint >= ?
        """;
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, graceSeconds)
        .setParameter(2, graceSeconds)
        .executeUpdate();
  }

  int resetOrphanJobsForNode(String nodeId) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING',
            picked_by = NULL, picked_at = NULL,
            updated_at = statement_timestamp()
        WHERE status = 'RUNNING' AND picked_by = ?
        """;
    return ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate();
  }
}

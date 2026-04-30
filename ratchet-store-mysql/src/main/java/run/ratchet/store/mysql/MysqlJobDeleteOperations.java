package run.ratchet.store.mysql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

final class MysqlJobDeleteOperations {

  private final MysqlStoreContext ctx;
  private final MysqlBusinessKeyReservations reservations;

  MysqlJobDeleteOperations(MysqlStoreContext ctx, MysqlBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  void delete(UUID id) {
    reservations.deleteReservationByOwner(id);
    // language=MySQL
    String sql = "DELETE FROM scheduler_job WHERE job_id = ?";
    ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate();
  }

  int deleteJobsByIds(List<UUID> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=MySQL
    String bkresSql =
        "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN ("
            + placeholders
            + ")";
    Query bkresDelete = ctx.em().createNativeQuery(bkresSql);
    int parameter = 1;
    for (UUID id : ids) {
      bkresDelete.setParameter(parameter++, id);
    }
    bkresDelete.executeUpdate();
    // language=MySQL
    String jobSql = "DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")";
    Query jobDelete = ctx.em().createNativeQuery(jobSql);
    parameter = 1;
    for (UUID id : ids) {
      jobDelete.setParameter(parameter++, id);
    }
    return jobDelete.executeUpdate();
  }

  int deleteDlqOlderThan(Instant cutoff) {
    // language=MySQL
    String selectSql =
        """
        SELECT job_id FROM scheduler_job
        WHERE terminal_status = 'FAILED' AND total_attempts >= max_retries
          AND terminated_at < ?
        """;
    @SuppressWarnings("unchecked")
    List<?> idRows =
        ctx.em()
            .createNativeQuery(selectSql)
            .setParameter(1, Timestamp.from(cutoff))
            .getResultList();
    if (idRows.isEmpty()) {
      return 0;
    }
    List<UUID> ids = new ArrayList<>(idRows.size());
    for (Object n : idRows) {
      ids.add(MysqlJobRowMapper.uuidOrNull(n));
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=MySQL
    String bkresSql =
        "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN ("
            + placeholders
            + ")";
    Query bkresDelete = ctx.em().createNativeQuery(bkresSql);
    int parameter = 1;
    for (UUID id : ids) {
      bkresDelete.setParameter(parameter++, id);
    }
    bkresDelete.executeUpdate();
    // language=MySQL
    String jobSql = "DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")";
    Query jobDelete = ctx.em().createNativeQuery(jobSql);
    parameter = 1;
    for (UUID id : ids) {
      jobDelete.setParameter(parameter++, id);
    }
    return jobDelete.executeUpdate();
  }

  int resetOrphanJobs(Duration grace) {
    long graceSec = grace.toSeconds();
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = NOW(3)
        WHERE status = 'RUNNING'
          AND picked_by NOT IN (
            SELECT node_id FROM scheduler_node
            WHERE TIMESTAMPDIFF(SECOND, heartbeat_ts, NOW(3)) <= ?
          )
          AND TIMESTAMPDIFF(SECOND, picked_at, NOW(3)) >= ?
        """;
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, graceSec)
        .setParameter(2, graceSec)
        .executeUpdate();
  }

  int resetOrphanJobsForNode(String nodeId) {
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = NOW(3)
        WHERE status = 'RUNNING' AND picked_by = ?
        """;
    return ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate();
  }
}

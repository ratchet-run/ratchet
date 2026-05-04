package run.ratchet.store.postgresql;

import run.ratchet.api.JobStatus;
import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;

final class PostgresqlJobStatusTransitions {

  private static final Logger log = Logger.getLogger(PostgresqlJobStatusTransitions.class);

  private final PostgresqlStoreContext ctx;

  PostgresqlJobStatusTransitions(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  boolean tryPickUpJob(UUID id, String nodeId) {
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'RUNNING', picked_by = ?, picked_at = statement_timestamp(),
            updated_at = statement_timestamp()
        WHERE job_id = ? AND status = 'PENDING'
        """;
    int updated =
        ctx.em().createNativeQuery(sql).setParameter(1, nodeId).setParameter(2, id).executeUpdate();
    return updated > 0;
  }

  boolean transitionToPaused(UUID id, JobStatus expected) {
    if (expected == JobStatus.PAUSED) {
      throw new IllegalArgumentException("transitionToPaused expects expected != PAUSED");
    }
    if (expected == JobStatus.WAITING) {
      log.debugf("transitionToPaused(%s, WAITING) is a no-op — waiting jobs cannot be paused", id);
      return false;
    }
    if (!PostgresqlJobRowMapper.isLiveStatus(expected)) {
      log.debugf(
          "transitionToPaused(%s, %s) is a no-op post hot/cold-split — terminal jobs cannot be paused",
          id, expected);
      return false;
    }
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PAUSED', paused_from_status = ?, updated_at = statement_timestamp()
        WHERE job_id = ? AND status = ?
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, expected.name())
            .setParameter(2, id)
            .setParameter(3, expected.name())
            .executeUpdate();
    return updated > 0;
  }

  boolean transitionFromPaused(UUID id, JobStatus target) {
    if (!PostgresqlJobRowMapper.isLiveStatus(target)
        || target == JobStatus.PAUSED
        || target == JobStatus.WAITING) {
      throw new IllegalArgumentException(
          "transitionFromPaused expects a non-PAUSED live status; got " + target);
    }
    // language=PostgreSQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = ?, paused_from_status = NULL, updated_at = statement_timestamp()
        WHERE job_id = ? AND status = 'PAUSED'
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, target.name())
            .setParameter(2, id)
            .executeUpdate();
    return updated > 0;
  }

  @SuppressWarnings("unchecked")
  JobStatus transitionFromPausedAtomic(UUID id) {
    // language=PostgreSQL
    String selectSql =
        """
        SELECT paused_from_status FROM scheduler_job_queue
        WHERE job_id = ? AND status = 'PAUSED'
        FOR UPDATE
        """;
    List<?> results = ctx.em().createNativeQuery(selectSql).setParameter(1, id).getResultList();
    if (results.isEmpty()) {
      return null;
    }
    String pausedFrom = (String) results.get(0);
    JobStatus target = pausedFrom != null ? JobStatus.valueOf(pausedFrom) : JobStatus.PENDING;
    // language=PostgreSQL
    String updateSql =
        """
        UPDATE scheduler_job_queue
        SET status = ?, paused_from_status = NULL, updated_at = statement_timestamp()
        WHERE job_id = ? AND status = 'PAUSED'
        """;
    int updated =
        ctx.em()
            .createNativeQuery(updateSql)
            .setParameter(1, target.name())
            .setParameter(2, id)
            .executeUpdate();
    return updated > 0 ? target : null;
  }
}

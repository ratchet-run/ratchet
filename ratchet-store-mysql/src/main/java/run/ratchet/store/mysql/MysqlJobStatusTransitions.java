package run.ratchet.store.mysql;

import run.ratchet.store.entity.JobStatus;
import java.util.List;
import org.jboss.logging.Logger;

final class MysqlJobStatusTransitions {

  private static final Logger log = Logger.getLogger(MysqlJobStatusTransitions.class);

  private final MysqlStoreContext ctx;

  MysqlJobStatusTransitions(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  boolean tryPickUpJob(long id, String nodeId) {
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'RUNNING', picked_by = ?, picked_at = NOW(3), updated_at = NOW(3)
        WHERE job_id = ? AND status = 'PENDING'
        """;
    return ctx.timedStoreOperation(
            "pickup_job",
            () ->
                ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, nodeId)
                    .setParameter(2, id)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  boolean transitionToPaused(long id, JobStatus expected) {
    if (expected == JobStatus.PAUSED) {
      throw new IllegalArgumentException("transitionToPaused expects expected != PAUSED");
    }
    if (!MysqlJobRowMapper.isLiveStatus(expected)) {
      log.debugf(
          "transitionToPaused(%d, %s) is a no-op post hot/cold-split — terminal jobs cannot be paused",
          id, expected);
      return false;
    }
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PAUSED', paused_from_status = ?, updated_at = NOW(3)
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

  boolean transitionFromPaused(long id, JobStatus target) {
    if (!MysqlJobRowMapper.isLiveStatus(target) || target == JobStatus.PAUSED) {
      throw new IllegalArgumentException(
          "transitionFromPaused expects a non-PAUSED live status; got " + target);
    }
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = ?, paused_from_status = NULL, updated_at = NOW(3)
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

  JobStatus transitionFromPausedAtomic(long id) {
    // language=MySQL
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
    // language=MySQL
    String updateSql =
        """
        UPDATE scheduler_job_queue
        SET status = ?, paused_from_status = NULL, updated_at = NOW(3)
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

package run.ratchet.store.postgresql;

import run.ratchet.store.entity.JobStatus;
import java.util.List;
import org.jboss.logging.Logger;

final class PostgresqlJobStatusTransitions {

  private static final Logger log = Logger.getLogger(PostgresqlJobStatusTransitions.class);

  private final PostgresqlStoreContext ctx;

  PostgresqlJobStatusTransitions(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  boolean tryPickUpJob(long id, String nodeId) {
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job_queue SET status = 'RUNNING', picked_by = ?, "
                    + "picked_at = statement_timestamp(), updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'PENDING'")
            .setParameter(1, nodeId)
            .setParameter(2, id)
            .executeUpdate();
    return updated > 0;
  }

  boolean transitionToPaused(long id, JobStatus expected) {
    if (expected == JobStatus.PAUSED) {
      throw new IllegalArgumentException("transitionToPaused expects expected != PAUSED");
    }
    if (!PostgresqlJobRowMapper.isLiveStatus(expected)) {
      log.debugf(
          "transitionToPaused(%d, %s) is a no-op post hot/cold-split — terminal jobs cannot be paused",
          id, expected);
      return false;
    }
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job_queue SET status = 'PAUSED', "
                    + "paused_from_status = ?, updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = ?")
            .setParameter(1, expected.name())
            .setParameter(2, id)
            .setParameter(3, expected.name())
            .executeUpdate();
    return updated > 0;
  }

  boolean transitionFromPaused(long id, JobStatus target) {
    if (!PostgresqlJobRowMapper.isLiveStatus(target) || target == JobStatus.PAUSED) {
      throw new IllegalArgumentException(
          "transitionFromPaused expects a non-PAUSED live status; got " + target);
    }
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job_queue SET status = ?, "
                    + "paused_from_status = NULL, updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'PAUSED'")
            .setParameter(1, target.name())
            .setParameter(2, id)
            .executeUpdate();
    return updated > 0;
  }

  @SuppressWarnings("unchecked")
  JobStatus transitionFromPausedAtomic(long id) {
    List<?> results =
        ctx.em()
            .createNativeQuery(
                "SELECT paused_from_status FROM scheduler_job_queue "
                    + "WHERE job_id = ? AND status = 'PAUSED' FOR UPDATE")
            .setParameter(1, id)
            .getResultList();
    if (results.isEmpty()) {
      return null;
    }
    String pausedFrom = (String) results.get(0);
    JobStatus target = pausedFrom != null ? JobStatus.valueOf(pausedFrom) : JobStatus.PENDING;
    int updated =
        ctx.em()
            .createNativeQuery(
                "UPDATE scheduler_job_queue SET status = ?, "
                    + "paused_from_status = NULL, updated_at = statement_timestamp() "
                    + "WHERE job_id = ? AND status = 'PAUSED'")
            .setParameter(1, target.name())
            .setParameter(2, id)
            .executeUpdate();
    return updated > 0 ? target : null;
  }
}

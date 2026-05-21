package run.ratchet.store.mysql;

import java.util.UUID;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;

// Reset + cancel-by-tag operations against the executable scheduler_job table. Recurring-master
// pause / resume / cancel / orphan-cleanup live on MysqlRecurringJobOperations against
// scheduler_recurring_job.
final class MysqlJobRecurringAndResetOperations {

  private final MysqlStoreContext ctx;

  MysqlJobRecurringAndResetOperations(
      MysqlStoreContext ctx, MysqlBusinessKeyReservations reservations) {
    this.ctx = ctx;
  }

  boolean resetRunningJob(UUID id, String nodeId) {
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = NOW(3)
        WHERE job_id = ? AND status = 'RUNNING' AND picked_by = ?
        """;
    return ctx.timedStoreOperation(
            "reset_running_job",
            () ->
                ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, UuidByteArrayConverter.toBytes(id))
                    .setParameter(2, nodeId)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  int resetRunningJobs(String nodeId) {
    // language=MySQL
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = NOW(3)
        WHERE status = 'RUNNING' AND picked_by = ?
        """;
    return ctx.timedStoreOperation(
        "reset_running_jobs",
        () -> ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate(),
        updated -> updated > 0 ? "updated" : "miss");
  }

  int cancelJobsByTag(String tag) {
    // language=MySQL
    String coldSql =
        """
        UPDATE scheduler_job j
          JOIN scheduler_job_tag t ON t.job_id = j.job_id
          JOIN scheduler_job_queue q ON q.job_id = j.job_id
        SET j.terminal_status = 'CANCELED',
            j.terminated_at = NOW(3)
        WHERE t.tag = ?
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
    // language=MySQL
    String hotSql =
        """
        DELETE q FROM scheduler_job_queue q
        WHERE q.status IN ('PENDING','PAUSED','WAITING')
          AND q.job_id IN (
            SELECT j.job_id FROM scheduler_job j
              JOIN scheduler_job_tag t ON t.job_id = j.job_id
            WHERE t.tag = ?
              AND j.job_type <> 'RECURRING'
              AND j.terminal_status = 'CANCELED'
          )
        """;
    ctx.timedStoreOperation(
        "cancel_jobs_by_tag_hot_delete",
        () -> ctx.em().createNativeQuery(hotSql).setParameter(1, tag).executeUpdate(),
        deleted -> deleted > 0 ? "updated" : "miss");
    // language=MySQL
    String reservationsSql =
        """
        DELETE r FROM scheduler_business_key_reservation r
        WHERE r.owner_job_id IN (
          SELECT j.job_id FROM scheduler_job j
            JOIN scheduler_job_tag t ON t.job_id = j.job_id
          WHERE t.tag = ?
            AND j.job_type <> 'RECURRING'
            AND j.terminal_status = 'CANCELED'
        )
        """;
    ctx.timedStoreOperation(
        "cancel_jobs_by_tag_reservations",
        () -> ctx.em().createNativeQuery(reservationsSql).setParameter(1, tag).executeUpdate(),
        deleted -> deleted > 0 ? "updated" : "miss");
    return cancelled;
  }
}

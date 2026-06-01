/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.store.postgresql;

import java.util.UUID;

// Reset + cancel-by-tag operations against the executable scheduler_job table. Recurring-master
// pause / resume / cancel / orphan-cleanup live on PostgresqlRecurringJobOperations against
// scheduler_recurring_job.
final class PostgresqlJobRecurringAndResetOperations {

  private final PostgresqlStoreContext ctx;

  PostgresqlJobRecurringAndResetOperations(
      PostgresqlStoreContext ctx, PostgresqlBusinessKeyReservations reservations) {
    this.ctx = ctx;
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

  int cancelJobsByTag(String tag) {
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
    ctx.timedStoreOperation(
        "cancel_jobs_by_tag_hot",
        () -> ctx.em().createNativeQuery(hotSql).setParameter(1, tag).executeUpdate(),
        updated -> updated > 0 ? "updated" : "miss");
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
    ctx.timedStoreOperation(
        "cancel_jobs_by_tag_reservations",
        () -> ctx.em().createNativeQuery(reservationsSql).setParameter(1, tag).executeUpdate(),
        updated -> updated > 0 ? "updated" : "miss");
    return cancelled;
  }
}

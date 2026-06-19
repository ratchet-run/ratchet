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
package run.ratchet.store.oracle;

import java.util.UUID;
import run.ratchet.store.oracle.converter.UuidRawConverter;

// Reset + cancel-by-tag operations against the executable scheduler_job table. Recurring-master
// pause / resume / cancel / orphan-cleanup live on OracleRecurringJobOperations against
// scheduler_recurring_job.
final class OracleJobRecurringAndResetOperations {

  private final OracleStoreContext ctx;

  OracleJobRecurringAndResetOperations(
      OracleStoreContext ctx, OracleBusinessKeyReservations reservations) {
    this.ctx = ctx;
  }

  boolean resetRunningJob(UUID id, String nodeId) {
    // language=Oracle
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
        WHERE job_id = ? AND status = 'RUNNING' AND picked_by = ?
        """;
    return ctx.timedStoreOperation(
            "reset_running_job",
            () ->
                ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, UuidRawConverter.toBytes(id))
                    .setParameter(2, nodeId)
                    .executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss")
        > 0;
  }

  int resetRunningJobs(String nodeId) {
    // language=Oracle
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
        WHERE status = 'RUNNING' AND picked_by = ?
        """;
    return ctx.timedStoreOperation(
        "reset_running_jobs",
        () -> ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate(),
        updated -> updated > 0 ? "updated" : "miss");
  }

  int cancelJobsByTag(String tag) {
    // Oracle has no UPDATE..JOIN; the tag and live-status joins become EXISTS predicates.
    // language=Oracle
    String coldSql =
        "UPDATE scheduler_job j SET j.terminal_status = 'CANCELED',"
            + " j.terminated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)"
            + " WHERE j.job_type <> 'RECURRING' AND j.terminal_status IS NULL"
            + " AND EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = j.job_id AND t.tag = ?)"
            + " AND EXISTS (SELECT 1 FROM scheduler_job_queue q WHERE q.job_id = j.job_id"
            + " AND q.status IN ('PENDING','PAUSED','WAITING'))";
    int cancelled =
        ctx.timedStoreOperation(
            "cancel_jobs_by_tag",
            () -> ctx.em().createNativeQuery(coldSql).setParameter(1, tag).executeUpdate(),
            updated -> updated > 0 ? "updated" : "miss");
    if (cancelled == 0) {
      return 0;
    }
    // language=Oracle
    String hotSql =
        """
        DELETE FROM scheduler_job_queue q
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
    // language=Oracle
    String reservationsSql =
        """
        DELETE FROM scheduler_business_key_reservation r
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

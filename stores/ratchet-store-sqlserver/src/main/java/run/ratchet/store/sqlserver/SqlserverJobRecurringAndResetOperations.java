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
package run.ratchet.store.sqlserver;

import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

// Reset + cancel-by-tag operations against the executable scheduler_job table. Recurring-master
// pause / resume / cancel / orphan-cleanup live on SqlserverRecurringJobOperations against
// scheduler_recurring_job.
final class SqlserverJobRecurringAndResetOperations {

  private final SqlserverStoreContext ctx;

  SqlserverJobRecurringAndResetOperations(
      SqlserverStoreContext ctx, SqlserverBusinessKeyReservations reservations) {
    this.ctx = ctx;
  }

  boolean resetRunningJob(UUID id, String nodeId) {
    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL,
            updated_at = SYSUTCDATETIME()
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
    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL,
            updated_at = SYSUTCDATETIME()
        WHERE status = 'RUNNING' AND picked_by = ?
        """;
    return ctx.timedStoreOperation(
        "reset_running_jobs",
        () -> ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate(),
        updated -> updated > 0 ? "updated" : "miss");
  }

  int cancelJobsByTag(String tag) {
    return ctx.timedStoreOperation(
        "cancel_jobs_by_tag",
        () -> doCancelJobsByTag(tag),
        cancelled -> cancelled > 0 ? "updated" : "miss");
  }

  private int doCancelJobsByTag(String tag) {
    // Lock the candidate hot rows first, inside the method transaction, before touching the cold
    // row. SQL Server's UPDATE ... FROM does NOT lock the FROM-referenced rows, so a cold UPDATE
    // alone leaves the queue row free for a concurrent poller to claim into RUNNING between the
    // cancel and the hot DELETE — the DELETE (which only matches PENDING/PAUSED/WAITING) would then
    // skip the now-RUNNING row, stranding it forever while the reservation is still freed. Holding
    // UPDLOCK on each queue row makes the claim path's READPAST step past it, so a
    // claim cannot interleave. Mirrors the single-job cancel gate in
    // SqlserverJobTerminalOperations.
    // language=SQL Server
    String lockSql =
        """
        SELECT q.job_id
        FROM scheduler_job_queue q WITH (UPDLOCK, ROWLOCK)
        JOIN scheduler_job j ON j.job_id = q.job_id
        JOIN scheduler_job_tag t ON t.job_id = q.job_id
        WHERE t.tag = ?
          AND j.job_type <> 'RECURRING'
          AND j.terminal_status IS NULL
          AND q.status IN ('PENDING','PAUSED','WAITING')
        """;
    @SuppressWarnings("unchecked")
    List<Object> lockedRows =
        ctx.em().createNativeQuery(lockSql).setParameter(1, tag).getResultList();
    if (lockedRows.isEmpty()) {
      return 0;
    }
    List<UUID> ids = new ArrayList<>(lockedRows.size());
    for (Object row : lockedRows) {
      ids.add(SqlserverJobRowMapper.uuidOrNull(row));
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));

    // language=SQL Server
    String coldSql =
        """
        UPDATE scheduler_job
        SET terminal_status = 'CANCELED',
            terminated_at = SYSUTCDATETIME()
        WHERE job_id IN (%s)
          AND terminal_status IS NULL
        """
            .formatted(placeholders);
    int cancelled = bindIds(coldSql, ids).executeUpdate();

    // language=SQL Server
    String hotSql = "DELETE FROM scheduler_job_queue WHERE job_id IN (%s)".formatted(placeholders);
    int hotDeleted = bindIds(hotSql, ids).executeUpdate();
    if (hotDeleted != cancelled) {
      throw new IllegalStateException(
          "cancel-by-tag canceled "
              + cancelled
              + " cold rows but removed "
              + hotDeleted
              + " hot rows for tag "
              + tag);
    }

    // language=SQL Server
    String reservationsSql =
        "DELETE FROM scheduler_business_key_reservation WHERE owner_job_id IN (%s)"
            .formatted(placeholders);
    bindIds(reservationsSql, ids).executeUpdate();
    return cancelled;
  }

  private Query bindIds(String sql, List<UUID> ids) {
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (UUID id : ids) {
      query.setParameter(parameter++, UuidByteArrayConverter.toBytes(id));
    }
    return query;
  }
}

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

import static run.ratchet.store.entity.JobExecutionEntity.SHUTDOWN_RELEASE_ERROR_MESSAGE;
import static run.ratchet.store.sqlserver.SqlserverBusinessKeyReservations.DELETE_RESERVATIONS_CHUNK_SIZE;

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

  private final SqlserverBusinessKeyReservations reservations;

  SqlserverJobRecurringAndResetOperations(
      SqlserverStoreContext ctx, SqlserverBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  boolean resetRunningJob(UUID id, String nodeId, long expectedClaimSeq) {
    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL,
            updated_at = SYSUTCDATETIME(), version = version + 1
        WHERE job_id = ? AND status = 'RUNNING' AND picked_by = ? AND claim_seq = ?
        """;
    return ctx.timedStoreOperation(
            "reset_running_job",
            () ->
                ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, UuidByteArrayConverter.toBytes(id))
                    .setParameter(2, nodeId)
                    .setParameter(3, expectedClaimSeq)
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
            updated_at = SYSUTCDATETIME(), version = version + 1
        WHERE status = 'RUNNING' AND picked_by = ?
        """;
    return ctx.timedStoreOperation(
        "reset_running_jobs",
        () -> {
          ctx.em()
              .createNativeQuery(
                  """
                  UPDATE scheduler_job_execution
                  SET status = 'FAILED', ended_at = SYSUTCDATETIME(),
                      error_message = ?
                  WHERE status = 'RUNNING' AND job_id IN (
                    SELECT job_id FROM scheduler_job_queue
                    WHERE status = 'RUNNING' AND picked_by = ?
                  )
                  """)
              .setParameter(1, SHUTDOWN_RELEASE_ERROR_MESSAGE)
              .setParameter(2, nodeId)
              .executeUpdate();
          return ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate();
        },
        updated -> updated > 0 ? "updated" : "miss");
  }

  int cancelJobsByTag(String tag) {
    return ctx.timedStoreOperation(
        "cancel_jobs_by_tag",
        () -> doCancelJobsByTag(tag),
        cancelled -> cancelled > 0 ? "updated" : "miss");
  }

  private int doCancelJobsByTag(String tag) {
    // UPDATE ... FROM does not lock the queue row. Lock queue rows before the cold update so a
    // poller cannot claim one between the cold update and hot delete and strand it RUNNING. The
    // claim path's READPAST skips these locked rows. Read candidates without locks, then lock them
    // by primary key so cancellation locks only the queue rows of the jobs it cancels, whatever
    // plan SQL Server picks for the join. Lock ids in job_id byte order, the order of the other
    // multi-row queue writes, and lock every chunk before the first write. Chunks keep statements
    // under SQL Server's 2,100-parameter limit. The tag parameter and column are both NVARCHAR, so
    // the read seeks idx_job_tag_tag_job. The writes seek the primary key, as the batch cancel's
    // do: a scan would wait on rows that an open cancel of another tag has written. SQL Server
    // rejects FORCESEEK on the target of an UPDATE or DELETE, so each write puts the hint on an
    // alias.
    // language=SQL Server
    String candidatesSql =
        """
        SELECT q.job_id
        FROM scheduler_job_tag t
        JOIN scheduler_job_queue q ON q.job_id = t.job_id
        JOIN scheduler_job j ON j.job_id = t.job_id
        WHERE t.tag = ?
          AND j.job_type <> 'RECURRING'
          AND j.terminal_status IS NULL
          AND q.status IN ('PENDING','PAUSED','WAITING')
        ORDER BY q.job_id
        """;
    List<?> candidateRows =
        ctx.em().createNativeQuery(candidatesSql).setParameter(1, tag).getResultList();
    List<UUID> candidates = new ArrayList<>(candidateRows.size());
    for (Object row : candidateRows) {
      candidates.add(SqlserverJobRowMapper.uuidOrNull(row));
    }
    if (candidates.isEmpty()) {
      return 0;
    }

    List<UUID> ids = new ArrayList<>(candidates.size());
    for (int start = 0; start < candidates.size(); start += DELETE_RESERVATIONS_CHUNK_SIZE) {
      List<UUID> chunk =
          candidates.subList(
              start, Math.min(start + DELETE_RESERVATIONS_CHUNK_SIZE, candidates.size()));
      String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
      // language=SQL Server
      String lockSql =
          """
          SELECT q.job_id
          FROM scheduler_job_queue q WITH (UPDLOCK, ROWLOCK, FORCESEEK(pk_scheduler_job_queue(job_id)))
          JOIN scheduler_job j ON j.job_id = q.job_id
          WHERE q.job_id IN (%s)
            AND q.status IN ('PENDING','PAUSED','WAITING')
            AND j.job_type <> 'RECURRING'
            AND j.terminal_status IS NULL
          ORDER BY q.job_id
          """
              .formatted(placeholders);
      for (Object row : bindIds(lockSql, chunk).getResultList()) {
        ids.add(SqlserverJobRowMapper.uuidOrNull(row));
      }
    }
    if (ids.isEmpty()) {
      return 0;
    }

    int cancelled = 0;
    for (int start = 0; start < ids.size(); start += DELETE_RESERVATIONS_CHUNK_SIZE) {
      List<UUID> chunk =
          ids.subList(start, Math.min(start + DELETE_RESERVATIONS_CHUNK_SIZE, ids.size()));
      String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
      // language=SQL Server
      String coldSql =
          """
          UPDATE c
          SET row_version = c.row_version + 1, terminal_status = 'CANCELED',
              terminated_at = SYSUTCDATETIME()
          FROM scheduler_job c WITH (ROWLOCK, FORCESEEK, INDEX(pk_scheduler_job))
          WHERE c.job_id IN (%s)
            AND c.terminal_status IS NULL
          """
              .formatted(placeholders);
      int updated = bindIds(coldSql, chunk).executeUpdate();

      // language=SQL Server
      String hotSql =
          """
          DELETE q
          FROM scheduler_job_queue q WITH (ROWLOCK, FORCESEEK, INDEX(pk_scheduler_job_queue))
          WHERE q.job_id IN (%s)
          """
              .formatted(placeholders);
      int deleted = bindIds(hotSql, chunk).executeUpdate();
      if (updated != deleted) {
        throw new IllegalStateException(
            "cancel-by-tag canceled "
                + updated
                + " cold rows but removed "
                + deleted
                + " hot rows for tag "
                + tag);
      }
      cancelled += updated;
    }

    reservations.deleteReservationsByOwners(ids);
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

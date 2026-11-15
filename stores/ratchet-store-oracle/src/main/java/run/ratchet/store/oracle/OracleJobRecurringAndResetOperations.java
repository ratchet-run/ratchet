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

import static run.ratchet.store.entity.JobExecutionEntity.SHUTDOWN_RELEASE_ERROR_MESSAGE;
import static run.ratchet.store.oracle.OracleBusinessKeyReservations.DELETE_RESERVATIONS_CHUNK_SIZE;

import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import run.ratchet.store.oracle.converter.UuidRawConverter;

// Reset + cancel-by-tag operations against the executable scheduler_job table. Recurring-master
// pause / resume / cancel / orphan-cleanup live on OracleRecurringJobOperations against
// scheduler_recurring_job.
final class OracleJobRecurringAndResetOperations {

  private final OracleStoreContext ctx;
  private final OracleBusinessKeyReservations reservations;

  OracleJobRecurringAndResetOperations(
      OracleStoreContext ctx, OracleBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  boolean resetRunningJob(UUID id, String nodeId, long expectedClaimSeq) {
    // language=Oracle
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
        WHERE job_id = ? AND status = 'RUNNING' AND picked_by = ? AND claim_seq = ?
        """;
    return ctx.timedStoreOperation(
            "reset_running_job",
            () ->
                ctx.em()
                    .createNativeQuery(sql)
                    .setParameter(1, UuidRawConverter.toBytes(id))
                    .setParameter(2, nodeId)
                    .setParameter(3, expectedClaimSeq)
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
        () -> {
          ctx.em()
              .createNativeQuery(
                  """
                  UPDATE scheduler_job_execution
                  SET status = 'FAILED',
                      ended_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP),
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
    String candidatesSql =
        """
        SELECT q.job_id FROM scheduler_job_queue q
        JOIN scheduler_job j ON j.job_id = q.job_id
        JOIN scheduler_job_tag t ON t.job_id = q.job_id
        WHERE t.tag = ? AND j.job_type <> 'RECURRING' AND j.terminal_status IS NULL
          AND q.status IN ('PENDING','PAUSED','WAITING')
        ORDER BY q.job_id
        """;
    List<UUID> candidates = new ArrayList<>();
    for (Object row :
        ctx.em().createNativeQuery(candidatesSql).setParameter(1, tag).getResultList()) {
      candidates.add(OracleJobRowMapper.uuidOrNull(row));
    }
    // Lock queue rows first so a poller cannot claim them between the cold update and hot delete.
    // Every queue chunk is locked before any job or reservation is changed.
    List<UUID> ids = new ArrayList<>();
    for (int start = 0; start < candidates.size(); start += DELETE_RESERVATIONS_CHUNK_SIZE) {
      List<UUID> chunk =
          candidates.subList(
              start, Math.min(start + DELETE_RESERVATIONS_CHUNK_SIZE, candidates.size()));
      String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
      String lockSql =
          """
          SELECT job_id FROM scheduler_job_queue
          WHERE job_id IN (%s) AND status IN ('PENDING','PAUSED','WAITING')
          ORDER BY job_id FOR UPDATE
          """
              .formatted(placeholders);
      for (Object row : bindIds(lockSql, chunk).getResultList()) {
        ids.add(OracleJobRowMapper.uuidOrNull(row));
      }
    }
    int cancelled = 0;
    for (int start = 0; start < ids.size(); start += DELETE_RESERVATIONS_CHUNK_SIZE) {
      List<UUID> chunk =
          ids.subList(start, Math.min(start + DELETE_RESERVATIONS_CHUNK_SIZE, ids.size()));
      String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
      String coldSql =
          """
          UPDATE scheduler_job
          SET row_version = row_version + 1, terminal_status = 'CANCELED',
              terminated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          WHERE job_id IN (%s) AND terminal_status IS NULL
          """
              .formatted(placeholders);
      int updated = bindIds(coldSql, chunk).executeUpdate();
      String hotSql =
          "DELETE FROM scheduler_job_queue WHERE job_id IN (%s)".formatted(placeholders);
      int deleted =
          ctx.timedStoreOperation(
              "cancel_jobs_by_tag_hot_delete",
              () -> bindIds(hotSql, chunk).executeUpdate(),
              count -> count > 0 ? "updated" : "miss");
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
    ctx.timedStoreOperation(
        "cancel_jobs_by_tag_reservations",
        () -> {
          reservations.deleteReservationsByOwners(ids);
          return ids.size();
        },
        count -> count > 0 ? "updated" : "miss");
    return cancelled;
  }

  private Query bindIds(String sql, List<UUID> ids) {
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (UUID id : ids) query.setParameter(parameter++, UuidRawConverter.toBytes(id));
    return query;
  }
}

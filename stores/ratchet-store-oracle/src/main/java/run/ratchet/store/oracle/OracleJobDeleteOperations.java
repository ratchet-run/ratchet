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

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import run.ratchet.store.oracle.converter.UuidRawConverter;

final class OracleJobDeleteOperations {

  private final OracleStoreContext ctx;
  private final OracleBusinessKeyReservations reservations;

  OracleJobDeleteOperations(OracleStoreContext ctx, OracleBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  void delete(UUID id) {
    try {
      reservations.deleteReservationByOwner(id);
      // language=Oracle
      String sql = "DELETE FROM scheduler_job WHERE job_id = ?";
      ctx.em().createNativeQuery(sql).setParameter(1, UuidRawConverter.toBytes(id)).executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete job", e);
    }
  }

  int deleteJobsByIds(List<UUID> ids) {
    try {
      if (ids.isEmpty()) {
        return 0;
      }
      reservations.deleteReservationsByOwners(ids);
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=Oracle
      String jobSql = "DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")";
      Query jobDelete = ctx.em().createNativeQuery(jobSql);
      bindUuidList(jobDelete, ids, 1);
      return jobDelete.executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete jobs by ids", e);
    }
  }

  int deleteTerminalJobsByIds(List<UUID> ids) {
    try {
      if (ids.isEmpty()) {
        return 0;
      }
      reservations.deleteReservationsByOwners(ids);
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=Oracle
      String jobSql =
          "DELETE FROM scheduler_job WHERE job_id IN ("
              + placeholders
              + ") AND terminal_status IS NOT NULL";
      Query jobDelete = ctx.em().createNativeQuery(jobSql);
      bindUuidList(jobDelete, ids, 1);
      return jobDelete.executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete terminal jobs by ids", e);
    }
  }

  int deleteDlqOlderThan(Instant cutoff) {
    /*
     * Transaction contract: this method is reached through OracleJobStoreImpl's REQUIRED boundary.
     * The reservation cleanup and cold-row delete must commit or roll back together.
     */
    try {
      // language=Oracle
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
              .setParameter(1, OracleTimestamps.microTimestamp(cutoff))
              .getResultList();
      if (idRows.isEmpty()) {
        return 0;
      }
      List<UUID> ids = new ArrayList<>(idRows.size());
      for (Object n : idRows) {
        ids.add(OracleJobRowMapper.uuidOrNull(n));
      }
      reservations.deleteReservationsByOwners(ids);
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=Oracle
      String jobSql = "DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")";
      Query jobDelete = ctx.em().createNativeQuery(jobSql);
      bindUuidList(jobDelete, ids, 1);
      return jobDelete.executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete dlq older than", e);
    }
  }

  private static void bindUuidList(Query query, List<UUID> ids, int startParam) {
    int parameter = startParam;
    for (UUID id : ids) {
      query.setParameter(parameter++, UuidRawConverter.toBytes(id));
    }
  }

  int resetOrphanJobs(Duration grace) {
    try {
      long graceSec = grace.toSeconds();
      // language=Oracle
      String sql =
          """
          UPDATE scheduler_job_queue
          SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          WHERE status = 'RUNNING'
            AND (
              picked_by IS NULL OR picked_by NOT IN (
                SELECT node_id FROM scheduler_node
                WHERE heartbeat_ts >= CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) - NUMTODSINTERVAL(?, 'SECOND')
              )
            )
            AND picked_at <= CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) - NUMTODSINTERVAL(?, 'SECOND')
          """;
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, graceSec)
          .setParameter(2, graceSec)
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("reset orphan jobs", e);
    }
  }

  int resetOrphanJobsBefore(Instant cutoff) {
    try {
      // language=Oracle
      String sql =
          """
          UPDATE scheduler_job_queue
          SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          WHERE status = 'RUNNING'
            AND (
              picked_by IS NULL OR picked_by NOT IN (
                SELECT node_id FROM scheduler_node
                WHERE heartbeat_ts >= ?
              )
            )
            AND picked_at < ?
          """;
      Timestamp cutoffTimestamp = OracleTimestamps.microTimestamp(cutoff);
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, cutoffTimestamp)
          .setParameter(2, cutoffTimestamp)
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("reset orphan jobs before cutoff", e);
    }
  }

  int resetOrphanJobsForNode(String nodeId) {
    try {
      // language=Oracle
      String sql =
          """
          UPDATE scheduler_job_queue
          SET status = 'PENDING', picked_by = NULL, picked_at = NULL, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          WHERE status = 'RUNNING' AND picked_by = ?
          """;
      return ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("reset orphan jobs for node", e);
    }
  }
}

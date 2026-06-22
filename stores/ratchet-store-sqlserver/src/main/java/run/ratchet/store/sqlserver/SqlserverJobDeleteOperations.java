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
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;

final class SqlserverJobDeleteOperations {

  private final SqlserverStoreContext ctx;
  private final SqlserverBusinessKeyReservations reservations;

  SqlserverJobDeleteOperations(
      SqlserverStoreContext ctx, SqlserverBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  void delete(UUID id) {
    try {
      reservations.deleteReservationByOwner(id);
      // language=SQL Server
      String sql = "DELETE FROM scheduler_job WHERE job_id = ?";
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, UuidByteArrayConverter.toBytes(id))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete job", e);
    }
  }

  int deleteJobsByIds(List<UUID> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    try {
      reservations.deleteReservationsByOwners(ids);
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=SQL Server
      String sql = "DELETE FROM scheduler_job WHERE job_id IN (" + placeholders + ")";
      Query jobDelete = ctx.em().createNativeQuery(sql);
      bindUuidParameters(jobDelete, ids);
      return jobDelete.executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete jobs by ids", e);
    }
  }

  int deleteDlqOlderThan(Instant cutoff) {
    try {
      // language=SQL Server
      String deleteSql =
          """
          DELETE FROM scheduler_job
          WHERE terminal_status = 'FAILED' AND total_attempts >= max_retries
            AND terminated_at < ?
          """;
      return ctx.em()
          .createNativeQuery(deleteSql)
          .setParameter(1, Timestamp.from(cutoff))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete dlq older than cutoff", e);
    }
  }

  private static void bindUuidParameters(Query query, List<UUID> ids) {
    int parameter = 1;
    for (UUID id : ids) {
      query.setParameter(parameter++, UuidByteArrayConverter.toBytes(id));
    }
  }

  int resetOrphanJobs(Duration grace) {
    try {
      long graceSeconds = grace.toSeconds();
      // language=SQL Server
      String sql =
          """
          UPDATE scheduler_job_queue
          SET status = 'PENDING',
              picked_by = NULL, picked_at = NULL,
              updated_at = SYSUTCDATETIME()
          WHERE status = 'RUNNING'
            AND NOT EXISTS (
              SELECT 1 FROM scheduler_node n
              WHERE n.node_id = scheduler_job_queue.picked_by
                AND n.heartbeat_ts > DATEADD(SECOND, -(?), SYSUTCDATETIME())
            )
            AND DATEDIFF_BIG(SECOND, picked_at, SYSUTCDATETIME()) >= ?
          """;
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, graceSeconds)
          .setParameter(2, graceSeconds)
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("reset orphan jobs", e);
    }
  }

  int resetOrphanJobsBefore(Instant cutoff) {
    try {
      // language=SQL Server
      String sql =
          """
          UPDATE scheduler_job_queue
          SET status = 'PENDING',
              picked_by = NULL, picked_at = NULL,
              updated_at = SYSUTCDATETIME()
          WHERE status = 'RUNNING'
            AND NOT EXISTS (
              SELECT 1 FROM scheduler_node n
              WHERE n.node_id = scheduler_job_queue.picked_by
                AND n.heartbeat_ts >= ?
            )
            AND picked_at < ?
          """;
      Timestamp cutoffTimestamp = Timestamp.from(cutoff);
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
      // language=SQL Server
      String sql =
          """
          UPDATE scheduler_job_queue
          SET status = 'PENDING',
              picked_by = NULL, picked_at = NULL,
              updated_at = SYSUTCDATETIME()
          WHERE status = 'RUNNING' AND picked_by = ?
          """;
      return ctx.em().createNativeQuery(sql).setParameter(1, nodeId).executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("reset orphan jobs for node", e);
    }
  }
}

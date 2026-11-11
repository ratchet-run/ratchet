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

import static run.ratchet.store.entity.JobExecutionEntity.ORPHAN_RECOVERY_ERROR_MESSAGE;
import static run.ratchet.store.entity.JobExecutionEntity.STALE_EXECUTION_ERROR_MESSAGE;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExhaustedOrphan;
import run.ratchet.store.spi.OrphanRecovery;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.RowValues;

final class SqlserverJobDeleteOperations {

  static final int ORPHAN_RECOVERY_CHUNK_SIZE = 500;

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

  int deleteTerminalJobsByIds(List<UUID> ids) {
    if (ids.isEmpty()) {
      return 0;
    }
    try {
      reservations.deleteReservationsByOwners(ids);
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=SQL Server
      String sql =
          "DELETE FROM scheduler_job WHERE job_id IN ("
              + placeholders
              + ") AND terminal_status IS NOT NULL";
      Query jobDelete = ctx.em().createNativeQuery(sql);
      bindUuidParameters(jobDelete, ids);
      return jobDelete.executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete terminal jobs by ids", e);
    }
  }

  int deleteDlqOlderThan(Instant cutoff) {
    try {
      // language=SQL Server
      String deleteSql =
          """
          DELETE FROM scheduler_job
          WHERE terminal_status = 'FAILED'
            AND terminated_at < ?
            AND NOT EXISTS (SELECT 1 FROM scheduler_batch pb
              JOIN scheduler_partition_set ps ON ps.set_id = pb.partition_set_id
              WHERE pb.partition_set_id IS NOT NULL AND ps.sealed_generation < pb.generation
                AND (scheduler_job.job_id = pb.batch_id OR (scheduler_job.job_type = 'BATCH_CHILD' AND scheduler_job.depends_on = pb.batch_id)))
          """;
      return ctx.em()
          .createNativeQuery(deleteSql)
          .setParameter(1, SqlserverTimestamps.microTimestamp(cutoff))
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

  Optional<JobEntity> findOrphanCompletionSnapshot(UUID jobId) {
    String sql =
        """
        SELECT j.job_type, j.depends_on, q.claim_seq, q.picked_by, q.attempts, j.max_retries,
               j.priority, j.business_key, j.recurring_master_id
        FROM scheduler_job j JOIN scheduler_job_queue q ON q.job_id = j.job_id
        WHERE j.job_id = ?
        """;
    List<?> rows =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    Object[] row = (Object[]) rows.get(0);
    JobEntity snapshot = new JobEntity();
    snapshot.setId(jobId);
    snapshot.setStatus(JobStatus.RUNNING);
    snapshot.setJobType(JobExecutionType.valueOf(row[0].toString()));
    snapshot.setDependsOn(SqlserverJobRowMapper.uuidOrNull(row[1]));
    snapshot.setClaimSeq(((Number) row[2]).longValue());
    snapshot.setPickedBy(row[3] == null ? null : row[3].toString());
    snapshot.setAttempts(((Number) row[4]).intValue());
    snapshot.setMaxRetries(((Number) row[5]).intValue());
    snapshot.setPriority(RowValues.safeJobPriority(((Number) row[6]).intValue()));
    snapshot.setBusinessKey(RowValues.stringOrNull(row[7]));
    snapshot.setRecurringMasterId(SqlserverJobRowMapper.uuidOrNull(row[8]));
    return Optional.of(snapshot);
  }

  OrphanRecovery resetOrphanJobs(Duration grace, int maxCrashRedeliveries, int exhaustedLimit) {
    return resetOrphanJobsBefore(Instant.now().minus(grace), maxCrashRedeliveries, exhaustedLimit);
  }

  int closeStaleExecutions(Instant cutoff, int limit) {
    if (limit <= 0) {
      return 0;
    }
    try {
      String sql =
          """
          UPDATE scheduler_job_execution
          SET status = 'FAILED', ended_at = SYSUTCDATETIME(), error_message = ?
          WHERE status = 'RUNNING' AND id IN (
            SELECT TOP (?) e.id FROM scheduler_job_execution e
            WHERE e.status = 'RUNNING' AND e.started_at < ?
              AND NOT EXISTS (SELECT 1 FROM scheduler_job_queue q
                WHERE q.job_id = e.job_id AND (q.status = 'RUNNING' OR q.updated_at >= ?))
              AND NOT EXISTS (SELECT 1 FROM scheduler_job j
                WHERE j.job_id = e.job_id AND j.terminated_at >= ?)
            ORDER BY e.started_at
          )
          """;
      Timestamp cutoffTimestamp = SqlserverTimestamps.microTimestamp(cutoff);
      Query query = ctx.em().createNativeQuery(sql);
      query.setParameter(1, STALE_EXECUTION_ERROR_MESSAGE);
      query.setParameter(2, limit);
      query.setParameter(3, cutoffTimestamp);
      query.setParameter(4, cutoffTimestamp);
      query.setParameter(5, cutoffTimestamp);
      int closed = query.executeUpdate();
      ctx.em().clear();
      return closed;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("close stale executions", e);
    }
  }

  OrphanRecovery resetOrphanJobsBefore(
      Instant cutoff, int maxCrashRedeliveries, int exhaustedLimit) {
    String predicate =
        """
        status = 'RUNNING'
            AND NOT EXISTS (
              SELECT 1 FROM scheduler_node n
              WHERE n.node_id = scheduler_job_queue.picked_by
                AND n.heartbeat_ts >= ?
            )
            AND picked_at < ?
        """;
    Timestamp cutoffTimestamp = SqlserverTimestamps.microTimestamp(cutoff);
    return recoverOrphans(
        predicate,
        List.of(cutoffTimestamp, cutoffTimestamp),
        maxCrashRedeliveries,
        exhaustedLimit,
        false,
        cutoffTimestamp);
  }

  OrphanRecovery resetOrphanJobsForNode(
      String nodeId, int maxCrashRedeliveries, int exhaustedLimit) {
    return recoverOrphans(
        "status = 'RUNNING' AND picked_by = ?",
        List.of(nodeId),
        maxCrashRedeliveries,
        exhaustedLimit,
        true,
        null);
  }

  private OrphanRecovery recoverOrphans(
      String predicate,
      List<?> parameters,
      int maxCrashRedeliveries,
      int exhaustedLimit,
      boolean releaseOwner,
      Timestamp liveSince) {
    try {
      // The candidate read takes no locks, so a scan without orphans locks nothing. It reads one
      // page of ids at a time, keyed by job_id, so memory stays bounded by the page size. The
      // re-check uses UPDLOCK to lock only candidates by primary key, matching the terminal
      // path's first lock. Later statements address those locked rows by id. All pages share one
      // transaction and hold their locks until commit. The re-check and the queue updates force a
      // seek on pk_scheduler_job_queue, so they lock clustered rows first, in the same order as
      // the terminal path, and never seek idx_queue_orphan. The updates use FORCESEEK with
      // INDEX(...) because the SQL Server documentation does not allow FORCESEEK with index
      // parameters on an UPDATE target.
      //
      // On periodic scans the re-check also reads each candidate owner's node row under HOLDLOCK,
      // seeking pk_scheduler_node. It sees a heartbeat that committed after the candidate read,
      // and its shared lock keeps that owner from recording a heartbeat until the scan commits.
      // REPEATABLEREAD is not enough: SQL Server releases its lock on a row the subquery's WHERE
      // rejects, which is the stale owner's row. Only the candidates' owners are locked, so other
      // nodes' heartbeats don't wait.
      int count = 0;
      List<ExhaustedOrphan> exhausted = new ArrayList<>();
      UUID lastId = null;
      boolean more = true;
      while (more) {
        Query candidates =
            ctx.em()
                .createNativeQuery(
                    "SELECT TOP ("
                        + ORPHAN_RECOVERY_CHUNK_SIZE
                        + ") job_id FROM scheduler_job_queue WHERE "
                        + predicate
                        + (lastId == null ? "" : " AND job_id > ?")
                        + " ORDER BY job_id");
        bindPredicate(candidates, parameters, 1);
        if (lastId != null) {
          candidates.setParameter(parameters.size() + 1, UuidByteArrayConverter.toBytes(lastId));
        }
        List<?> candidateRows = candidates.getResultList();
        if (candidateRows.isEmpty()) {
          if (lastId == null) return new OrphanRecovery(0, List.of());
          break;
        }
        List<UUID> chunk = new ArrayList<>(candidateRows.size());
        for (Object row : candidateRows) {
          chunk.add(SqlserverJobRowMapper.uuidOrNull(row));
        }
        lastId = chunk.get(chunk.size() - 1);
        more = chunk.size() == ORPHAN_RECOVERY_CHUNK_SIZE;
        String ownerCheck =
            liveSince == null
                ? "0 AS owner_live"
                : """
                CASE WHEN EXISTS (
                  SELECT 1 FROM scheduler_node n
                      WITH (HOLDLOCK, ROWLOCK, FORCESEEK(pk_scheduler_node(node_id)))
                  WHERE n.node_id = scheduler_job_queue.picked_by AND n.heartbeat_ts >= ?
                ) THEN 1 ELSE 0 END AS owner_live
                """;
        String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
        Query lock =
            ctx.em()
                .createNativeQuery(
                    """
                    SELECT job_id, claim_seq, crash_count, picked_by,
                    """
                        + ownerCheck
                        + " "
                        + """
                        FROM scheduler_job_queue
                            WITH (UPDLOCK, ROWLOCK, FORCESEEK(pk_scheduler_job_queue(job_id)))
                        WHERE job_id IN (
                        """
                        + placeholders
                        + ") AND "
                        + predicate
                        + " ORDER BY job_id");
        int index = 1;
        if (liveSince != null) lock.setParameter(index++, liveSince);
        bindUuidList(lock, chunk, index);
        bindPredicate(lock, parameters, chunk.size() + index);
        List<UUID> confirmed = new ArrayList<>();
        List<UUID> resettable = new ArrayList<>();
        List<UUID> exhaustedIds = new ArrayList<>();
        for (Object result : lock.getResultList()) {
          Object[] row = (Object[]) result;
          if (((Number) row[4]).intValue() != 0) continue;
          UUID id = SqlserverJobRowMapper.uuidOrNull(row[0]);
          int crashCount = ((Number) row[2]).intValue();
          confirmed.add(id);
          if (crashCount < maxCrashRedeliveries) {
            resettable.add(id);
          } else {
            exhaustedIds.add(id);
            if (exhausted.size() < exhaustedLimit) {
              exhausted.add(
                  new ExhaustedOrphan(
                      id,
                      ((Number) row[1]).longValue(),
                      crashCount,
                      row[3] == null ? null : row[3].toString()));
            }
          }
        }
        if (confirmed.isEmpty()) {
          continue;
        }
        Query close =
            ctx.em()
                .createNativeQuery(
                    """
                    UPDATE scheduler_job_execution
                    SET status = 'FAILED', ended_at = SYSUTCDATETIME(), error_message = ?
                    WHERE status = 'RUNNING' AND job_id IN (
                    """
                        + String.join(",", Collections.nCopies(confirmed.size(), "?"))
                        + ")");
        close.setParameter(1, ORPHAN_RECOVERY_ERROR_MESSAGE);
        bindUuidList(close, confirmed, 2);
        close.executeUpdate();
        if (!resettable.isEmpty()) {
          Query reset =
              ctx.em()
                  .createNativeQuery(
                      """
                      UPDATE q
                      SET status = 'PENDING', picked_by = NULL, picked_at = NULL,
                          crash_count = crash_count + 1, updated_at = SYSUTCDATETIME()
                      FROM scheduler_job_queue q WITH (FORCESEEK, INDEX(pk_scheduler_job_queue))
                      WHERE q.job_id IN (
                      """
                          + String.join(",", Collections.nCopies(resettable.size(), "?"))
                          + ") AND q.status = 'RUNNING'");
          bindUuidList(reset, resettable, 1);
          count += reset.executeUpdate();
        }
        if (releaseOwner && !exhaustedIds.isEmpty()) {
          Query release =
              ctx.em()
                  .createNativeQuery(
                      """
                      UPDATE q SET picked_by = NULL
                      FROM scheduler_job_queue q WITH (FORCESEEK, INDEX(pk_scheduler_job_queue))
                      WHERE q.job_id IN (
                      """
                          + String.join(",", Collections.nCopies(exhaustedIds.size(), "?"))
                          + ")");
          bindUuidList(release, exhaustedIds, 1);
          release.executeUpdate();
        }
      }
      ctx.em().clear();
      return new OrphanRecovery(count, List.copyOf(exhausted));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("recover orphan jobs", e);
    }
  }

  private static void bindUuidList(Query query, List<UUID> ids, int index) {
    for (UUID id : ids) {
      query.setParameter(index++, UuidByteArrayConverter.toBytes(id));
    }
  }

  private static void bindPredicate(Query query, List<?> parameters, int index) {
    for (Object parameter : parameters) {
      query.setParameter(index++, parameter);
    }
  }
}

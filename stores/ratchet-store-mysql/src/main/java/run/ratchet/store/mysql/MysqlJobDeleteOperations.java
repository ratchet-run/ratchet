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
package run.ratchet.store.mysql;

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
import java.util.TreeSet;
import java.util.UUID;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.ExhaustedOrphan;
import run.ratchet.store.spi.OrphanRecovery;
import run.ratchet.store.util.RowValues;

final class MysqlJobDeleteOperations {

  static final int ORPHAN_RECOVERY_CHUNK_SIZE = 500;

  private final MysqlStoreContext ctx;
  private final MysqlBusinessKeyReservations reservations;

  MysqlJobDeleteOperations(MysqlStoreContext ctx, MysqlBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  void delete(UUID id) {
    try {
      reservations.deleteReservationByOwner(id);
      // language=MySQL
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
    try {
      if (ids.isEmpty()) {
        return 0;
      }
      reservations.deleteReservationsByOwners(ids);
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=MySQL
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
      // language=MySQL
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
     * Transaction contract: this method is reached through MysqlJobStoreImpl's REQUIRED boundary.
     * The reservation cleanup and cold-row delete must commit or roll back together.
     */
    try {
      // language=MySQL
      String selectSql =
          """
          SELECT job_id FROM scheduler_job
          WHERE terminal_status = 'FAILED'
            AND terminated_at < ?
          """;
      @SuppressWarnings("unchecked")
      List<?> idRows =
          ctx.em()
              .createNativeQuery(selectSql)
              .setParameter(1, Timestamp.from(cutoff))
              .getResultList();
      if (idRows.isEmpty()) {
        return 0;
      }
      List<UUID> ids = new ArrayList<>(idRows.size());
      for (Object n : idRows) {
        ids.add(MysqlJobRowMapper.uuidOrNull(n));
      }
      reservations.deleteReservationsByOwners(ids);
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=MySQL
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
    snapshot.setDependsOn(MysqlJobRowMapper.uuidOrNull(row[1]));
    snapshot.setClaimSeq(((Number) row[2]).longValue());
    snapshot.setPickedBy(row[3] == null ? null : row[3].toString());
    snapshot.setAttempts(((Number) row[4]).intValue());
    snapshot.setMaxRetries(((Number) row[5]).intValue());
    snapshot.setPriority(RowValues.safeJobPriority(((Number) row[6]).intValue()));
    snapshot.setBusinessKey(RowValues.stringOrNull(row[7]));
    snapshot.setRecurringMasterId(MysqlJobRowMapper.uuidOrNull(row[8]));
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
          UPDATE scheduler_job_execution e
          SET status = 'FAILED', ended_at = NOW(3), error_message = ?
          WHERE e.status = 'RUNNING' AND e.started_at < ?
              AND NOT EXISTS (SELECT 1 FROM scheduler_job_queue q
                WHERE q.job_id = e.job_id AND (q.status = 'RUNNING' OR q.updated_at >= ?))
              AND NOT EXISTS (SELECT 1 FROM scheduler_job j
                WHERE j.job_id = e.job_id AND j.terminated_at >= ?)
          ORDER BY e.started_at LIMIT ?
          """;
      Timestamp cutoffTimestamp = Timestamp.from(cutoff);
      Query query = ctx.em().createNativeQuery(sql);
      query.setParameter(1, STALE_EXECUTION_ERROR_MESSAGE);
      query.setParameter(2, cutoffTimestamp);
      query.setParameter(3, cutoffTimestamp);
      query.setParameter(4, cutoffTimestamp);
      query.setParameter(5, limit);
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
            AND (
              picked_by IS NULL OR picked_by NOT IN (
                SELECT node_id FROM scheduler_node
                WHERE heartbeat_ts >= ?
              )
            )
            AND picked_at < ?
        """;
    Timestamp cutoffTimestamp = Timestamp.from(cutoff);
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
      // The candidate read takes no locks, so a scan without orphans locks nothing. The re-check
      // locks only candidates through PRIMARY in key order, matching the terminal DELETE; later
      // statements address those locked rows by id. All chunks share one transaction and hold
      // locks until commit: chunking only bounds statement size.
      // Under REPEATABLE READ the lock statement's node subquery reads the candidate snapshot,
      // so periodic scans re-read owner heartbeats with a locking read keyed by primary key.
      // This sees heartbeats committed since and locks only owners' node rows, so other nodes'
      // heartbeats do not wait for the scan.
      Query candidates =
          ctx.em()
              .createNativeQuery(
                  "SELECT job_id FROM scheduler_job_queue WHERE " + predicate + " ORDER BY job_id");
      bindPredicate(candidates, parameters, 1);
      List<?> candidateRows = candidates.getResultList();
      if (candidateRows.isEmpty()) {
        return new OrphanRecovery(0, List.of());
      }
      List<UUID> ids = new ArrayList<>(candidateRows.size());
      for (Object row : candidateRows) {
        ids.add(MysqlJobRowMapper.uuidOrNull(row));
      }
      int count = 0;
      List<ExhaustedOrphan> exhausted = new ArrayList<>();
      for (int offset = 0; offset < ids.size(); offset += ORPHAN_RECOVERY_CHUNK_SIZE) {
        List<UUID> chunk =
            ids.subList(offset, Math.min(offset + ORPHAN_RECOVERY_CHUNK_SIZE, ids.size()));
        String placeholders = String.join(",", Collections.nCopies(chunk.size(), "?"));
        Query lock =
            ctx.em()
                .createNativeQuery(
                    """
                    SELECT job_id, claim_seq, crash_count, picked_by
                    FROM scheduler_job_queue FORCE INDEX (PRIMARY)
                    WHERE job_id IN (
                    """
                        + placeholders
                        + ") AND "
                        + predicate
                        + " ORDER BY job_id FOR UPDATE");
        bindUuidList(lock, chunk, 1);
        bindPredicate(lock, parameters, chunk.size() + 1);
        List<?> lockedRows = lock.getResultList();
        List<?> liveOwners = List.of();
        if (liveSince != null) {
          TreeSet<String> owners = new TreeSet<>();
          for (Object result : lockedRows) {
            Object[] row = (Object[]) result;
            if (row[3] != null) {
              owners.add(row[3].toString());
            }
          }
          if (!owners.isEmpty()) {
            Query nodes =
                ctx.em()
                    .createNativeQuery(
                        """
                        SELECT node_id FROM scheduler_node FORCE INDEX (PRIMARY)
                        WHERE node_id IN (
                        """
                            + String.join(",", Collections.nCopies(owners.size(), "?"))
                            + ") AND heartbeat_ts >= ? ORDER BY node_id FOR SHARE");
            int index = 1;
            for (String owner : owners) {
              nodes.setParameter(index++, owner);
            }
            nodes.setParameter(index, liveSince);
            liveOwners = nodes.getResultList();
          }
        }
        List<UUID> confirmed = new ArrayList<>();
        List<UUID> resettable = new ArrayList<>();
        List<UUID> exhaustedIds = new ArrayList<>();
        for (Object result : lockedRows) {
          Object[] row = (Object[]) result;
          if (row[3] != null && liveOwners.contains(row[3].toString())) {
            continue;
          }
          UUID id = MysqlJobRowMapper.uuidOrNull(row[0]);
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
                    SET status = 'FAILED', ended_at = NOW(3), error_message = ?
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
                      UPDATE scheduler_job_queue FORCE INDEX (PRIMARY)
                      SET status = 'PENDING', picked_by = NULL, picked_at = NULL,
                          crash_count = crash_count + 1, updated_at = NOW(3)
                      WHERE job_id IN (
                      """
                          + String.join(",", Collections.nCopies(resettable.size(), "?"))
                          + ") AND status = 'RUNNING' ORDER BY job_id");
          bindUuidList(reset, resettable, 1);
          count += reset.executeUpdate();
        }
        if (releaseOwner && !exhaustedIds.isEmpty()) {
          Query release =
              ctx.em()
                  .createNativeQuery(
                      """
                      UPDATE scheduler_job_queue FORCE INDEX (PRIMARY) SET picked_by = NULL
                      WHERE job_id IN (
                      """
                          + String.join(",", Collections.nCopies(exhaustedIds.size(), "?"))
                          + ") ORDER BY job_id");
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

  private static void bindPredicate(Query query, List<?> parameters, int index) {
    for (Object parameter : parameters) {
      query.setParameter(index++, parameter);
    }
  }
}

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

import static run.ratchet.store.entity.JobExecutionEntity.STALE_EXECUTION_ERROR_MESSAGE;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
            AND NOT EXISTS (SELECT 1 FROM scheduler_batch pb
              JOIN scheduler_partition_set ps ON ps.set_id = pb.partition_set_id
              WHERE pb.partition_set_id IS NOT NULL AND ps.sealed_generation < pb.generation
                AND (scheduler_job.job_id = pb.batch_id OR (scheduler_job.job_type = 'BATCH_CHILD' AND scheduler_job.depends_on = pb.batch_id)))
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

  OrphanRecovery resetOrphanJobsBefore(Instant cutoff, int maxCrashRedeliveries, int limit) {
    throw new UnsupportedOperationException("Bounded orphan recovery is not implemented yet");
  }

  OrphanRecovery resetOrphanJobsForNode(String nodeId, int maxCrashRedeliveries, int limit) {
    throw new UnsupportedOperationException("Bounded orphan recovery is not implemented yet");
  }

  private static void bindPredicate(Query query, List<?> parameters, int index) {
    for (Object parameter : parameters) {
      query.setParameter(index++, parameter);
    }
  }

  public List<ExhaustedOrphan> recoverExhaustedOrphans(
      Instant cutoff, int maxCrashRedeliveries, int limit) {
    throw new UnsupportedOperationException("Bounded orphan recovery is not implemented yet");
  }

  public boolean deferExhaustedOrphan(UUID jobId, long claimSeq) {
    throw new UnsupportedOperationException("Bounded orphan recovery is not implemented yet");
  }
}

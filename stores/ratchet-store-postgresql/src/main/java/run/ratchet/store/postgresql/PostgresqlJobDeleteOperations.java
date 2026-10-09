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
import run.ratchet.store.util.RowValues;

final class PostgresqlJobDeleteOperations {

  private final PostgresqlStoreContext ctx;
  private final PostgresqlBusinessKeyReservations reservations;

  PostgresqlJobDeleteOperations(
      PostgresqlStoreContext ctx, PostgresqlBusinessKeyReservations reservations) {
    this.ctx = ctx;
    this.reservations = reservations;
  }

  void delete(UUID id) {
    try {
      reservations.deleteReservationByOwner(id);
      // language=PostgreSQL
      String sql = "DELETE FROM scheduler_job WHERE job_id = ?";
      ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate();
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
      // language=PostgreSQL
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
      // language=PostgreSQL
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
      // language=PostgreSQL
      String deleteSql =
          """
          DELETE FROM scheduler_job
          WHERE terminal_status = 'FAILED'
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
      query.setParameter(parameter++, id);
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
    List<?> rows = ctx.em().createNativeQuery(sql).setParameter(1, jobId).getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    Object[] row = (Object[]) rows.get(0);
    JobEntity snapshot = new JobEntity();
    snapshot.setId(jobId);
    snapshot.setStatus(JobStatus.RUNNING);
    snapshot.setJobType(JobExecutionType.valueOf(row[0].toString()));
    snapshot.setDependsOn(PostgresqlJobRowMapper.uuidOrNull(row[1]));
    snapshot.setClaimSeq(((Number) row[2]).longValue());
    snapshot.setPickedBy(row[3] == null ? null : row[3].toString());
    snapshot.setAttempts(((Number) row[4]).intValue());
    snapshot.setMaxRetries(((Number) row[5]).intValue());
    snapshot.setPriority(RowValues.safeJobPriority(((Number) row[6]).intValue()));
    snapshot.setBusinessKey(RowValues.stringOrNull(row[7]));
    snapshot.setRecurringMasterId(PostgresqlJobRowMapper.uuidOrNull(row[8]));
    return Optional.of(snapshot);
  }

  OrphanRecovery resetOrphanJobs(Duration grace, int maxCrashRedeliveries, int exhaustedLimit) {
    return resetOrphanJobsBefore(Instant.now().minus(grace), maxCrashRedeliveries, exhaustedLimit);
  }

  OrphanRecovery resetOrphanJobsBefore(
      Instant cutoff, int maxCrashRedeliveries, int exhaustedLimit) {
    String predicate =
        """
        WHERE status = 'RUNNING'
            AND NOT EXISTS (
              SELECT 1 FROM scheduler_node n
              WHERE n.node_id = scheduler_job_queue.picked_by
                AND n.heartbeat_ts >= ?
            )
            AND picked_at < ?
        """;
    Timestamp cutoffTimestamp = Timestamp.from(cutoff);
    return recoverOrphans(
        predicate,
        List.of(cutoffTimestamp, cutoffTimestamp),
        maxCrashRedeliveries,
        exhaustedLimit,
        false);
  }

  OrphanRecovery resetOrphanJobsForNode(
      String nodeId, int maxCrashRedeliveries, int exhaustedLimit) {
    return recoverOrphans(
        "WHERE status = 'RUNNING' AND picked_by = ?",
        List.of(nodeId),
        maxCrashRedeliveries,
        exhaustedLimit,
        true);
  }

  private OrphanRecovery recoverOrphans(
      String predicate,
      List<?> parameters,
      int maxCrashRedeliveries,
      int exhaustedLimit,
      boolean releaseOwner) {
    try {
      String resetSql =
          """
          UPDATE scheduler_job_queue
          SET status = 'PENDING', picked_by = NULL, picked_at = NULL,
              crash_count = crash_count + 1, updated_at = statement_timestamp()
          """
              + predicate
              + " AND crash_count < ?";
      Query reset = ctx.em().createNativeQuery(resetSql);
      bindRecovery(reset, parameters, maxCrashRedeliveries);
      int count = reset.executeUpdate();
      List<ExhaustedOrphan> exhausted = new ArrayList<>();
      if (exhaustedLimit > 0) {
        String selectSql =
            """
            SELECT job_id, claim_seq, crash_count, picked_by FROM scheduler_job_queue
            """
                + predicate
                + " AND crash_count >= ? ORDER BY job_id";
        Query select = ctx.em().createNativeQuery(selectSql);
        bindRecovery(select, parameters, maxCrashRedeliveries);
        select.setMaxResults(exhaustedLimit);
        for (Object result : select.getResultList()) {
          Object[] row = (Object[]) result;
          exhausted.add(
              new ExhaustedOrphan(
                  PostgresqlJobRowMapper.uuidOrNull(row[0]),
                  ((Number) row[1]).longValue(),
                  ((Number) row[2]).intValue(),
                  row[3] == null ? null : row[3].toString()));
        }
      }
      if (releaseOwner) {
        Query release =
            ctx.em()
                .createNativeQuery(
                    "UPDATE scheduler_job_queue SET picked_by = NULL "
                        + predicate
                        + " AND crash_count >= ?");
        bindRecovery(release, parameters, maxCrashRedeliveries);
        release.executeUpdate();
      }
      ctx.em().clear();
      return new OrphanRecovery(count, List.copyOf(exhausted));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("recover orphan jobs", e);
    }
  }

  private static void bindRecovery(Query query, List<?> parameters, int maxCrashRedeliveries) {
    int index = 1;
    for (Object parameter : parameters) {
      query.setParameter(index++, parameter);
    }
    query.setParameter(index, maxCrashRedeliveries);
  }
}

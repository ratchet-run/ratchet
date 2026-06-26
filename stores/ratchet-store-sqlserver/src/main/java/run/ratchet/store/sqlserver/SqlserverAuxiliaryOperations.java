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

import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.RowValues;

final class SqlserverAuxiliaryOperations
    implements JobAuditStore, WorkflowConditionStore, DlqAlertStore, ResourcePermitStore {

  private static final int PERMIT_CLEANUP_CHUNK_SIZE = 500;

  private final SqlserverStoreContext ctx;

  SqlserverAuxiliaryOperations(SqlserverStoreContext ctx) {
    this.ctx = ctx;
  }

  private static WorkflowConditionEntity mapCondition(Object[] row) {
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setId(SqlserverJobRowMapper.uuidOrNull(row[0]));
    condition.setParentJobId(SqlserverJobRowMapper.uuidOrNull(row[1]));
    condition.setChildJobId(SqlserverJobRowMapper.uuidOrNull(row[2]));
    condition.setConditionType(WorkflowCondition.ConditionType.valueOf(row[3].toString()));
    condition.setConditionExpression(row[4] == null ? null : row[4].toString());
    condition.setConditionPriority(((Number) row[5]).intValue());
    condition.setCreatedAt(RowValues.instantOrNull(row[6]));
    return condition;
  }

  @Override
  public JobExecutionEntity saveExecution(JobExecutionEntity execution) {
    if (execution.getId() == null) {
      ctx.em().persist(execution);
      return execution;
    }
    return ctx.em().merge(execution);
  }

  @Override
  public List<JobExecutionEntity> findExecutionsByJobId(UUID jobId, int limit, int offset) {
    // language=JPAQL
    String jpql = "SELECT e FROM JobExecutionEntity e WHERE e.jobId = :jid ORDER BY e.attempt ASC";
    return ctx.em()
        .createQuery(jpql, JobExecutionEntity.class)
        .setParameter("jid", jobId)
        .setFirstResult(offset)
        .setMaxResults(limit)
        .getResultList();
  }

  @Override
  public Optional<JobExecutionEntity> findLatestExecution(UUID jobId) {
    // language=JPAQL
    String jpql = "SELECT e FROM JobExecutionEntity e WHERE e.jobId = :jid ORDER BY e.attempt DESC";
    List<JobExecutionEntity> results =
        ctx.em()
            .createQuery(jpql, JobExecutionEntity.class)
            .setParameter("jid", jobId)
            .setMaxResults(1)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public int countExecutionAttempts(UUID jobId) {
    // language=JPAQL
    String jpql = "SELECT COUNT(e) FROM JobExecutionEntity e WHERE e.jobId = :jid";
    return ctx.em()
        .createQuery(jpql, Long.class)
        .setParameter("jid", jobId)
        .getSingleResult()
        .intValue();
  }

  @Override
  public void appendLog(JobLogEntity logEntry) {
    ctx.em().persist(logEntry);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    // language=JPAQL
    String jpql = "DELETE FROM JobLogEntity l WHERE l.ts < :cutoff";
    // ts is DATETIME2(6); floor the cutoff to microseconds so the boundary matches the column
    // precision rather than mssql-jdbc's nanosecond-precision bind (see existsRecentDlqAlert).
    return ctx.em()
        .createQuery(jpql)
        .setParameter("cutoff", cutoff.truncatedTo(ChronoUnit.MICROS))
        .executeUpdate();
  }

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    prepareCondition(condition);
    // language=SQL Server
    String sql =
        """
        MERGE scheduler_workflow_condition WITH (HOLDLOCK) AS tgt
        USING (VALUES (?, ?, ?, ?, ?, ?, ?))
          AS src(id, parent_job_id, child_job_id, condition_type, condition_expression,
                 condition_priority, created_at)
          ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET
          parent_job_id = src.parent_job_id,
          child_job_id = src.child_job_id,
          condition_type = src.condition_type,
          condition_expression = src.condition_expression,
          condition_priority = src.condition_priority,
          created_at = src.created_at
        WHEN NOT MATCHED THEN INSERT
          (id, parent_job_id, child_job_id, condition_type, condition_expression,
           condition_priority, created_at)
          VALUES (src.id, src.parent_job_id, src.child_job_id, src.condition_type,
                  src.condition_expression, src.condition_priority, src.created_at);
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(condition.getId()))
        .setParameter(2, UuidByteArrayConverter.toBytes(condition.getParentJobId()))
        .setParameter(3, UuidByteArrayConverter.toBytes(condition.getChildJobId()))
        .setParameter(4, condition.getConditionType().name())
        .setParameter(5, condition.getConditionExpression())
        .setParameter(6, condition.getConditionPriority())
        .setParameter(7, Timestamp.from(condition.getCreatedAt()))
        .executeUpdate();
    return condition;
  }

  @Override
  public WorkflowConditionEntity findConditionById(UUID id) {
    List<WorkflowConditionEntity> results =
        findConditions(
            "WHERE id = ?",
            List.of(UuidByteArrayConverter.toBytes(id)),
            "ORDER BY condition_priority ASC");
    return results.isEmpty() ? null : results.get(0);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId) {
    return findConditions(
        "WHERE parent_job_id = ?",
        List.of(UuidByteArrayConverter.toBytes(parentJobId)),
        "ORDER BY condition_priority ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId) {
    return findConditions(
        "WHERE child_job_id = ?",
        List.of(UuidByteArrayConverter.toBytes(childJobId)),
        "ORDER BY condition_priority ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type) {
    return findConditions(
        "WHERE parent_job_id = ? AND condition_type = ?",
        List.of(UuidByteArrayConverter.toBytes(parentJobId), type.name()),
        "ORDER BY condition_priority ASC");
  }

  @Override
  public void deleteConditionById(UUID id) {
    // language=SQL Server
    String sql = "DELETE FROM scheduler_workflow_condition WHERE id = ?";
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(id))
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByParentJobId(UUID parentJobId) {
    // language=SQL Server
    String sql = "DELETE FROM scheduler_workflow_condition WHERE parent_job_id = ?";
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(parentJobId))
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByChildJobId(UUID childJobId) {
    // language=SQL Server
    String sql = "DELETE FROM scheduler_workflow_condition WHERE child_job_id = ?";
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(childJobId))
        .executeUpdate();
  }

  @Override
  public long countConditionsByParentJobId(UUID parentJobId) {
    // language=SQL Server
    String sql = "SELECT COUNT(*) FROM scheduler_workflow_condition WHERE parent_job_id = ?";
    return ctx.countByNative(sql, UuidByteArrayConverter.toBytes(parentJobId));
  }

  @Override
  public DlqAlertEntity saveDlqAlert(DlqAlertEntity alert) {
    if (alert.getId() == null) {
      ctx.em().persist(alert);
      return alert;
    }
    return ctx.em().merge(alert);
  }

  @Override
  public boolean existsRecentDlqAlert(UUID jobId, String errorHash, Instant cutoff) {
    // language=JPAQL
    String jpql =
        """
        SELECT COUNT(a) FROM DlqAlertEntity a
        WHERE a.jobId = :jid AND a.errorHash = :hash AND a.alertSentAt >= :cutoff
        """;
    Long count =
        ctx.em()
            .createQuery(jpql, Long.class)
            .setParameter("jid", jobId)
            .setParameter("hash", errorHash)
            // alert_sent_at is DATETIME2(6), so a persisted Instant is floored to microsecond
            // precision. The mssql-jdbc driver binds an Instant parameter at full nanosecond
            // precision and compares it literally, so an unmodified cutoff equal to a stored alert
            // time fails the `>=` boundary by the sub-microsecond remainder (the MySQL/PG drivers
            // floor the bind for us, which is why this only surfaces on SQL Server and Oracle, and
            // only on a nanosecond-resolution clock). Floor the cutoff to match the column
            // precision.
            .setParameter("cutoff", cutoff.truncatedTo(ChronoUnit.MICROS))
            .getSingleResult();
    return count > 0;
  }

  @Override
  public boolean tryAcquirePermit(String resource, UUID jobId, String nodeId) {
    try {
      // language=SQL Server
      String lockSql =
          """
          SELECT resource_name FROM scheduler_resource_limit WITH (UPDLOCK, HOLDLOCK, ROWLOCK)
          WHERE resource_name = ?
          """;
      @SuppressWarnings("unchecked")
      List<Object> lockedLimits =
          ctx.em().createNativeQuery(lockSql).setParameter(1, resource).getResultList();
      if (lockedLimits.isEmpty()) {
        throw new IllegalArgumentException("Resource is not configured: " + resource);
      }

      // language=SQL Server
      String existingSql =
          """
          SELECT COUNT(*) FROM scheduler_resource_permit
          WHERE resource_name = ? AND job_id = ?
          """;
      Object existing =
          ctx.em()
              .createNativeQuery(existingSql)
              .setParameter(1, resource)
              .setParameter(2, UuidByteArrayConverter.toBytes(jobId))
              .getSingleResult();
      if (((Number) existing).intValue() > 0) {
        return true;
      }

      // SQL Server uses one statement snapshot even after waiting on FOR UPDATE. Keep the lock
      // acquisition as its own statement, then count and insert together with a fresh snapshot.
      // language=SQL Server
      String insertSql =
          """
          INSERT INTO scheduler_resource_permit
            (id, resource_name, job_id, node_id, acquired_at)
          SELECT ?, resource_name, ?, ?, ?
          FROM scheduler_resource_limit
          WHERE resource_name = ?
            AND (
              SELECT COUNT(*) FROM scheduler_resource_permit
              WHERE resource_name = ?
            ) < max_concurrent
          """;
      int inserted =
          ctx.em()
              .createNativeQuery(insertSql)
              .setParameter(1, UuidByteArrayConverter.toBytes(UuidV7Factory.create()))
              .setParameter(2, UuidByteArrayConverter.toBytes(jobId))
              .setParameter(3, nodeId)
              .setParameter(4, Timestamp.from(Instant.now()))
              .setParameter(5, resource)
              .setParameter(6, resource)
              .executeUpdate();
      return inserted > 0;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("try acquire permit", e);
    }
  }

  @Override
  public void releasePermit(String resource, UUID jobId) {
    try {
      // language=SQL Server
      String sql = "DELETE FROM scheduler_resource_permit WHERE resource_name = ? AND job_id = ?";
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, resource)
          .setParameter(2, UuidByteArrayConverter.toBytes(jobId))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("release resource permit", e);
    }
  }

  @Override
  public void releaseAllPermits(UUID jobId) {
    try {
      // language=SQL Server
      String sql = "DELETE FROM scheduler_resource_permit WHERE job_id = ?";
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("release all permits", e);
    }
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    try {
      // language=SQL Server
      String sql = "SELECT retry_delay_ms FROM scheduler_resource_limit WHERE resource_name = ?";
      Object result = ctx.em().createNativeQuery(sql).setParameter(1, resource).getSingleResult();
      return ((Number) result).intValue();
    } catch (NoResultException e) {
      return ResourceLimitEntity.DEFAULT_RETRY_DELAY_MS;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("get permit retry delay", e);
    }
  }

  @Override
  public void configureResource(
      String name, int maxConcurrent, int retryDelayMs, String description) {
    try {
      // language=SQL Server
      String sql =
          """
          MERGE scheduler_resource_limit WITH (HOLDLOCK) AS tgt
          USING (VALUES (?, ?, ?, ?))
            AS src(resource_name, max_concurrent, retry_delay_ms, description)
            ON tgt.resource_name = src.resource_name
          WHEN MATCHED THEN UPDATE SET
            max_concurrent = src.max_concurrent,
            retry_delay_ms = src.retry_delay_ms,
            description = src.description,
            updated_at = SYSUTCDATETIME()
          WHEN NOT MATCHED THEN INSERT
            (resource_name, max_concurrent, retry_delay_ms, description, created_at, updated_at)
            VALUES (src.resource_name, src.max_concurrent, src.retry_delay_ms, src.description,
                    SYSUTCDATETIME(), SYSUTCDATETIME());
          """;
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, name)
          .setParameter(2, maxConcurrent)
          .setParameter(3, retryDelayMs)
          .setParameter(4, description)
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("configure resource", e);
    }
  }

  @Override
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    if (staleNodeIds.isEmpty()) {
      return 0;
    }
    try {
      int deleted = 0;
      for (int start = 0; start < staleNodeIds.size(); start += PERMIT_CLEANUP_CHUNK_SIZE) {
        deleted +=
            cleanupOrphanedPermitsChunk(
                staleNodeIds.subList(
                    start, Math.min(start + PERMIT_CLEANUP_CHUNK_SIZE, staleNodeIds.size())));
      }
      return deleted;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("cleanup orphaned permits", e);
    }
  }

  private int cleanupOrphanedPermitsChunk(List<String> staleNodeIds) {
    String placeholders = String.join(",", Collections.nCopies(staleNodeIds.size(), "?"));
    // language=SQL Server
    String sql = "DELETE FROM scheduler_resource_permit WHERE node_id IN (" + placeholders + ")";
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (String nodeId : staleNodeIds) {
      query.setParameter(parameter++, nodeId);
    }
    return query.executeUpdate();
  }

  private void prepareCondition(WorkflowConditionEntity condition) {
    if (condition.getId() == null) {
      condition.setId(UuidV7Factory.create());
    }
    if (condition.getCreatedAt() == null) {
      condition.setCreatedAt(Instant.now());
    }
  }

  @SuppressWarnings("unchecked")
  private List<WorkflowConditionEntity> findConditions(
      String whereClause, List<Object> params, String orderClause) {
    // language=SQL Server
    String sqlPrefix =
        """
        SELECT id, parent_job_id, child_job_id, condition_type, condition_expression,
               condition_priority, created_at
        FROM scheduler_workflow_condition
        """;
    Query query = ctx.em().createNativeQuery(sqlPrefix + whereClause + " " + orderClause);
    for (int i = 0; i < params.size(); i++) {
      query.setParameter(i + 1, params.get(i));
    }
    return ((List<Object[]>) query.getResultList())
        .stream().map(SqlserverAuxiliaryOperations::mapCondition).toList();
  }
}

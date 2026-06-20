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

import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
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
import run.ratchet.store.oracle.converter.UuidRawConverter;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.util.RowValues;

final class OracleAuxiliaryOperations
    implements JobAuditStore, WorkflowConditionStore, DlqAlertStore, ResourcePermitStore {

  private static final int PERMIT_CLEANUP_CHUNK_SIZE = 500;

  private final OracleStoreContext ctx;

  OracleAuxiliaryOperations(OracleStoreContext ctx) {
    this.ctx = ctx;
  }

  private static WorkflowConditionEntity mapCondition(Object[] row) {
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setId(OracleJobRowMapper.uuidOrNull(row[0]));
    condition.setParentJobId(OracleJobRowMapper.uuidOrNull(row[1]));
    condition.setChildJobId(OracleJobRowMapper.uuidOrNull(row[2]));
    condition.setConditionType(WorkflowCondition.ConditionType.valueOf(row[3].toString()));
    condition.setConditionExpression(RowValues.stringOrNull(row[4]));
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
  public void appendLog(JobLogEntity log) {
    ctx.em().persist(log);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    // language=JPAQL
    String jpql = "DELETE FROM JobLogEntity l WHERE l.ts < :cutoff";
    return ctx.em().createQuery(jpql).setParameter("cutoff", cutoff).executeUpdate();
  }

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    prepareCondition(condition);
    // language=Oracle
    String sql =
        """
        MERGE INTO scheduler_workflow_condition d
        USING (SELECT ? AS id, ? AS parent_job_id, ? AS child_job_id, ? AS condition_type,
                      ? AS condition_expression, ? AS condition_priority, ? AS created_at
               FROM dual) s
        ON (d.id = s.id)
        WHEN MATCHED THEN UPDATE SET
          d.parent_job_id = s.parent_job_id,
          d.child_job_id = s.child_job_id,
          d.condition_type = s.condition_type,
          d.condition_expression = s.condition_expression,
          d.condition_priority = s.condition_priority,
          d.created_at = s.created_at
        WHEN NOT MATCHED THEN INSERT
          (id, parent_job_id, child_job_id, condition_type, condition_expression,
           condition_priority, created_at)
          VALUES (s.id, s.parent_job_id, s.child_job_id, s.condition_type,
                  s.condition_expression, s.condition_priority, s.created_at)
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidRawConverter.toBytes(condition.getId()))
        .setParameter(2, UuidRawConverter.toBytes(condition.getParentJobId()))
        .setParameter(3, UuidRawConverter.toBytes(condition.getChildJobId()))
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
            List.of(UuidRawConverter.toBytes(id)),
            "ORDER BY condition_priority ASC",
            1);
    return results.isEmpty() ? null : results.get(0);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId) {
    return findConditions(
        "WHERE parent_job_id = ?",
        List.of(UuidRawConverter.toBytes(parentJobId)),
        "ORDER BY condition_priority ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId) {
    return findConditions(
        "WHERE child_job_id = ?",
        List.of(UuidRawConverter.toBytes(childJobId)),
        "ORDER BY condition_priority ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type) {
    return findConditions(
        "WHERE parent_job_id = ? AND condition_type = ?",
        List.of(UuidRawConverter.toBytes(parentJobId), type.name()),
        "ORDER BY condition_priority ASC");
  }

  @Override
  public void deleteConditionById(UUID id) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE id = ?")
        .setParameter(1, UuidRawConverter.toBytes(id))
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByParentJobId(UUID parentJobId) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE parent_job_id = ?")
        .setParameter(1, UuidRawConverter.toBytes(parentJobId))
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByChildJobId(UUID childJobId) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE child_job_id = ?")
        .setParameter(1, UuidRawConverter.toBytes(childJobId))
        .executeUpdate();
  }

  @Override
  public long countConditionsByParentJobId(UUID parentJobId) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_workflow_condition WHERE parent_job_id = ?")
            .setParameter(1, UuidRawConverter.toBytes(parentJobId))
            .getSingleResult();
    return ((Number) result).longValue();
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
            .setParameter("cutoff", cutoff)
            .getSingleResult();
    return count > 0;
  }

  @Override
  public boolean tryAcquirePermit(String resource, UUID jobId, String nodeId) {
    try {
      // Lock the resource-limit row in its own statement so concurrent acquirers for the same
      // resource serialize on it, then let the capacity check ride inside the INSERT rather than a
      // preceding SELECT COUNT. A separate count read can still observe the pre-lock statement
      // snapshot after waiting on FOR UPDATE — EclipseLink on Oracle keeps over-admitting that way
      // —
      // whereas the guarded INSERT ... SELECT re-evaluates the count in the same statement that
      // performs the write. PostgreSQL solves the identical EclipseLink behaviour the same way.
      // language=Oracle
      String lockSql =
          "SELECT resource_name FROM scheduler_resource_limit WHERE resource_name = ? FOR UPDATE";
      @SuppressWarnings("unchecked")
      List<Object> lockedLimits =
          ctx.em().createNativeQuery(lockSql).setParameter(1, resource).getResultList();
      if (lockedLimits.isEmpty()) {
        throw new IllegalArgumentException("Resource is not configured: " + resource);
      }

      // language=Oracle
      String existingForJobSql =
          "SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = ? AND job_id = ?";
      int existingForJob =
          ((Number)
                  ctx.em()
                      .createNativeQuery(existingForJobSql)
                      .setParameter(1, resource)
                      .setParameter(2, UuidRawConverter.toBytes(jobId))
                      .getSingleResult())
              .intValue();
      if (existingForJob > 0) {
        return true;
      }

      // language=Oracle
      String insertSql =
          """
          INSERT INTO scheduler_resource_permit (id, resource_name, job_id, node_id, acquired_at)
          SELECT ?, resource_name, ?, ?, CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          FROM scheduler_resource_limit
          WHERE resource_name = ?
            AND (SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = ?)
                < max_concurrent
          """;
      int inserted =
          ctx.em()
              .createNativeQuery(insertSql)
              .setParameter(1, UuidRawConverter.toBytes(UuidV7Factory.create()))
              .setParameter(2, UuidRawConverter.toBytes(jobId))
              .setParameter(3, nodeId)
              .setParameter(4, resource)
              .setParameter(5, resource)
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
      // language=Oracle
      String sql = "DELETE FROM scheduler_resource_permit WHERE resource_name = ? AND job_id = ?";
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, resource)
          .setParameter(2, UuidRawConverter.toBytes(jobId))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("release resource permit", e);
    }
  }

  @Override
  public void releaseAllPermits(UUID jobId) {
    try {
      // language=Oracle
      String sql = "DELETE FROM scheduler_resource_permit WHERE job_id = ?";
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, UuidRawConverter.toBytes(jobId))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("release all permits", e);
    }
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    try {
      // language=Oracle
      String sql = "SELECT retry_delay_ms FROM scheduler_resource_limit WHERE resource_name = ?";
      return ((Number) ctx.em().createNativeQuery(sql).setParameter(1, resource).getSingleResult())
          .intValue();
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
      // language=Oracle
      String sql =
          """
          MERGE INTO scheduler_resource_limit d
          USING (SELECT ? AS resource_name, ? AS max_concurrent, ? AS retry_delay_ms,
                        ? AS description FROM dual) s
          ON (d.resource_name = s.resource_name)
          WHEN MATCHED THEN UPDATE SET
            d.max_concurrent = s.max_concurrent,
            d.retry_delay_ms = s.retry_delay_ms,
            d.description = s.description,
            d.updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          WHEN NOT MATCHED THEN INSERT
            (resource_name, max_concurrent, retry_delay_ms, description, created_at, updated_at)
            VALUES (s.resource_name, s.max_concurrent, s.retry_delay_ms, s.description,
                    CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP),
                    CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP))
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
    // language=Oracle
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
    return findConditions(whereClause, params, orderClause, 0);
  }

  @SuppressWarnings("unchecked")
  private List<WorkflowConditionEntity> findConditions(
      String whereClause, List<Object> params, String orderClause, int maxResults) {
    // language=Oracle
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
    if (maxResults > 0) {
      query.setMaxResults(maxResults);
    }
    return ((List<Object[]>) query.getResultList())
        .stream().map(OracleAuxiliaryOperations::mapCondition).toList();
  }
}

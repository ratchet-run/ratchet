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
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.util.RowValues;

final class PostgresqlAuxiliaryOperations
    implements JobAuditStore, WorkflowConditionStore, DlqAlertStore, ResourcePermitStore {

  private static final int PERMIT_CLEANUP_CHUNK_SIZE = 500;

  private final PostgresqlStoreContext ctx;

  PostgresqlAuxiliaryOperations(PostgresqlStoreContext ctx) {
    this.ctx = ctx;
  }

  private static WorkflowConditionEntity mapCondition(Object[] row) {
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setId(PostgresqlJobRowMapper.uuidOrNull(row[0]));
    condition.setParentJobId(PostgresqlJobRowMapper.uuidOrNull(row[1]));
    condition.setChildJobId(PostgresqlJobRowMapper.uuidOrNull(row[2]));
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
    return ctx.em().createQuery(jpql).setParameter("cutoff", cutoff).executeUpdate();
  }

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    prepareCondition(condition);
    // language=PostgreSQL
    String sql =
        """
        INSERT INTO scheduler_workflow_condition
          (id, parent_job_id, child_job_id, condition_type, condition_expression,
           condition_priority, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (id) DO UPDATE SET
          parent_job_id = EXCLUDED.parent_job_id,
          child_job_id = EXCLUDED.child_job_id,
          condition_type = EXCLUDED.condition_type,
          condition_expression = EXCLUDED.condition_expression,
          condition_priority = EXCLUDED.condition_priority,
          created_at = EXCLUDED.created_at
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, condition.getId())
        .setParameter(2, condition.getParentJobId())
        .setParameter(3, condition.getChildJobId())
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
        findConditions("WHERE id = ?", List.of(id), "ORDER BY condition_priority ASC");
    return results.isEmpty() ? null : results.get(0);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId) {
    return findConditions(
        "WHERE parent_job_id = ?", List.of(parentJobId), "ORDER BY condition_priority ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId) {
    return findConditions(
        "WHERE child_job_id = ?", List.of(childJobId), "ORDER BY condition_priority ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type) {
    return findConditions(
        "WHERE parent_job_id = ? AND condition_type = ?",
        List.of(parentJobId, type.name()),
        "ORDER BY condition_priority ASC");
  }

  @Override
  public void deleteConditionById(UUID id) {
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_workflow_condition WHERE id = ?";
    ctx.em().createNativeQuery(sql).setParameter(1, id).executeUpdate();
  }

  @Override
  public void deleteConditionsByParentJobId(UUID parentJobId) {
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_workflow_condition WHERE parent_job_id = ?";
    ctx.em().createNativeQuery(sql).setParameter(1, parentJobId).executeUpdate();
  }

  @Override
  public void deleteConditionsByChildJobId(UUID childJobId) {
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_workflow_condition WHERE child_job_id = ?";
    ctx.em().createNativeQuery(sql).setParameter(1, childJobId).executeUpdate();
  }

  @Override
  public long countConditionsByParentJobId(UUID parentJobId) {
    // language=PostgreSQL
    String sql = "SELECT COUNT(*) FROM scheduler_workflow_condition WHERE parent_job_id = ?";
    return ctx.countByNative(sql, parentJobId);
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
      // language=PostgreSQL
      String lockSql =
          """
          SELECT resource_name FROM scheduler_resource_limit
          WHERE resource_name = ?
          FOR UPDATE
          """;
      @SuppressWarnings("unchecked")
      List<Object> lockedLimits =
          ctx.em().createNativeQuery(lockSql).setParameter(1, resource).getResultList();
      if (lockedLimits.isEmpty()) {
        throw new IllegalArgumentException("Resource is not configured: " + resource);
      }

      // language=PostgreSQL
      String existingSql =
          """
          SELECT COUNT(*) FROM scheduler_resource_permit
          WHERE resource_name = ? AND job_id = ?
          """;
      Object existing =
          ctx.em()
              .createNativeQuery(existingSql)
              .setParameter(1, resource)
              .setParameter(2, jobId)
              .getSingleResult();
      if (((Number) existing).intValue() > 0) {
        return true;
      }

      // PostgreSQL uses one statement snapshot even after waiting on FOR UPDATE. Keep the lock
      // acquisition as its own statement, then count and insert together with a fresh snapshot.
      // language=PostgreSQL
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
              .setParameter(1, UuidV7Factory.create())
              .setParameter(2, jobId)
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
      // language=PostgreSQL
      String sql = "DELETE FROM scheduler_resource_permit WHERE resource_name = ? AND job_id = ?";
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, resource)
          .setParameter(2, jobId)
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("release resource permit", e);
    }
  }

  @Override
  public void releaseAllPermits(UUID jobId) {
    try {
      // language=PostgreSQL
      String sql = "DELETE FROM scheduler_resource_permit WHERE job_id = ?";
      ctx.em().createNativeQuery(sql).setParameter(1, jobId).executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("release all permits", e);
    }
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    try {
      // language=PostgreSQL
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
      // language=PostgreSQL
      String sql =
          """
          INSERT INTO scheduler_resource_limit
            (resource_name, max_concurrent, retry_delay_ms, description, created_at, updated_at)
          VALUES (?, ?, ?, ?, statement_timestamp(), statement_timestamp())
          ON CONFLICT (resource_name) DO UPDATE SET
            max_concurrent = EXCLUDED.max_concurrent,
            retry_delay_ms = EXCLUDED.retry_delay_ms,
            description = EXCLUDED.description,
            updated_at = statement_timestamp()
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
    // language=PostgreSQL
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
    // language=PostgreSQL
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
        .stream().map(PostgresqlAuxiliaryOperations::mapCondition).toList();
  }
}

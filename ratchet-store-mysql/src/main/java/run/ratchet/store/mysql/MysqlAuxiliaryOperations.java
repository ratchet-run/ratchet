package run.ratchet.store.mysql;

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
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.ExecutionStore;
import run.ratchet.store.spi.JobLogStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.WorkflowConditionStore;

final class MysqlAuxiliaryOperations
    implements ExecutionStore,
        JobLogStore,
        WorkflowConditionStore,
        DlqAlertStore,
        ResourcePermitStore {

  private static final int PERMIT_CLEANUP_CHUNK_SIZE = 500;

  private final MysqlStoreContext ctx;

  MysqlAuxiliaryOperations(MysqlStoreContext ctx) {
    this.ctx = ctx;
  }

  private static WorkflowConditionEntity mapCondition(Object[] row) {
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setId(MysqlJobRowMapper.uuidOrNull(row[0]));
    condition.setParentJobId(MysqlJobRowMapper.uuidOrNull(row[1]));
    condition.setChildJobId(MysqlJobRowMapper.uuidOrNull(row[2]));
    condition.setConditionType(WorkflowCondition.ConditionType.valueOf(row[3].toString()));
    condition.setConditionExpression(row[4] == null ? null : row[4].toString());
    condition.setConditionPriority(((Number) row[5]).intValue());
    condition.setCreatedAt(MysqlJobRowMapper.toInstant(row[6]));
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
    // language=MySQL
    String sql =
        """
        INSERT INTO scheduler_workflow_condition
          (id, parent_job_id, child_job_id, condition_type, condition_expression,
           condition_priority, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          parent_job_id = VALUES(parent_job_id),
          child_job_id = VALUES(child_job_id),
          condition_type = VALUES(condition_type),
          condition_expression = VALUES(condition_expression),
          condition_priority = VALUES(condition_priority),
          created_at = VALUES(created_at)
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
            "ORDER BY condition_priority ASC",
            1);
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
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE id = ?")
        .setParameter(1, UuidByteArrayConverter.toBytes(id))
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByParentJobId(UUID parentJobId) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE parent_job_id = ?")
        .setParameter(1, UuidByteArrayConverter.toBytes(parentJobId))
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByChildJobId(UUID childJobId) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE child_job_id = ?")
        .setParameter(1, UuidByteArrayConverter.toBytes(childJobId))
        .executeUpdate();
  }

  @Override
  public long countConditionsByParentJobId(UUID parentJobId) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_workflow_condition WHERE parent_job_id = ?")
            .setParameter(1, UuidByteArrayConverter.toBytes(parentJobId))
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
      // language=MySQL
      String sql =
          """
          SELECT max_concurrent,
                 (SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = ?),
                 (SELECT COUNT(*) FROM scheduler_resource_permit
                  WHERE resource_name = ? AND job_id = ?)
          FROM scheduler_resource_limit
          WHERE resource_name = ?
          FOR UPDATE
          """;
      @SuppressWarnings("unchecked")
      List<Object[]> permitResults =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, resource)
              .setParameter(2, resource)
              .setParameter(3, UuidByteArrayConverter.toBytes(jobId))
              .setParameter(4, resource)
              .getResultList();
      Object[] limits = permitResults.stream().findFirst().orElse(null);

      if (limits == null) {
        throw new IllegalArgumentException("Resource is not configured: " + resource);
      }

      int maxConcurrent = ((Number) limits[0]).intValue();
      int active = ((Number) limits[1]).intValue();
      int existingForJob = ((Number) limits[2]).intValue();

      if (existingForJob > 0) {
        return true;
      }
      if (active >= maxConcurrent) {
        return false;
      }

      ResourcePermitEntity permit = ResourcePermitEntity.create(resource, jobId, nodeId);
      ctx.em().persist(permit);
      return true;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("try acquire permit", e);
    }
  }

  @Override
  public void releasePermit(String resource, UUID jobId) {
    try {
      // language=MySQL
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
      // language=MySQL
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
      // language=MySQL
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
      // language=MySQL
      String sql =
          """
          INSERT INTO scheduler_resource_limit
            (resource_name, max_concurrent, retry_delay_ms, description, created_at, updated_at)
          VALUES (?, ?, ?, ?, NOW(3), NOW(3))
          ON DUPLICATE KEY UPDATE
            max_concurrent = VALUES(max_concurrent),
            retry_delay_ms = VALUES(retry_delay_ms),
            description = VALUES(description),
            updated_at = NOW(3)
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
    // language=MySQL
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
    // language=MySQL
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
        .stream().map(MysqlAuxiliaryOperations::mapCondition).toList();
  }
}

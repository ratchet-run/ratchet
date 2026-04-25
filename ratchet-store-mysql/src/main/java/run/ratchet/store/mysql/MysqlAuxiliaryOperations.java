package run.ratchet.store.mysql;

import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.TsidFactory;
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.ExecutionStore;
import run.ratchet.store.spi.JobLogStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

final class MysqlAuxiliaryOperations
    implements ExecutionStore,
        JobLogStore,
        WorkflowConditionStore,
        DlqAlertStore,
        ResourcePermitStore {

  private final MysqlStoreContext ctx;

  MysqlAuxiliaryOperations(MysqlStoreContext ctx) {
    this.ctx = ctx;
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
  public List<JobExecutionEntity> findExecutionsByJobId(long jobId) {
    return ctx.em()
        .createQuery(
            "SELECT e FROM JobExecutionEntity e WHERE e.jobId = :jid ORDER BY e.attempt ASC",
            JobExecutionEntity.class)
        .setParameter("jid", jobId)
        .getResultList();
  }

  @Override
  public Optional<JobExecutionEntity> findLatestExecution(long jobId) {
    List<JobExecutionEntity> results =
        ctx.em()
            .createQuery(
                "SELECT e FROM JobExecutionEntity e WHERE e.jobId = :jid ORDER BY e.attempt DESC",
                JobExecutionEntity.class)
            .setParameter("jid", jobId)
            .setMaxResults(1)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public int countExecutionAttempts(long jobId) {
    return ctx.em()
        .createQuery("SELECT COUNT(e) FROM JobExecutionEntity e WHERE e.jobId = :jid", Long.class)
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
    return ctx.em()
        .createQuery("DELETE FROM JobLogEntity l WHERE l.ts < :cutoff")
        .setParameter("cutoff", cutoff)
        .executeUpdate();
  }

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    prepareCondition(condition);
    ctx.em()
        .createNativeQuery(
            "INSERT INTO scheduler_workflow_condition "
                + "(id, parent_job_id, child_job_id, condition_type, condition_expression, "
                + "condition_priority, created_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE "
                + "parent_job_id = VALUES(parent_job_id), "
                + "child_job_id = VALUES(child_job_id), "
                + "condition_type = VALUES(condition_type), "
                + "condition_expression = VALUES(condition_expression), "
                + "condition_priority = VALUES(condition_priority), "
                + "created_at = VALUES(created_at)")
        .setParameter(1, condition.getId())
        .setParameter(2, condition.getParentJobId())
        .setParameter(3, condition.getChildJobId())
        .setParameter(4, condition.getConditionType().name())
        .setParameter(5, condition.getConditionExpression())
        .setParameter(6, condition.getConditionPriority())
        .setParameter(7, Timestamp.from(condition.getCreatedAt()))
        .executeUpdate();
    WorkflowConditionEntity saved = findConditionById(condition.getId());
    return saved == null ? condition : saved;
  }

  @Override
  public WorkflowConditionEntity findConditionById(long id) {
    List<WorkflowConditionEntity> results =
        findConditions("WHERE id = ?", List.of(id), "ORDER BY condition_priority ASC");
    return results.isEmpty() ? null : results.get(0);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId) {
    return findConditions(
        "WHERE parent_job_id = ?", List.of(parentJobId), "ORDER BY condition_priority ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId) {
    return findConditions(
        "WHERE child_job_id = ?", List.of(childJobId), "ORDER BY condition_priority ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type) {
    return findConditions(
        "WHERE parent_job_id = ? AND condition_type = ?",
        List.of(parentJobId, type.name()),
        "ORDER BY condition_priority ASC");
  }

  @Override
  public void deleteConditionById(long id) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE id = ?")
        .setParameter(1, id)
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByParentJobId(long parentJobId) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE parent_job_id = ?")
        .setParameter(1, parentJobId)
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByChildJobId(long childJobId) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_workflow_condition WHERE child_job_id = ?")
        .setParameter(1, childJobId)
        .executeUpdate();
  }

  @Override
  public long countConditionsByParentJobId(long parentJobId) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_workflow_condition WHERE parent_job_id = ?")
            .setParameter(1, parentJobId)
            .getSingleResult();
    return ((Number) result).longValue();
  }

  private void prepareCondition(WorkflowConditionEntity condition) {
    if (condition.getId() == null || condition.getId() == 0L) {
      condition.setId(TsidFactory.next());
    }
    if (condition.getCreatedAt() == null) {
      condition.setCreatedAt(Instant.now());
    }
  }

  @SuppressWarnings("unchecked")
  private List<WorkflowConditionEntity> findConditions(
      String whereClause, List<Object> params, String orderClause) {
    Query query =
        ctx.em()
            .createNativeQuery(
                "SELECT id, parent_job_id, child_job_id, condition_type, condition_expression, "
                    + "condition_priority, created_at "
                    + "FROM scheduler_workflow_condition "
                    + whereClause
                    + " "
                    + orderClause);
    for (int i = 0; i < params.size(); i++) {
      query.setParameter(i + 1, params.get(i));
    }
    return ((List<Object[]>) query.getResultList())
        .stream().map(MysqlAuxiliaryOperations::mapCondition).toList();
  }

  private static WorkflowConditionEntity mapCondition(Object[] row) {
    WorkflowConditionEntity condition = new WorkflowConditionEntity();
    condition.setId(((Number) row[0]).longValue());
    condition.setParentJobId(((Number) row[1]).longValue());
    condition.setChildJobId(((Number) row[2]).longValue());
    condition.setConditionType(WorkflowCondition.ConditionType.valueOf(row[3].toString()));
    condition.setConditionExpression(row[4] == null ? null : row[4].toString());
    condition.setConditionPriority(((Number) row[5]).intValue());
    condition.setCreatedAt(MysqlJobRowMapper.toInstant(row[6]));
    return condition;
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
  public boolean existsRecentDlqAlert(long jobId, String errorHash, Instant cutoff) {
    Long count =
        ctx.em()
            .createQuery(
                "SELECT COUNT(a) FROM DlqAlertEntity a "
                    + "WHERE a.jobId = :jid AND a.errorHash = :hash AND a.alertSentAt >= :cutoff",
                Long.class)
            .setParameter("jid", jobId)
            .setParameter("hash", errorHash)
            .setParameter("cutoff", cutoff)
            .getSingleResult();
    return count > 0;
  }

  @Override
  public boolean tryAcquirePermit(String resource, long jobId, String nodeId) {
    @SuppressWarnings("unchecked")
    List<Object[]> permitResults =
        ctx.em()
            .createNativeQuery(
                "SELECT max_concurrent, "
                    + "(SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = ?) "
                    + "FROM scheduler_resource_limit WHERE resource_name = ? "
                    + "FOR UPDATE")
            .setParameter(1, resource)
            .setParameter(2, resource)
            .getResultList();
    Object[] limits = permitResults.stream().findFirst().orElse(null);

    if (limits == null) {
      return false;
    }

    int maxConcurrent = ((Number) limits[0]).intValue();
    int active = ((Number) limits[1]).intValue();

    if (active >= maxConcurrent) {
      return false;
    }

    ResourcePermitEntity permit = ResourcePermitEntity.create(resource, jobId, nodeId);
    ctx.em().persist(permit);
    return true;
  }

  @Override
  public void releasePermit(String resource, long jobId) {
    ctx.em()
        .createNativeQuery(
            "DELETE FROM scheduler_resource_permit " + "WHERE resource_name = ? AND job_id = ?")
        .setParameter(1, resource)
        .setParameter(2, jobId)
        .executeUpdate();
  }

  @Override
  public void releaseAllPermits(long jobId) {
    ctx.em()
        .createNativeQuery("DELETE FROM scheduler_resource_permit WHERE job_id = ?")
        .setParameter(1, jobId)
        .executeUpdate();
  }

  @Override
  public int getPermitRetryDelay(String resource) {
    try {
      return ((Number)
              ctx.em()
                  .createNativeQuery(
                      "SELECT retry_delay_ms FROM scheduler_resource_limit WHERE resource_name = ?")
                  .setParameter(1, resource)
                  .getSingleResult())
          .intValue();
    } catch (NoResultException e) {
      return 5000;
    }
  }

  @Override
  public void configureResource(
      String name, int maxConcurrent, int retryDelayMs, String description) {
    ctx.em()
        .createNativeQuery(
            "INSERT INTO scheduler_resource_limit "
                + "(resource_name, max_concurrent, retry_delay_ms, description, created_at, updated_at) "
                + "VALUES (?, ?, ?, ?, NOW(3), NOW(3)) "
                + "ON DUPLICATE KEY UPDATE "
                + "max_concurrent = VALUES(max_concurrent), "
                + "retry_delay_ms = VALUES(retry_delay_ms), "
                + "description = VALUES(description), "
                + "updated_at = NOW(3)")
        .setParameter(1, name)
        .setParameter(2, maxConcurrent)
        .setParameter(3, retryDelayMs)
        .setParameter(4, description)
        .executeUpdate();
  }

  @Override
  public int cleanupOrphanedPermits(List<String> staleNodeIds) {
    if (staleNodeIds.isEmpty()) {
      return 0;
    }
    String placeholders = String.join(",", Collections.nCopies(staleNodeIds.size(), "?"));
    Query query =
        ctx.em()
            .createNativeQuery(
                "DELETE FROM scheduler_resource_permit WHERE node_id IN (" + placeholders + ")");
    int parameter = 1;
    for (String nodeId : staleNodeIds) {
      query.setParameter(parameter++, nodeId);
    }
    return query.executeUpdate();
  }
}

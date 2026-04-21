package run.ratchet.store.postgresql;

import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.DlqAlertEntity;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
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

final class PostgresqlAuxiliaryOperations
    implements ExecutionStore,
        JobLogStore,
        WorkflowConditionStore,
        DlqAlertStore,
        ResourcePermitStore {

  private final PostgresqlStoreContext ctx;

  PostgresqlAuxiliaryOperations(PostgresqlStoreContext ctx) {
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
  @SuppressWarnings("unchecked")
  public List<JobExecutionEntity> findExecutionsByJobId(long jobId) {
    return ctx.em()
        .createNativeQuery(
            "SELECT * FROM scheduler_job_execution WHERE job_id = ? ORDER BY attempt ASC",
            JobExecutionEntity.class)
        .setParameter(1, jobId)
        .getResultList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Optional<JobExecutionEntity> findLatestExecution(long jobId) {
    List<JobExecutionEntity> results =
        ctx.em()
            .createNativeQuery(
                "SELECT * FROM scheduler_job_execution WHERE job_id = ? ORDER BY attempt DESC LIMIT 1",
                JobExecutionEntity.class)
            .setParameter(1, jobId)
            .getResultList();
    return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
  }

  @Override
  public int countExecutionAttempts(long jobId) {
    return ((Number)
            ctx.em()
                .createNativeQuery("SELECT COUNT(*) FROM scheduler_job_execution WHERE job_id = ?")
                .setParameter(1, jobId)
                .getSingleResult())
        .intValue();
  }

  @Override
  public void appendLog(JobLogEntity logEntry) {
    ctx.em().persist(logEntry);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    return ctx.em()
        .createNativeQuery("DELETE FROM scheduler_job_log WHERE ts < ?")
        .setParameter(1, Timestamp.from(cutoff))
        .executeUpdate();
  }

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    if (condition.getId() == null) {
      ctx.em().persist(condition);
      return condition;
    }
    return ctx.em().merge(condition);
  }

  @Override
  public WorkflowConditionEntity findConditionById(long id) {
    return ctx.em().find(WorkflowConditionEntity.class, id);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId) {
    return ctx.em()
        .createNativeQuery(
            "SELECT * FROM scheduler_workflow_condition WHERE parent_job_id = ? "
                + "ORDER BY condition_priority ASC",
            WorkflowConditionEntity.class)
        .setParameter(1, parentJobId)
        .getResultList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId) {
    return ctx.em()
        .createNativeQuery(
            "SELECT * FROM scheduler_workflow_condition WHERE child_job_id = ?",
            WorkflowConditionEntity.class)
        .setParameter(1, childJobId)
        .getResultList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type) {
    return ctx.em()
        .createNativeQuery(
            "SELECT * FROM scheduler_workflow_condition "
                + "WHERE parent_job_id = ? AND condition_type = ?",
            WorkflowConditionEntity.class)
        .setParameter(1, parentJobId)
        .setParameter(2, type.name())
        .getResultList();
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
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_workflow_condition WHERE parent_job_id = ?", parentJobId);
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
    long count =
        ctx.countByNative(
            "SELECT COUNT(*) FROM scheduler_dlq_alerts "
                + "WHERE job_id = ? AND error_hash = ? AND alert_sent_at >= ?",
            jobId,
            errorHash,
            Timestamp.from(cutoff));
    return count > 0;
  }

  @Override
  public boolean tryAcquirePermit(String resource, long jobId, String nodeId) {
    Object[] limitRow;
    try {
      limitRow =
          (Object[])
              ctx.em()
                  .createNativeQuery(
                      "SELECT max_concurrent, retry_delay_ms FROM scheduler_resource_limit "
                          + "WHERE resource_name = ? FOR UPDATE")
                  .setParameter(1, resource)
                  .getSingleResult();
    } catch (NoResultException e) {
      return false;
    }

    int maxConcurrent = ((Number) limitRow[0]).intValue();
    long activeCount =
        ctx.countByNative(
            "SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = ?", resource);

    if (activeCount >= maxConcurrent) {
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
            "DELETE FROM scheduler_resource_permit WHERE resource_name = ? AND job_id = ?")
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
      Object result =
          ctx.em()
              .createNativeQuery(
                  "SELECT retry_delay_ms FROM scheduler_resource_limit WHERE resource_name = ?")
              .setParameter(1, resource)
              .getSingleResult();
      return ((Number) result).intValue();
    } catch (NoResultException e) {
      return 5000;
    }
  }

  @Override
  public void configureResource(
      String name, int maxConcurrent, int retryDelayMs, String description) {
    ctx.em()
        .createNativeQuery(
            "INSERT INTO scheduler_resource_limit (resource_name, max_concurrent, retry_delay_ms, "
                + "description, created_at, updated_at) "
                + "VALUES (?, ?, ?, ?, statement_timestamp(), statement_timestamp()) "
                + "ON CONFLICT (resource_name) DO UPDATE SET "
                + "  max_concurrent = EXCLUDED.max_concurrent, "
                + "  retry_delay_ms = EXCLUDED.retry_delay_ms, "
                + "  description = EXCLUDED.description, "
                + "  updated_at = statement_timestamp()")
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

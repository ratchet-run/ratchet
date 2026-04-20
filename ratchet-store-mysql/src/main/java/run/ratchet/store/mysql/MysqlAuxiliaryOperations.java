package run.ratchet.store.mysql;

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
  public List<WorkflowConditionEntity> findConditionsByParentJobId(long parentJobId) {
    return ctx.em()
        .createQuery(
            "SELECT c FROM WorkflowConditionEntity c WHERE c.parentJobId = :pid "
                + "ORDER BY c.conditionPriority ASC",
            WorkflowConditionEntity.class)
        .setParameter("pid", parentJobId)
        .getResultList();
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(long childJobId) {
    return ctx.em()
        .createQuery(
            "SELECT c FROM WorkflowConditionEntity c WHERE c.childJobId = :cid",
            WorkflowConditionEntity.class)
        .setParameter("cid", childJobId)
        .getResultList();
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      long parentJobId, WorkflowCondition.ConditionType type) {
    return ctx.em()
        .createQuery(
            "SELECT c FROM WorkflowConditionEntity c WHERE c.parentJobId = :pid "
                + "AND c.conditionType = :type ORDER BY c.conditionPriority ASC",
            WorkflowConditionEntity.class)
        .setParameter("pid", parentJobId)
        .setParameter("type", type)
        .getResultList();
  }

  @Override
  public void deleteConditionById(long id) {
    WorkflowConditionEntity entity = ctx.em().find(WorkflowConditionEntity.class, id);
    if (entity != null) {
      ctx.em().remove(entity);
    }
  }

  @Override
  public void deleteConditionsByParentJobId(long parentJobId) {
    ctx.em()
        .createQuery("DELETE FROM WorkflowConditionEntity c WHERE c.parentJobId = :pid")
        .setParameter("pid", parentJobId)
        .executeUpdate();
  }

  @Override
  public void deleteConditionsByChildJobId(long childJobId) {
    ctx.em()
        .createQuery("DELETE FROM WorkflowConditionEntity c WHERE c.childJobId = :cid")
        .setParameter("cid", childJobId)
        .executeUpdate();
  }

  @Override
  public long countConditionsByParentJobId(long parentJobId) {
    return ctx.em()
        .createQuery(
            "SELECT COUNT(c) FROM WorkflowConditionEntity c WHERE c.parentJobId = :pid", Long.class)
        .setParameter("pid", parentJobId)
        .getSingleResult();
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

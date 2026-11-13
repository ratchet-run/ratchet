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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Nullable;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.oracle.converter.UuidRawConverter;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.ResourceWaiterLease;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.util.RowValues;
import run.ratchet.store.util.WorkflowConditionOrdering;

final class OracleAuxiliaryOperations
    implements JobAuditStore, WorkflowConditionStore, ResourcePermitStore {

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
    condition.setDefinitionOrder(((Number) row[6]).intValue());
    condition.setCreatedAt(RowValues.instantOrNull(row[7]));
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
    if (limit == 0) {
      return List.of();
    }
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
    // ts is TIMESTAMP(6); floor the cutoff to microseconds so the boundary matches the column
    // precision rather than Oracle's nanosecond-precision bind.
    return ctx.em()
        .createQuery(jpql)
        .setParameter("cutoff", OracleTimestamps.floorMicros(cutoff))
        .executeUpdate();
  }

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    prepareCondition(condition);
    // Oracle cannot bind a value over 32 KB inside a MERGE, so use update-then-insert.
    if (updateCondition(condition) == 0) {
      // language=Oracle
      String insertSql =
          """
          INSERT INTO scheduler_workflow_condition
            (parent_job_id, child_job_id, condition_type, condition_expression,
             condition_priority, definition_order, created_at, id)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          """;
      bindCondition(insertSql, condition).executeUpdate();
    }
    return condition;
  }

  private int updateCondition(WorkflowConditionEntity condition) {
    // language=Oracle
    String sql =
        """
        UPDATE scheduler_workflow_condition
        SET parent_job_id = ?, child_job_id = ?, condition_type = ?, condition_expression = ?,
            condition_priority = ?, definition_order = ?, created_at = ?
        WHERE id = ?
        """;
    return bindCondition(sql, condition).executeUpdate();
  }

  private Query bindCondition(String sql, WorkflowConditionEntity condition) {
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidRawConverter.toBytes(condition.getParentJobId()))
        .setParameter(2, UuidRawConverter.toBytes(condition.getChildJobId()))
        .setParameter(3, condition.getConditionType().name())
        .setParameter(4, condition.getConditionExpression())
        .setParameter(5, condition.getConditionPriority())
        .setParameter(6, condition.getDefinitionOrder())
        .setParameter(7, Timestamp.from(condition.getCreatedAt()))
        .setParameter(8, UuidRawConverter.toBytes(condition.getId()));
  }

  @Override
  public WorkflowConditionEntity findConditionById(UUID id) {
    List<WorkflowConditionEntity> results =
        findConditions(
            "WHERE id = ?",
            List.of(UuidRawConverter.toBytes(id)),
            "ORDER BY condition_priority ASC, definition_order ASC",
            1);
    return results.isEmpty() ? null : results.get(0);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId) {
    return findConditions(
        "WHERE parent_job_id = ?",
        List.of(UuidRawConverter.toBytes(parentJobId)),
        "ORDER BY condition_priority ASC, definition_order ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId) {
    return findConditions(
        "WHERE child_job_id = ?",
        List.of(UuidRawConverter.toBytes(childJobId)),
        "ORDER BY condition_priority ASC, definition_order ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type) {
    return findConditions(
        "WHERE parent_job_id = ? AND condition_type = ?",
        List.of(UuidRawConverter.toBytes(parentJobId), type.name()),
        "ORDER BY condition_priority ASC, definition_order ASC");
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
  public AcquireResult tryAcquirePermit(String resource, UUID jobId, long claimSeq, String nodeId) {
    try {
      // language=Oracle
      String sql =
          """
          SELECT max_concurrent, retry_delay_ms FROM scheduler_resource_limit
          WHERE resource_name = ? FOR UPDATE
          """;
      List<?> limits = ctx.em().createNativeQuery(sql).setParameter(1, resource).getResultList();
      if (limits.isEmpty()) {
        throw new IllegalArgumentException("Resource is not configured: " + resource);
      }
      // READ COMMITTED reads the latest committed claim after the resource lock.
      // language=Oracle
      List<?> claims =
          ctx.em()
              .createNativeQuery(
                  """
                  SELECT q.status, q.claim_seq, q.priority, q.scheduled_time,
                         w.priority, w.wait_since
                  FROM scheduler_job_queue q
                  LEFT JOIN scheduler_resource_waiter w ON w.job_id = q.job_id
                  WHERE q.job_id = ?
                  """)
              .setParameter(1, UuidRawConverter.toBytes(jobId))
              .getResultList();
      if (claims.isEmpty()) {
        return AcquireResult.SUPERSEDED;
      }
      Object[] claim = (Object[]) claims.get(0);
      if (!"RUNNING".equals(claim[0].toString()) || ((Number) claim[1]).longValue() != claimSeq) {
        return AcquireResult.SUPERSEDED;
      }

      Object[] limit = (Object[]) limits.get(0);
      int maxConcurrent = ((Number) limit[0]).intValue();
      int retryDelayMs = ((Number) limit[1]).intValue();
      // The resource row serializes acquisitions; this plain read has a fresh snapshot.
      // language=Oracle
      List<?> permits =
          ctx.em()
              .createNativeQuery(
                  """
                  SELECT id FROM scheduler_resource_permit
                  WHERE resource_name = ?
                  """)
              .setParameter(1, resource)
              .getResultList();
      int active = permits.size();
      UUID permitId = ResourcePermitEntity.idFor(resource, jobId, claimSeq);
      for (Object row : permits) {
        if (permitId.equals(OracleJobRowMapper.uuidOrNull(row))) {
          return AcquireResult.ACQUIRED;
        }
      }
      int free = maxConcurrent - active;
      if (free <= 0 || waitersAhead(resource, jobId, claim, free) >= free) {
        return parkWaiter(resource, jobId, claimSeq, claim, retryDelayMs);
      }

      // Retain the capacity guard on the write as a final safety check.
      // language=Oracle
      String insertSql =
          """
          INSERT INTO scheduler_resource_permit (id, resource_name, job_id, node_id, acquired_at)
          SELECT ?, resource_name, ?, ?, CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          FROM scheduler_resource_limit
          WHERE resource_name = ?
            AND (SELECT COUNT(*) FROM scheduler_resource_permit
                 WHERE resource_name = ?) < max_concurrent
          """;
      int inserted =
          ctx.em()
              .createNativeQuery(insertSql)
              .setParameter(
                  1,
                  UuidRawConverter.toBytes(ResourcePermitEntity.idFor(resource, jobId, claimSeq)))
              .setParameter(2, UuidRawConverter.toBytes(jobId))
              .setParameter(3, nodeId)
              .setParameter(4, resource)
              .setParameter(5, resource)
              .executeUpdate();
      if (inserted == 0) {
        return parkWaiter(resource, jobId, claimSeq, claim, retryDelayMs);
      }
      if (claim[4] != null) {
        deleteWaiter(jobId);
      }
      return AcquireResult.ACQUIRED;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("try acquire permit", e);
    }
  }

  private int waitersAhead(String resource, UUID jobId, Object[] claim, int free) {
    int priority = ((Number) (claim[4] != null ? claim[4] : claim[2])).intValue();
    Object waitSince = claim[4] != null ? claim[5] : claim[3];
    // language=Oracle
    String sql =
        """
        SELECT w.job_id FROM scheduler_resource_waiter w
        JOIN scheduler_job_queue q ON q.job_id = w.job_id
        WHERE w.resource_name = ? AND w.lease_until > CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          AND q.status IN ('PENDING', 'RUNNING') AND w.job_id <> ?
          AND (w.priority > ? OR (w.priority = ? AND w.wait_since < ?)
            OR (w.priority = ? AND w.wait_since = ? AND w.job_id < ?))
        ORDER BY w.priority DESC, w.wait_since ASC, w.job_id ASC FETCH FIRST ? ROWS ONLY
        """;
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, resource)
        .setParameter(2, UuidRawConverter.toBytes(jobId))
        .setParameter(3, priority)
        .setParameter(4, priority)
        .setParameter(5, waitSince)
        .setParameter(6, priority)
        .setParameter(7, waitSince)
        .setParameter(8, UuidRawConverter.toBytes(jobId))
        .setParameter(9, free)
        .getResultList()
        .size();
  }

  private AcquireResult parkWaiter(
      String resource, UUID jobId, long claimSeq, Object[] claim, int retryDelayMs) {
    boolean paused = OracleJobTerminalOperations.retryIntoPausedBatch(ctx, jobId);
    // language=Oracle
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = ?, paused_from_status = ?, last_error = ?,
            scheduled_time = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) + NUMTODSINTERVAL(? / 1000, 'SECOND'),
            picked_by = NULL, picked_at = NULL, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
        WHERE job_id = ? AND status = 'RUNNING' AND claim_seq = ?
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, paused ? "PAUSED" : "PENDING")
            .setParameter(2, paused ? "PENDING" : null)
            .setParameter(3, "Waiting for resource: " + resource)
            .setParameter(4, retryDelayMs)
            .setParameter(5, UuidRawConverter.toBytes(jobId))
            .setParameter(6, claimSeq)
            .executeUpdate();
    if (updated == 0) {
      return AcquireResult.SUPERSEDED;
    }
    long leaseMillis = ResourceWaiterLease.leaseMillis(retryDelayMs);
    if (claim[4] != null) {
      // language=Oracle
      String renew =
          """
          UPDATE scheduler_resource_waiter
          SET lease_until = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) + NUMTODSINTERVAL(? / 1000, 'SECOND') WHERE job_id = ?
          """;
      ctx.em()
          .createNativeQuery(renew)
          .setParameter(1, leaseMillis)
          .setParameter(2, UuidRawConverter.toBytes(jobId))
          .executeUpdate();
    } else {
      // language=Oracle
      String insert =
          """
          INSERT INTO scheduler_resource_waiter
            (job_id, resource_name, priority, wait_since, lease_until)
          VALUES (?, ?, ?, ?, CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) + NUMTODSINTERVAL(? / 1000, 'SECOND'))
          """;
      ctx.em()
          .createNativeQuery(insert)
          .setParameter(1, UuidRawConverter.toBytes(jobId))
          .setParameter(2, resource)
          .setParameter(3, claim[2])
          .setParameter(4, claim[3])
          .setParameter(5, leaseMillis)
          .executeUpdate();
    }
    return AcquireResult.AT_CAPACITY;
  }

  private int deleteWaiter(UUID jobId) {
    // language=Oracle
    String sql =
        """
        DELETE FROM scheduler_resource_waiter WHERE job_id = ?
        """;
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidRawConverter.toBytes(jobId))
        .executeUpdate();
  }

  @Override
  public List<WokenWaiter> wakeWaiters(String resource) {
    try {
      // language=Oracle
      String lock =
          """
          SELECT max_concurrent, retry_delay_ms FROM scheduler_resource_limit
          WHERE resource_name = ? FOR UPDATE
          """;
      List<?> limits = ctx.em().createNativeQuery(lock).setParameter(1, resource).getResultList();
      if (limits.isEmpty()) {
        return List.of();
      }
      int max = ((Number) ((Object[]) limits.get(0))[0]).intValue();
      // language=Oracle
      String heads =
          """
          SELECT w.job_id, q.status, w.priority, q.execution_target, w.wait_since, q.scheduled_time
          FROM scheduler_resource_waiter w JOIN scheduler_job_queue q ON q.job_id = w.job_id
          WHERE w.resource_name = ? AND w.lease_until > CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
            AND q.status IN ('PENDING', 'RUNNING')
          ORDER BY w.priority DESC, w.wait_since ASC, w.job_id ASC FETCH FIRST ? ROWS ONLY
          """;
      List<?> rows =
          ctx.em()
              .createNativeQuery(heads)
              .setParameter(1, resource)
              .setParameter(2, max)
              .getResultList();
      if (rows.isEmpty()) {
        return List.of();
      }
      // READ COMMITTED sees committed permits after locking the resource.
      // language=Oracle
      String held =
          """
          SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = ?
          """;
      int free =
          max
              - ((Number)
                      ctx.em().createNativeQuery(held).setParameter(1, resource).getSingleResult())
                  .intValue();
      if (free <= 0) {
        return List.of();
      }
      List<Object[]> pending = new ArrayList<>();
      Map<UUID, Integer> order = new HashMap<>();
      for (Object row : rows.subList(0, Math.min(free, rows.size()))) {
        Object[] head = (Object[]) row;
        order.put(OracleJobRowMapper.uuidOrNull(head[0]), order.size());
        if ("PENDING".equals(head[1].toString())) {
          pending.add(head);
        }
      }
      if (pending.isEmpty()) {
        return List.of();
      }
      String placeholders = String.join(", ", Collections.nCopies(pending.size(), "?"));
      // language=Oracle
      String available =
          """
          SELECT job_id, scheduled_time FROM scheduler_job_queue
          WHERE job_id IN (%s) AND status = 'PENDING' FOR UPDATE SKIP LOCKED
          """
              .formatted(placeholders);
      Query availableQuery = ctx.em().createNativeQuery(available);
      for (int i = 0; i < pending.size(); i++) {
        availableQuery.setParameter(i + 1, pending.get(i)[0]);
      }
      List<?> availableRows = availableQuery.getResultList();
      Map<UUID, Instant> locked = new HashMap<>();
      for (Object availableRow : availableRows) {
        Object[] lockedRow = (Object[]) availableRow;
        locked.put(
            OracleJobRowMapper.uuidOrNull(lockedRow[0]), RowValues.instantOrNull(lockedRow[1]));
      }
      List<WokenWaiter> result = new ArrayList<>();
      for (Object[] row : pending) {
        // Skip a head whose due time changed after the line was read, as a user reschedule does.
        UUID jobId = OracleJobRowMapper.uuidOrNull(row[0]);
        if (!locked.containsKey(jobId)
            || !Objects.equals(RowValues.instantOrNull(row[5]), locked.get(jobId))) {
          continue;
        }
        // language=Oracle
        String wake =
            """
            UPDATE scheduler_job_queue SET scheduled_time = ?, updated_at = CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
            WHERE job_id = ? AND status = 'PENDING'
            """;
        if (ctx.em()
                .createNativeQuery(wake)
                .setParameter(1, row[4])
                .setParameter(2, row[0])
                .executeUpdate()
            == 1) {
          result.add(
              new WokenWaiter(
                  OracleJobRowMapper.uuidOrNull(row[0]),
                  ((Number) row[2]).intValue(),
                  (String) row[3]));
        }
      }
      // Preserve waiter order among the heads whose queue rows were available.
      result.sort(Comparator.comparingInt(w -> order.get(w.jobId())));
      return result;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("wake resource waiters", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> findResourcesWithWaiters(@Nullable String after, int limit) {
    // language=Oracle
    String sql =
        """
        SELECT DISTINCT resource_name FROM scheduler_resource_waiter
        ORDER BY resource_name FETCH FIRST ? ROWS ONLY
        """;
    if (after != null) {
      // language=Oracle
      sql =
          """
          SELECT DISTINCT resource_name FROM scheduler_resource_waiter
          WHERE resource_name > ?
          ORDER BY resource_name FETCH FIRST ? ROWS ONLY
          """;
    }
    try {
      Query query = ctx.em().createNativeQuery(sql);
      if (after != null) {
        query.setParameter(1, after);
        query.setParameter(2, limit);
      } else {
        query.setParameter(1, limit);
      }
      return query.getResultList();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find resources with waiters", e);
    }
  }

  @Override
  public int deleteStaleWaiters(int limit) {
    // language=Oracle
    String sql =
        """
        SELECT w.job_id FROM scheduler_resource_waiter w
        LEFT JOIN scheduler_job_queue q ON q.job_id = w.job_id
        WHERE q.job_id IS NULL OR q.status NOT IN ('PENDING', 'RUNNING', 'PAUSED') FETCH FIRST ? ROWS ONLY
        """;
    try {
      List<?> ids = ctx.em().createNativeQuery(sql).setParameter(1, limit).getResultList();
      int deleted = 0;
      for (Object id : ids) {
        deleted += deleteWaiter(OracleJobRowMapper.uuidOrNull(id));
      }
      return deleted;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete stale resource waiters", e);
    }
  }

  @Override
  public void releasePermit(String resource, UUID jobId, long claimSeq) {
    try {
      // language=Oracle
      String sql =
          """
          DELETE FROM scheduler_resource_permit WHERE id = ? AND resource_name = ? AND job_id = ?
          """;
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(
              1, UuidRawConverter.toBytes(ResourcePermitEntity.idFor(resource, jobId, claimSeq)))
          .setParameter(2, resource)
          .setParameter(3, UuidRawConverter.toBytes(jobId))
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
               condition_priority, definition_order, created_at
        FROM scheduler_workflow_condition
        """;
    Query query = ctx.em().createNativeQuery(sqlPrefix + whereClause + " " + orderClause);
    for (int i = 0; i < params.size(); i++) {
      query.setParameter(i + 1, params.get(i));
    }
    if (maxResults > 0) {
      query.setMaxResults(maxResults);
    }
    List<WorkflowConditionEntity> conditions =
        ((List<Object[]>) query.getResultList())
            .stream().map(OracleAuxiliaryOperations::mapCondition).toList();
    return WorkflowConditionOrdering.sorted(conditions);
  }
}

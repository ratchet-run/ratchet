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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.Nullable;
import run.ratchet.api.ResourceLimit;
import run.ratchet.api.WorkflowCondition;
import run.ratchet.api.exception.ResourceNotConfiguredException;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.entity.ResourceLimitEntity;
import run.ratchet.store.entity.ResourcePermitEntity;
import run.ratchet.store.entity.WorkflowConditionEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.store.spi.ResourceWaiterLease;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.RowValues;
import run.ratchet.store.util.WorkflowConditionOrdering;

final class SqlserverAuxiliaryOperations
    implements JobAuditStore, WorkflowConditionStore, ResourcePermitStore {

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
  public void appendLog(JobLogEntity logEntry) {
    ctx.em().persist(logEntry);
  }

  @Override
  public int purgeLogsOlderThan(Instant cutoff) {
    // language=JPAQL
    String jpql = "DELETE FROM JobLogEntity l WHERE l.ts < :cutoff";
    // ts is DATETIME2(6); floor the cutoff to microseconds so the boundary matches the column
    // precision rather than mssql-jdbc's nanosecond-precision bind.
    return ctx.em()
        .createQuery(jpql)
        .setParameter("cutoff", SqlserverTimestamps.floorMicros(cutoff))
        .executeUpdate();
  }

  @Override
  public WorkflowConditionEntity saveCondition(WorkflowConditionEntity condition) {
    prepareCondition(condition);
    // language=SQL Server
    String sql =
        """
        MERGE scheduler_workflow_condition WITH (HOLDLOCK) AS tgt
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?))
          AS src(id, parent_job_id, child_job_id, condition_type, condition_expression,
                 condition_priority, definition_order, created_at)
          ON tgt.id = src.id
        WHEN MATCHED THEN UPDATE SET
          parent_job_id = src.parent_job_id,
          child_job_id = src.child_job_id,
          condition_type = src.condition_type,
          condition_expression = src.condition_expression,
          condition_priority = src.condition_priority,
          definition_order = src.definition_order,
          created_at = src.created_at
        WHEN NOT MATCHED THEN INSERT
          (id, parent_job_id, child_job_id, condition_type, condition_expression,
           condition_priority, definition_order, created_at)
          VALUES (src.id, src.parent_job_id, src.child_job_id, src.condition_type,
                  src.condition_expression, src.condition_priority, src.definition_order,
                  src.created_at);
        """;
    ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(condition.getId()))
        .setParameter(2, UuidByteArrayConverter.toBytes(condition.getParentJobId()))
        .setParameter(3, UuidByteArrayConverter.toBytes(condition.getChildJobId()))
        .setParameter(4, condition.getConditionType().name())
        .setParameter(5, condition.getConditionExpression())
        .setParameter(6, condition.getConditionPriority())
        .setParameter(7, condition.getDefinitionOrder())
        .setParameter(8, Timestamp.from(condition.getCreatedAt()))
        .executeUpdate();
    return condition;
  }

  @Override
  public WorkflowConditionEntity findConditionById(UUID id) {
    List<WorkflowConditionEntity> results =
        findConditions(
            "WHERE id = ?",
            List.of(UuidByteArrayConverter.toBytes(id)),
            "ORDER BY condition_priority ASC, definition_order ASC");
    return results.isEmpty() ? null : results.get(0);
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByParentJobId(UUID parentJobId) {
    return findConditions(
        "WHERE parent_job_id = ?",
        List.of(UuidByteArrayConverter.toBytes(parentJobId)),
        "ORDER BY condition_priority ASC, definition_order ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByChildJobId(UUID childJobId) {
    return findConditions(
        "WHERE child_job_id = ?",
        List.of(UuidByteArrayConverter.toBytes(childJobId)),
        "ORDER BY condition_priority ASC, definition_order ASC");
  }

  @Override
  public List<WorkflowConditionEntity> findConditionsByType(
      UUID parentJobId, WorkflowCondition.ConditionType type) {
    return findConditions(
        "WHERE parent_job_id = ? AND condition_type = ?",
        List.of(UuidByteArrayConverter.toBytes(parentJobId), type.name()),
        "ORDER BY condition_priority ASC, definition_order ASC");
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
    // Cast to Object so the byte[] is bound as a single varargs parameter, not spread.
    return ctx.countByNative(sql, (Object) UuidByteArrayConverter.toBytes(parentJobId));
  }

  @Override
  public AcquireResult tryAcquirePermit(String resource, UUID jobId, long claimSeq, String nodeId) {
    try {
      // language=SQL Server
      String lockSql =
          """
          SELECT max_concurrent, retry_delay_ms FROM scheduler_resource_limit WITH (UPDLOCK, HOLDLOCK, ROWLOCK)
          WHERE resource_name = ?
          """;
      @SuppressWarnings("unchecked")
      List<Object> lockedLimits =
          ctx.em().createNativeQuery(lockSql).setParameter(1, resource).getResultList();
      if (lockedLimits.isEmpty()) {
        throw new ResourceNotConfiguredException(resource);
      }

      // Plain RCSI reads get a fresh statement snapshot after the resource lock.
      // language=SQL Server
      List<?> claims =
          ctx.em()
              .createNativeQuery(
                  """
                  SELECT q.status, q.claim_seq, q.priority, q.scheduled_time,
                         w.priority, w.wait_since, w.resource_name
                  FROM scheduler_job_queue q
                  LEFT JOIN scheduler_resource_waiter w ON w.job_id = q.job_id
                  WHERE q.job_id = ?
                  """)
              .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
              .getResultList();
      if (claims.isEmpty()) {
        return AcquireResult.SUPERSEDED;
      }
      Object[] claim = (Object[]) claims.get(0);
      if (!"RUNNING".equals(claim[0].toString()) || ((Number) claim[1]).longValue() != claimSeq) {
        return AcquireResult.SUPERSEDED;
      }

      Object[] limit = (Object[]) lockedLimits.get(0);
      int maxConcurrent = ((Number) limit[0]).intValue();
      int retryDelayMs = ((Number) limit[1]).intValue();
      // language=SQL Server
      int updated =
          ctx.em()
              .createNativeQuery(
                  """
                  UPDATE scheduler_resource_permit SET node_id = ?, acquired_at = SYSUTCDATETIME()
                  WHERE id = ?
                  """)
              .setParameter(1, nodeId)
              .setParameter(
                  2,
                  UuidByteArrayConverter.toBytes(
                      ResourcePermitEntity.idFor(resource, jobId, claimSeq)))
              .executeUpdate();
      if (updated > 0) {
        return AcquireResult.ACQUIRED;
      }

      int free = maxConcurrent - countPermits(resource);
      if (free <= 0 || waitersAhead(resource, jobId, claim, free) >= free) {
        return parkWaiter(resource, jobId, claimSeq, claim, retryDelayMs);
      }

      // SQL Server uses one statement snapshot even after waiting on a resource lock. Keep the lock
      // acquisition as its own statement, then count and insert together with a fresh snapshot.
      // language=SQL Server
      String insertSql =
          """
          INSERT INTO scheduler_resource_permit
            (id, resource_name, job_id, node_id, acquired_at)
          SELECT ?, resource_name, ?, ?, SYSUTCDATETIME()
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
              .setParameter(
                  1,
                  UuidByteArrayConverter.toBytes(
                      ResourcePermitEntity.idFor(resource, jobId, claimSeq)))
              .setParameter(2, UuidByteArrayConverter.toBytes(jobId))
              .setParameter(3, nodeId)
              .setParameter(4, resource)
              .setParameter(5, resource)
              .executeUpdate();
      if (inserted == 0) {
        return parkWaiter(resource, jobId, claimSeq, claim, retryDelayMs);
      }
      if (claim[4] != null) {
        // The claim may have moved to another resource and parked there since the claim read.
        deleteWaiter(resource, jobId);
      }
      return AcquireResult.ACQUIRED;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("try acquire permit", e);
    }
  }

  private int countPermits(String resource) {
    // language=SQL Server
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_resource_permit WHERE resource_name = ?
        """;
    return ((Number) ctx.em().createNativeQuery(sql).setParameter(1, resource).getSingleResult())
        .intValue();
  }

  private int waitersAhead(String resource, UUID jobId, Object[] claim, int free) {
    int priority =
        ((Number) (claim[4] != null && resource.equals(claim[6]) ? claim[4] : claim[2])).intValue();
    Object waitSince = claim[4] != null && resource.equals(claim[6]) ? claim[5] : claim[3];
    // language=SQL Server
    String sql =
        """
        SELECT w.job_id FROM scheduler_resource_waiter w
        JOIN scheduler_job_queue q ON q.job_id = w.job_id
        WHERE w.resource_name = ? AND w.lease_until > SYSUTCDATETIME()
          AND q.status IN ('PENDING', 'RUNNING') AND w.job_id <> ?
          AND (w.priority > ? OR (w.priority = ? AND w.wait_since < ?)
            OR (w.priority = ? AND w.wait_since = ? AND w.job_id < ?))
        ORDER BY w.priority DESC, w.wait_since ASC, w.job_id ASC
        OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """;
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, resource)
        .setParameter(2, UuidByteArrayConverter.toBytes(jobId))
        .setParameter(3, priority)
        .setParameter(4, priority)
        .setParameter(5, waitSince)
        .setParameter(6, priority)
        .setParameter(7, waitSince)
        .setParameter(8, UuidByteArrayConverter.toBytes(jobId))
        .setParameter(9, free)
        .getResultList()
        .size();
  }

  private AcquireResult parkWaiter(
      String resource, UUID jobId, long claimSeq, Object[] claim, int retryDelayMs) {
    boolean paused = SqlserverJobTerminalOperations.retryIntoPausedBatch(ctx, jobId);
    // SYSUTCDATETIME() has seven fractional digits and scheduled_time six. Storing it can round up
    // past a claim that reads the same clock tick, which lasts milliseconds. Starting one
    // microsecond earlier keeps the job due to that claim.
    // language=SQL Server
    String sql =
        """
        UPDATE scheduler_job_queue
        SET status = ?, paused_from_status = ?, last_error = ?,
            scheduled_time = DATEADD(MILLISECOND, ?, DATEADD(MICROSECOND, -1, SYSUTCDATETIME())),
            picked_by = NULL, picked_at = NULL, updated_at = SYSUTCDATETIME()
        WHERE job_id = ? AND status = 'RUNNING' AND claim_seq = ?
        """;
    int updated =
        ctx.em()
            .createNativeQuery(sql)
            .setParameter(1, paused ? "PAUSED" : "PENDING")
            .setParameter(2, paused ? "PENDING" : null)
            .setParameter(3, "Waiting for resource: " + resource)
            .setParameter(4, retryDelayMs)
            .setParameter(5, UuidByteArrayConverter.toBytes(jobId))
            .setParameter(6, claimSeq)
            .executeUpdate();
    if (updated == 0) {
      return AcquireResult.SUPERSEDED;
    }
    // Never wait for the job row here: a save can hold it while it waits for this queue row.
    // Write the waiter row only while the job still names this resource.
    // language=SQL Server
    String currentResource =
        """
        SELECT resource_name FROM scheduler_job WITH (UPDLOCK, READPAST, ROWLOCK) WHERE job_id = ?
        """;
    List<?> resources =
        ctx.em()
            .createNativeQuery(currentResource)
            .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
            .getResultList();
    if (resources.isEmpty() || !resource.equals(resources.get(0))) {
      return AcquireResult.AT_CAPACITY;
    }

    // DATEADD takes an int, so add whole seconds and the millisecond remainder separately.
    long leaseMillis = ResourceWaiterLease.leaseMillis(retryDelayMs);
    int renewed = 0;
    if (claim[4] != null) {
      // language=SQL Server
      String renew =
          """
          UPDATE scheduler_resource_waiter
          SET lease_until = DATEADD(MILLISECOND, ?, DATEADD(SECOND, ?, SYSUTCDATETIME())),
              resource_name = ?, priority = ?, wait_since = ?
          WHERE job_id = ?
          """;
      renewed =
          ctx.em()
              .createNativeQuery(renew)
              .setParameter(1, (int) (leaseMillis % 1000))
              .setParameter(2, (int) (leaseMillis / 1000))
              .setParameter(3, resource)
              .setParameter(4, claim[4] != null && resource.equals(claim[6]) ? claim[4] : claim[2])
              .setParameter(5, claim[4] != null && resource.equals(claim[6]) ? claim[5] : claim[3])
              .setParameter(6, UuidByteArrayConverter.toBytes(jobId))
              .executeUpdate();
    }
    // The stale sweep can delete the row after the claim read it. Write it back with the keys
    // the waiters-ahead count used.
    if (renewed == 0) {
      // language=SQL Server
      String insert =
          """
          INSERT INTO scheduler_resource_waiter
            (job_id, resource_name, priority, wait_since, lease_until)
          VALUES (?, ?, ?, ?, DATEADD(MILLISECOND, ?, DATEADD(SECOND, ?, SYSUTCDATETIME())))
          """;
      ctx.em()
          .createNativeQuery(insert)
          .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
          .setParameter(2, resource)
          .setParameter(3, claim[4] != null && resource.equals(claim[6]) ? claim[4] : claim[2])
          .setParameter(4, claim[4] != null && resource.equals(claim[6]) ? claim[5] : claim[3])
          .setParameter(5, (int) (leaseMillis % 1000))
          .setParameter(6, (int) (leaseMillis / 1000))
          .executeUpdate();
    }
    return AcquireResult.AT_CAPACITY;
  }

  private int deleteWaiter(String resource, UUID jobId) {
    // language=SQL Server
    String sql =
        """
        DELETE FROM scheduler_resource_waiter WHERE job_id = ? AND resource_name = ?
        """;
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
        .setParameter(2, resource)
        .executeUpdate();
  }

  @Override
  public List<WokenWaiter> wakeWaiters(String resource) {
    try {
      // language=SQL Server
      String lock =
          """
          SELECT max_concurrent, retry_delay_ms FROM scheduler_resource_limit WITH (UPDLOCK, HOLDLOCK, ROWLOCK)
          WHERE resource_name = ?
          """;
      List<?> limits = ctx.em().createNativeQuery(lock).setParameter(1, resource).getResultList();
      if (limits.isEmpty()) {
        return List.of();
      }
      int max = ((Number) ((Object[]) limits.get(0))[0]).intValue();
      // language=SQL Server
      String heads =
          """
          SELECT w.job_id, q.status, w.priority, q.execution_target, w.wait_since, q.scheduled_time
          FROM scheduler_resource_waiter w JOIN scheduler_job_queue q ON q.job_id = w.job_id
          WHERE w.resource_name = ? AND w.lease_until > SYSUTCDATETIME()
            AND q.status IN ('PENDING', 'RUNNING')
          ORDER BY w.priority DESC, w.wait_since ASC, w.job_id ASC
          OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
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
      int free = max - countPermits(resource);
      if (free <= 0) {
        return List.of();
      }
      List<Object[]> pending = new ArrayList<>();
      Map<UUID, Integer> order = new HashMap<>();
      for (Object row : rows.subList(0, Math.min(free, rows.size()))) {
        Object[] head = (Object[]) row;
        order.put(SqlserverJobRowMapper.uuidOrNull(head[0]), order.size());
        if ("PENDING".equals(head[1].toString())) {
          pending.add(head);
        }
      }
      List<WokenWaiter> result = new ArrayList<>();
      for (Object[] row : pending) {
        // Skip a head whose due time changed after the line was read, as a user reschedule does.
        // language=SQL Server
        String wake =
            """
            UPDATE q SET scheduled_time = ?, updated_at = SYSUTCDATETIME()
            FROM scheduler_job_queue q WITH (UPDLOCK, READPAST, ROWLOCK)
            JOIN scheduler_job c WITH (UPDLOCK, READPAST, ROWLOCK) ON c.job_id = q.job_id
            WHERE q.job_id = ? AND q.status = 'PENDING' AND q.scheduled_time = ?
              AND c.resource_name = ?
            """;
        if (ctx.em()
                .createNativeQuery(wake)
                .setParameter(1, row[4])
                .setParameter(
                    2, UuidByteArrayConverter.toBytes(SqlserverJobRowMapper.uuidOrNull(row[0])))
                .setParameter(3, row[5])
                .setParameter(4, resource)
                .executeUpdate()
            == 1) {
          result.add(
              new WokenWaiter(
                  SqlserverJobRowMapper.uuidOrNull(row[0]),
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
    // language=SQL Server
    String sql =
        """
        SELECT DISTINCT resource_name FROM scheduler_resource_waiter
        ORDER BY resource_name OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """;
    if (after != null) {
      // language=SQL Server
      sql =
          """
          SELECT DISTINCT resource_name FROM scheduler_resource_waiter
          WHERE resource_name > ?
          ORDER BY resource_name OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
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
    // language=SQL Server
    String sql =
        """
        SELECT w.job_id, w.lease_until FROM scheduler_resource_waiter w
        LEFT JOIN scheduler_job_queue q ON q.job_id = w.job_id
        WHERE q.job_id IS NULL OR q.status NOT IN ('PENDING', 'RUNNING', 'PAUSED')
        ORDER BY w.job_id OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """;
    try {
      List<?> ids = ctx.em().createNativeQuery(sql).setParameter(1, limit).getResultList();
      int deleted = 0;
      // Fenced on the lease read above: a park that renewed the row after the read keeps it.
      for (Object value : ids) {
        Object[] row = (Object[]) value;
        deleted += deleteStaleWaiter(row[0], row[1]);
      }
      return deleted;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("delete stale resource waiters", e);
    }
  }

  private int deleteStaleWaiter(Object jobId, Object leaseUntil) {
    // language=SQL Server
    String sql =
        """
        DELETE FROM scheduler_resource_waiter WHERE job_id = ? AND lease_until = ?
        """;
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, jobId)
        .setParameter(2, leaseUntil)
        .executeUpdate();
  }

  @Override
  public void releasePermit(String resource, UUID jobId, long claimSeq) {
    try {
      // language=SQL Server
      String sql =
          """
          DELETE FROM scheduler_resource_permit WHERE id = ? AND resource_name = ? AND job_id = ?
          """;
      ctx.em()
          .createNativeQuery(sql)
          .setParameter(
              1,
              UuidByteArrayConverter.toBytes(ResourcePermitEntity.idFor(resource, jobId, claimSeq)))
          .setParameter(2, resource)
          .setParameter(3, UuidByteArrayConverter.toBytes(jobId))
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
  public Optional<ResourceLimit> findResourceLimit(String name) {
    try {
      // language=SQL Server
      String sql =
          """
          SELECT resource_name, max_concurrent, retry_delay_ms, description
          FROM scheduler_resource_limit WHERE resource_name = ?
          """;
      Object[] row =
          (Object[]) ctx.em().createNativeQuery(sql).setParameter(1, name).getSingleResult();
      return Optional.of(mapResourceLimit(row));
    } catch (NoResultException e) {
      return Optional.empty();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find resource limit", e);
    }
  }

  @Override
  public List<ResourceLimit> listResourceLimits() {
    try {
      // language=SQL Server
      String sql =
          """
          SELECT resource_name, max_concurrent, retry_delay_ms, description
          FROM scheduler_resource_limit
          """;
      List<?> rows = ctx.em().createNativeQuery(sql).getResultList();
      List<ResourceLimit> limits = new ArrayList<>(rows.size());
      for (Object row : rows) {
        limits.add(mapResourceLimit((Object[]) row));
      }
      return limits;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("list resource limits", e);
    }
  }

  private static ResourceLimit mapResourceLimit(Object[] row) {
    return new ResourceLimit(
        (String) row[0],
        ((Number) row[1]).intValue(),
        Duration.ofMillis(((Number) row[2]).intValue()),
        (String) row[3]);
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
               condition_priority, definition_order, created_at
        FROM scheduler_workflow_condition
        """;
    Query query = ctx.em().createNativeQuery(sqlPrefix + whereClause + " " + orderClause);
    for (int i = 0; i < params.size(); i++) {
      query.setParameter(i + 1, params.get(i));
    }
    List<WorkflowConditionEntity> conditions =
        ((List<Object[]>) query.getResultList())
            .stream().map(SqlserverAuxiliaryOperations::mapCondition).toList();
    return WorkflowConditionOrdering.sorted(conditions);
  }
}

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

import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ClaimState;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.BusinessKeyReservations;
import run.ratchet.store.util.SqlIdempotencyKeys;
import run.ratchet.store.util.StatusClassifier;

final class SqlserverJobReadOperations {

  private static final int FIND_BY_IDS_CHUNK_SIZE = 500;

  // language=SQL Server
  private static final String HYDRATION_FROM =
      """
      FROM scheduler_job c
      LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
      """;

  private final SqlserverStoreContext ctx;
  private final SqlserverTagOperations tags;

  SqlserverJobReadOperations(SqlserverStoreContext ctx, SqlserverTagOperations tags) {
    this.ctx = ctx;
    this.tags = tags;
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findById(UUID id) {
    try {
      // language=SQL Server
      String sql =
          "SELECT "
              + SqlserverJobRowMapper.hydrationSelect()
              + " "
              + HYDRATION_FROM
              + " WHERE c.job_id = ?";
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(id))
              .getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      JobEntity job = SqlserverJobRowMapper.hydrate(rows.get(0));
      tags.hydrateTagsSingle(job);
      return Optional.of(job);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find job by id", e);
    }
  }

  Optional<JobEntity> findByIdLatest(UUID id) {
    return findById(id);
  }

  @SuppressWarnings("unchecked")
  Optional<ClaimState> currentClaim(UUID jobId) {
    try {
      // language=SQL Server
      String sql =
          """
          SELECT status, claim_seq FROM scheduler_job_queue WHERE job_id = ?
          """;
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(jobId))
              .getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      Object[] row = rows.get(0);
      return Optional.of(
          new ClaimState(JobStatus.valueOf((String) row[0]), ((Number) row[1]).longValue()));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("read current claim", e);
    }
  }

  @SuppressWarnings("unchecked")
  JobStatus getJobStatus(UUID id) {
    try {
      // language=SQL Server
      String sql =
          """
          SELECT q.status, c.terminal_status
          FROM scheduler_job c
          LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
          WHERE c.job_id = ?
          """;
      List<Object[]> results =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(id))
              .getResultList();
      if (results.isEmpty()) {
        return null;
      }
      Object[] row = results.get(0);
      String live = (String) row[0];
      if (live != null) {
        return JobStatus.valueOf(live);
      }
      String terminal = (String) row[1];
      if (terminal != null) {
        return JobStatus.valueOf(terminal);
      }
      throw new IllegalStateException("Job " + id + " has no live or terminal status");
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("get job status", e);
    }
  }

  @SuppressWarnings("unchecked")
  Map<UUID, JobStatus> findStatusesByIds(Collection<UUID> ids) {
    List<UUID> uniqueIds = List.copyOf(new LinkedHashSet<>(ids));
    if (uniqueIds.size() > JobCrudStore.STATUS_READ_LIMIT) {
      throw new IllegalArgumentException("Too many job ids");
    }
    if (uniqueIds.isEmpty()) {
      return Map.of();
    }
    try {
      // language=SQL Server
      String sql =
          """
          SELECT c.job_id, q.status, c.terminal_status
          FROM scheduler_job c
          LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
          WHERE c.job_id IN (%s)
          """
              .formatted(String.join(",", Collections.nCopies(uniqueIds.size(), "?")));
      Query query = ctx.em().createNativeQuery(sql);
      int parameter = 1;
      for (UUID id : uniqueIds) {
        query.setParameter(parameter++, UuidByteArrayConverter.toBytes(id));
      }
      List<Object[]> rows = query.getResultList();
      Map<UUID, JobStatus> result = new HashMap<>();
      for (Object[] row : rows) {
        String status = (String) (row[1] != null ? row[1] : row[2]);
        if (status != null) {
          result.put(SqlserverJobRowMapper.uuidOrNull(row[0]), JobStatus.valueOf(status));
        }
      }
      return result;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find job statuses by ids", e);
    }
  }

  List<JobEntity> findByIds(List<UUID> ids) {
    try {
      if (ids.isEmpty()) {
        return List.of();
      }
      List<JobEntity> jobs = new ArrayList<>(ids.size());
      for (int start = 0; start < ids.size(); start += FIND_BY_IDS_CHUNK_SIZE) {
        jobs.addAll(
            findByIdsChunk(
                ids.subList(start, Math.min(start + FIND_BY_IDS_CHUNK_SIZE, ids.size()))));
      }
      tags.hydrateTagsBatch(jobs);
      return jobs;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find jobs by ids", e);
    }
  }

  @SuppressWarnings("unchecked")
  private List<JobEntity> findByIdsChunk(List<UUID> ids) {
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=SQL Server
    String sql =
        "SELECT "
            + SqlserverJobRowMapper.hydrationSelect()
            + " "
            + HYDRATION_FROM
            + " WHERE c.job_id IN ("
            + placeholders
            + ")";
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (UUID id : ids) {
      query.setParameter(parameter++, UuidByteArrayConverter.toBytes(id));
    }
    List<Object[]> rows = query.getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(SqlserverJobRowMapper.hydrate(row));
    }
    return jobs;
  }

  @SuppressWarnings("unchecked")
  Optional<BusinessKeyHolder> findBusinessKeyHolder(String businessKey) {
    try {
      // language=SQL Server
      String sql =
          """
          SELECT br.owner_job_id, br.owner_table, q.status, q.job_type, r.is_paused,
                 br.held_until, c.job_type
          FROM scheduler_business_key_reservation br
          LEFT JOIN scheduler_job_queue q
            ON br.owner_table = 'QUEUE' AND q.job_id = br.owner_job_id
          LEFT JOIN scheduler_recurring_job r
            ON br.owner_table = 'RECURRING' AND r.id = br.owner_job_id
          LEFT JOIN scheduler_job c
            ON br.held_until IS NOT NULL AND c.job_id = br.owner_job_id
          WHERE br.business_key = ?
          """;
      List<Object[]> rows =
          ctx.em().createNativeQuery(sql).setParameter(1, businessKey).getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      Object[] row = rows.get(0);
      UUID id = SqlserverJobRowMapper.uuidOrNull(row[0]);
      if (row[5] != null) {
        return row[6] == null
            ? Optional.empty()
            : Optional.of(
                new BusinessKeyHolder(
                    id,
                    JobExecutionType.valueOf((String) row[6]).toPublicType(),
                    JobStatus.CANCELED));
      }
      if (BusinessKeyReservations.OWNER_TABLE_QUEUE.equals(row[1]) && row[2] != null) {
        JobStatus status = JobStatus.valueOf((String) row[2]);
        if (!StatusClassifier.isLiveStatus(status)) {
          return Optional.empty();
        }
        return Optional.of(
            new BusinessKeyHolder(
                id, JobExecutionType.valueOf((String) row[3]).toPublicType(), status));
      }
      if (BusinessKeyReservations.OWNER_TABLE_RECURRING.equals(row[1]) && row[4] != null) {
        boolean paused =
            row[4] instanceof Boolean value ? value : ((Number) row[4]).intValue() != 0;
        return Optional.of(
            new BusinessKeyHolder(
                id, JobType.RECURRING, paused ? JobStatus.PAUSED : JobStatus.PENDING));
      }
      return Optional.empty();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find business key holder", e);
    }
  }

  @SuppressWarnings("unchecked")
  Optional<UUID> findOriginalJobIdByIdempotencyKey(String idempotencyKey) {
    return SqlIdempotencyKeys.find(ctx, idempotencyKey);
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    try {
      // language=SQL Server
      String sql =
          "SELECT TOP 1 "
              + SqlserverJobRowMapper.hydrationSelect()
              + " "
              + HYDRATION_FROM
              + " WHERE c.idempotency_key = ?";
      List<Object[]> rows =
          ctx.em().createNativeQuery(sql).setParameter(1, idempotencyKey).getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      JobEntity job = SqlserverJobRowMapper.hydrate(rows.get(0));
      tags.hydrateTagsSingle(job);
      return Optional.of(job);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find job by idempotency key", e);
    }
  }

  @SuppressWarnings("unchecked")
  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset) {
    try {
      // language=SQL Server
      String sql =
          "SELECT "
              + SqlserverJobRowMapper.hydrationSelect()
              + " "
              + HYDRATION_FROM
              + " WHERE c.depends_on = ? ORDER BY c.created_at ASC, c.job_id ASC";
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(parentJobId))
              .setFirstResult(offset)
              .setMaxResults(limit)
              .getResultList();
      List<JobEntity> jobs = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        jobs.add(SqlserverJobRowMapper.hydrate(row));
      }
      tags.hydrateTagsBatch(jobs);
      return jobs;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find dependant jobs", e);
    }
  }

  JobEntity hydrateForArchive(JobEntity job) {
    return findById(job.getId())
        .orElseThrow(() -> new IllegalStateException("Job not found for archival: " + job.getId()));
  }

  List<JobEntity> hydrateRowsWithTags(List<Object[]> rows) {
    List<JobEntity> jobs = SqlserverJobRowMapper.hydrateRows(rows);
    tags.hydrateTagsBatch(jobs);
    return jobs;
  }
}

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
package run.ratchet.store.mysql;

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
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.ClaimState;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.util.BusinessKeyReservations;
import run.ratchet.store.util.SqlIdempotencyKeys;
import run.ratchet.store.util.StatusClassifier;

final class MysqlJobReadOperations {

  private static final int FIND_BY_IDS_CHUNK_SIZE = 500;

  // language=MySQL
  private static final String HYDRATION_FROM =
      """
      FROM scheduler_job c
      LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
      """;

  private final MysqlStoreContext ctx;
  private final MysqlJobRowMapper mapper;
  private final MysqlTagOperations tags;

  MysqlJobReadOperations(MysqlStoreContext ctx, MysqlJobRowMapper mapper, MysqlTagOperations tags) {
    this.ctx = ctx;
    this.mapper = mapper;
    this.tags = tags;
  }

  Optional<JobEntity> findById(UUID id) {
    return findById(id, false);
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findById(UUID id, boolean sharedLock) {
    try {
      // language=MySQL
      String sql =
          "SELECT "
              + MysqlJobRowMapper.HYDRATION_SELECT
              + " "
              + HYDRATION_FROM
              + " WHERE c.job_id = ?"
              + (sharedLock ? " LOCK IN SHARE MODE" : "");
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(id))
              .getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      JobEntity job = mapper.hydrateJobEntity(rows.get(0));
      // Tags play no part in completion planning, so they are read without shared locks.
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
      // language=MySQL
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
      // language=MySQL
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
      // language=MySQL
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
          result.put(MysqlJobRowMapper.uuidOrNull(row[0]), JobStatus.valueOf(status));
        }
      }
      return result;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find job statuses by ids", e);
    }
  }

  List<JobEntity> findByIds(List<UUID> ids) {
    return findByIds(ids, false);
  }

  @SuppressWarnings("unchecked")
  List<JobEntity> findByIds(List<UUID> ids, boolean sharedLock) {
    try {
      if (ids.isEmpty()) {
        return List.of();
      }
      List<JobEntity> jobs = new ArrayList<>(ids.size());
      for (int start = 0; start < ids.size(); start += FIND_BY_IDS_CHUNK_SIZE) {
        jobs.addAll(
            findByIdsChunk(
                ids.subList(start, Math.min(start + FIND_BY_IDS_CHUNK_SIZE, ids.size())),
                sharedLock));
      }
      tags.hydrateTagsBatch(jobs);
      return jobs;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find jobs by ids", e);
    }
  }

  @SuppressWarnings("unchecked")
  private List<JobEntity> findByIdsChunk(List<UUID> ids, boolean sharedLock) {
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=MySQL
    String sql =
        "SELECT "
            + MysqlJobRowMapper.HYDRATION_SELECT
            + " "
            + HYDRATION_FROM
            + " WHERE c.job_id IN ("
            + placeholders
            + ")"
            + (sharedLock ? " LOCK IN SHARE MODE" : "");
    Query idsQuery = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (UUID id : ids) {
      idsQuery.setParameter(parameter++, UuidByteArrayConverter.toBytes(id));
    }
    List<Object[]> rows = idsQuery.getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(mapper.hydrateJobEntity(row));
    }
    return jobs;
  }

  @SuppressWarnings("unchecked")
  Optional<BusinessKeyHolder> findBusinessKeyHolder(String businessKey) {
    try {
      // language=MySQL
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
      UUID id = MysqlJobRowMapper.uuidOrNull(row[0]);
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
      // language=MySQL
      String sql =
          "SELECT "
              + MysqlJobRowMapper.HYDRATION_SELECT
              + " "
              + HYDRATION_FROM
              + " WHERE c.idempotency_key = ? LIMIT 1";
      List<Object[]> rows =
          ctx.em().createNativeQuery(sql).setParameter(1, idempotencyKey).getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      JobEntity job = mapper.hydrateJobEntity(rows.get(0));
      tags.hydrateTagsSingle(job);
      return Optional.of(job);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find by idempotency key", e);
    }
  }

  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset) {
    return findDependants(parentJobId, limit, offset, false);
  }

  @SuppressWarnings("unchecked")
  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset, boolean sharedLock) {
    try {
      // language=MySQL
      String sql =
          "SELECT "
              + MysqlJobRowMapper.HYDRATION_SELECT
              + " "
              + HYDRATION_FROM
              + " WHERE c.depends_on = ? ORDER BY c.created_at ASC, c.job_id ASC"
              + (sharedLock ? " LIMIT ? OFFSET ? LOCK IN SHARE MODE" : "");
      Query query =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(parentJobId));
      if (sharedLock) {
        // Keep LIMIT/OFFSET before the lock clause instead of relying on provider SQL rewriting.
        query.setParameter(2, limit).setParameter(3, offset);
      } else {
        query.setFirstResult(offset).setMaxResults(limit);
      }
      List<Object[]> rows = query.getResultList();
      List<JobEntity> jobs = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        jobs.add(mapper.hydrateJobEntity(row));
      }
      tags.hydrateTagsBatch(jobs);
      return jobs;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find dependants", e);
    }
  }
}

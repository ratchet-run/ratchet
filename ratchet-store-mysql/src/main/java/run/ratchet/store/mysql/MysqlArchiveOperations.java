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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.util.ArchiveHelper;
import run.ratchet.store.util.ArchiveQuerySupport;
import run.ratchet.store.util.ArchiveRowMapper;

final class MysqlArchiveOperations implements ArchiveStore {

  private static final String ARCHIVE_COLUMNS =
      """
      archive_id, original_job_id, final_status, job_type, priority, total_attempts,
      max_retries, backoff_policy, backoff_param_ms, timeout_sec, target_class,
      method_name, business_key, cron_expr, zone_id, original_scheduled_time,
      original_created_at, first_execution_time, completion_time,
      total_execution_time_ms, queue_wait_ms, archived_at, archived_by, archive_reason,
      job_result, result_type, final_error, payload_summary, depended_on, superseded_by,
      tags
      """;

  private static final String ARCHIVE_VALUE_PLACEHOLDERS =
      "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  // language=MySQL
  private static final String INSERT_ARCHIVE_SQL =
      """
      INSERT INTO scheduler_job_archive (%s)
      VALUES %s
      """
          .formatted(ARCHIVE_COLUMNS, ARCHIVE_VALUE_PLACEHOLDERS);

  private final MysqlStoreContext ctx;
  private final MysqlJobRowMapper mapper;
  private final MysqlTagOperations tags;
  private final MysqlJobCrudOperations jobs;

  MysqlArchiveOperations(
      MysqlStoreContext ctx,
      MysqlJobRowMapper mapper,
      MysqlTagOperations tags,
      MysqlJobCrudOperations jobs) {
    this.ctx = ctx;
    this.mapper = mapper;
    this.tags = tags;
    this.jobs = jobs;
  }

  private static void prepareArchive(ArchivedJobEntity archive) {
    if (archive.getId() == null) {
      archive.setId(UuidV7Factory.create());
    }
    if (archive.getArchivedAt() == null) {
      archive.setArchivedAt(Instant.now());
    }
  }

  private static int setArchiveParameters(Query query, ArchivedJobEntity archive, int parameter) {
    query.setParameter(parameter++, UuidByteArrayConverter.toBytes(archive.getId()));
    query.setParameter(parameter++, UuidByteArrayConverter.toBytes(archive.getOriginalJobId()));
    query.setParameter(parameter++, archive.getFinalStatus().name());
    query.setParameter(parameter++, archive.getJobType().name());
    query.setParameter(parameter++, archive.getPriority().ordinal());
    query.setParameter(parameter++, archive.getTotalAttempts());
    query.setParameter(parameter++, archive.getMaxRetries());
    query.setParameter(parameter++, archive.getBackoffPolicy().name());
    query.setParameter(parameter++, archive.getBackoffParamMs());
    query.setParameter(parameter++, archive.getTimeoutSec());
    query.setParameter(parameter++, archive.getTargetClass());
    query.setParameter(parameter++, archive.getMethodName());
    query.setParameter(parameter++, archive.getBusinessKey());
    query.setParameter(parameter++, archive.getCronExpr());
    query.setParameter(parameter++, archive.getZoneId());
    query.setParameter(parameter++, timestampOrNull(archive.getOriginalScheduledTime()));
    query.setParameter(parameter++, timestampOrNull(archive.getOriginalCreatedAt()));
    query.setParameter(parameter++, timestampOrNull(archive.getFirstExecutionTime()));
    query.setParameter(parameter++, timestampOrNull(archive.getCompletionTime()));
    query.setParameter(parameter++, archive.getTotalExecutionTimeMs());
    query.setParameter(parameter++, archive.getQueueWaitMs());
    query.setParameter(parameter++, timestampOrNull(archive.getArchivedAt()));
    query.setParameter(parameter++, archive.getArchivedBy());
    query.setParameter(parameter++, archive.getArchiveReason());
    query.setParameter(parameter++, archive.getJobResult());
    query.setParameter(parameter++, archive.getResultType());
    query.setParameter(parameter++, archive.getFinalError());
    query.setParameter(parameter++, archive.getPayloadSummary());
    query.setParameter(parameter++, UuidByteArrayConverter.toBytes(archive.getDependedOn()));
    query.setParameter(parameter++, UuidByteArrayConverter.toBytes(archive.getSupersededBy()));
    query.setParameter(parameter++, archive.getTags());
    return parameter;
  }

  private static Timestamp timestampOrNull(Instant instant) {
    return instant == null ? null : Timestamp.from(instant);
  }

  /**
   * Archives one terminal job in the caller's store transaction.
   *
   * @return the archived row that was inserted
   */
  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    try {
      JobEntity hydrated = hydrateForArchive(job);
      ArchivedJobEntity archive = ArchiveHelper.buildArchive(hydrated, reason, archivedBy);
      prepareArchive(archive);
      Query query = ctx.em().createNativeQuery(INSERT_ARCHIVE_SQL);
      setArchiveParameters(query, archive, 1);
      query.executeUpdate();
      return archive;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("archive job", e);
    }
  }

  /**
   * Archives all supplied terminal jobs in the caller's store transaction using a single multi-row
   * insert.
   *
   * @return number of archive rows inserted
   */
  @Override
  public int archiveJobsBatch(List<JobEntity> jobsToArchive, String reason, String archivedBy) {
    try {
      if (jobsToArchive.isEmpty()) {
        return 0;
      }

      List<UUID> ids = jobsToArchive.stream().map(JobEntity::getId).toList();
      Map<UUID, JobEntity> hydratedById =
          jobs.findByIds(ids).stream()
              .collect(Collectors.toMap(JobEntity::getId, Function.identity()));
      List<ArchivedJobEntity> archives = new ArrayList<>(jobsToArchive.size());
      for (UUID id : ids) {
        JobEntity hydrated = hydratedById.get(id);
        if (hydrated == null) {
          throw new IllegalStateException("Job not found for archival: " + id);
        }
        ArchivedJobEntity archive = ArchiveHelper.buildArchive(hydrated, reason, archivedBy);
        prepareArchive(archive);
        archives.add(archive);
      }

      String rows =
          String.join(",", Collections.nCopies(archives.size(), ARCHIVE_VALUE_PLACEHOLDERS));
      Query query =
          ctx.em()
              .createNativeQuery(
                  """
                  INSERT INTO scheduler_job_archive (%s)
                  VALUES %s
                  """
                      .formatted(ARCHIVE_COLUMNS, rows));
      int parameter = 1;
      for (ArchivedJobEntity archive : archives) {
        parameter = setArchiveParameters(query, archive, parameter);
      }
      query.executeUpdate();
      return archives.size();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("archive jobs batch", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    try {
      // language=MySQL
      String sql =
          """
          SELECT %s
          FROM scheduler_job c
          LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
          WHERE c.terminal_status IS NOT NULL AND c.terminated_at < ?
          ORDER BY c.terminated_at ASC
          LIMIT ?
          """
              .formatted(MysqlJobRowMapper.HYDRATION_SELECT);
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, Timestamp.from(olderThan))
              .setParameter(2, limit)
              .getResultList();
      List<JobEntity> jobs = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        jobs.add(mapper.hydrateJobEntity(row));
      }
      tags.hydrateTagsBatch(jobs);
      return jobs;
    } catch (RatchetTransientStoreException e) {
      throw e;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find jobs for archiving", e);
    }
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    try {
      // language=MySQL
      String sql =
          """
          SELECT COUNT(*) FROM scheduler_job
          WHERE terminal_status IS NOT NULL AND terminated_at < ?
          """;
      Object result =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, Timestamp.from(olderThan))
              .getSingleResult();
      return ((Number) result).longValue();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs for archiving", e);
    }
  }

  // SQL template is assembled from a compile-time-constant column list and AND-clauses defined in
  // this package; runtime values are bound as JDBC parameters via setParameter.
  @Override
  @SuppressWarnings("SqlSourceToSinkFlow")
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    try {
      var searchQuery =
          ArchiveQuerySupport.buildFindArchivedJobsQuery(
              ARCHIVE_COLUMNS, targetClass, businessKey, from, to, limit);
      Query query = ctx.em().createNativeQuery(searchQuery.sql());
      ArchiveQuerySupport.bindParameters(query, searchQuery);
      @SuppressWarnings("unchecked")
      List<Object[]> rows = query.getResultList();
      return rows.stream()
          .map(row -> ArchiveRowMapper.map(row, MysqlJobRowMapper::toInstant))
          .toList();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find archived jobs", e);
    }
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    try {
      // language=MySQL
      String sql = "DELETE FROM scheduler_job_archive WHERE archived_at < ?";
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, Timestamp.from(olderThan))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("purge archived jobs", e);
    }
  }

  private JobEntity hydrateForArchive(JobEntity job) {
    return jobs.findById(job.getId())
        .orElseThrow(() -> new IllegalStateException("Job not found for archival: " + job.getId()));
  }
}

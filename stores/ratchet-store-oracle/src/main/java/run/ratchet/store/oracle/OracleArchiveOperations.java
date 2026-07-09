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

import jakarta.persistence.Query;
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
import run.ratchet.store.oracle.converter.UuidRawConverter;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.util.ArchiveExtensionData;
import run.ratchet.store.util.ArchiveHelper;
import run.ratchet.store.util.ArchiveParameterBinder;
import run.ratchet.store.util.ArchiveQuerySupport;
import run.ratchet.store.util.ArchiveRowMapper;
import run.ratchet.store.util.ExtensionArchiveJson;
import run.ratchet.store.util.RowValues;

final class OracleArchiveOperations implements ArchiveStore {

  // language=Oracle
  private static final String INSERT_ARCHIVE_SQL =
      """
      INSERT INTO scheduler_job_archive (%s)
      VALUES %s
      """
          .formatted(
              ArchiveParameterBinder.ARCHIVE_COLUMNS,
              ArchiveParameterBinder.ARCHIVE_VALUE_PLACEHOLDERS);

  private final OracleStoreContext ctx;
  private final OracleJobRowMapper mapper;
  private final OracleTagOperations tags;
  private final OracleJobCrudOperations jobs;

  OracleArchiveOperations(
      OracleStoreContext ctx,
      OracleJobRowMapper mapper,
      OracleTagOperations tags,
      OracleJobCrudOperations jobs) {
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
      ArchiveExtensionData extensionData = fetchArchiveExtensionData(List.of(hydrated.getId()));
      populateExtensionData(archive, extensionData, hydrated.getId());
      Query query = ctx.em().createNativeQuery(INSERT_ARCHIVE_SQL);
      ArchiveParameterBinder.bind(query, archive, 1, UuidRawConverter::toBytes);
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
      List<UUID> archiveIds = new ArrayList<>(jobsToArchive.size());
      for (UUID id : ids) {
        JobEntity hydrated = hydratedById.get(id);
        if (hydrated == null) {
          throw new IllegalStateException("Job not found for archival: " + id);
        }
        ArchivedJobEntity archive = ArchiveHelper.buildArchive(hydrated, reason, archivedBy);
        prepareArchive(archive);
        archives.add(archive);
        archiveIds.add(hydrated.getId());
      }
      ArchiveExtensionData extensionData = fetchArchiveExtensionData(archiveIds);
      for (int i = 0; i < archives.size(); i++) {
        populateExtensionData(archives.get(i), extensionData, archiveIds.get(i));
      }

      String rows =
          String.join(
              ",",
              Collections.nCopies(
                  archives.size(), ArchiveParameterBinder.ARCHIVE_VALUE_PLACEHOLDERS));
      Query query =
          ctx.em()
              .createNativeQuery(
                  """
                  INSERT INTO scheduler_job_archive (%s)
                  VALUES %s
                  """
                      .formatted(ArchiveParameterBinder.ARCHIVE_COLUMNS, rows));
      int parameter = 1;
      for (ArchivedJobEntity archive : archives) {
        parameter =
            ArchiveParameterBinder.bind(query, archive, parameter, UuidRawConverter::toBytes);
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
      // language=Oracle
      String sql =
          """
          SELECT %s
          FROM scheduler_job c
          LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
          WHERE c.terminal_status IS NOT NULL AND c.terminated_at < ?
          ORDER BY c.terminated_at ASC
          FETCH FIRST ? ROWS ONLY
          """
              .formatted(OracleJobRowMapper.HYDRATION_SELECT);
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, OracleTimestamps.microTimestamp(olderThan))
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
      // language=Oracle
      String sql =
          """
          SELECT COUNT(*) FROM scheduler_job
          WHERE terminal_status IS NOT NULL AND terminated_at < ?
          """;
      Object result =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, OracleTimestamps.microTimestamp(olderThan))
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
      // archived_at >= ? is an inclusive-lower bound; floor the cutoff to the TIMESTAMP(6) column
      // precision so Oracle's nanosecond bind matches a stored (floored) value at the boundary.
      // The shared builder is store-agnostic, so floor here rather than in store-core. The `to`
      // bound uses <= and is correct unfloored.
      var searchQuery =
          ArchiveQuerySupport.buildFindArchivedJobsQuery(
              ArchiveParameterBinder.ARCHIVE_COLUMNS,
              targetClass,
              businessKey,
              OracleTimestamps.floorMicros(from),
              to,
              limit);
      // The shared builder emits MySQL/PostgreSQL "LIMIT ?"; Oracle uses the row-limiting clause.
      // The limit bind stays the trailing parameter, so bindParameters' ordering is unaffected.
      String sql = searchQuery.sql().replace("LIMIT ?", "FETCH FIRST ? ROWS ONLY");
      Query query = ctx.em().createNativeQuery(sql);
      ArchiveQuerySupport.bindParameters(query, searchQuery);
      @SuppressWarnings("unchecked")
      List<Object[]> rows = query.getResultList();
      return rows.stream().map(row -> ArchiveRowMapper.map(row, RowValues::instantOrNull)).toList();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find archived jobs", e);
    }
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    try {
      // language=Oracle
      String sql = "DELETE FROM scheduler_job_archive WHERE archived_at < ?";
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, OracleTimestamps.microTimestamp(olderThan))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("purge archived jobs", e);
    }
  }

  private JobEntity hydrateForArchive(JobEntity job) {
    return jobs.findById(job.getId())
        .orElseThrow(() -> new IllegalStateException("Job not found for archival: " + job.getId()));
  }

  private ArchiveExtensionData fetchArchiveExtensionData(List<UUID> jobIds) {
    return ArchiveExtensionData.fetch(
        ctx.em(), jobIds, UuidRawConverter::toBytes, RowValues::uuidOrNull);
  }

  private void populateExtensionData(
      ArchivedJobEntity archive, ArchiveExtensionData extensionData, UUID jobId) {
    archive.setProperties(ExtensionArchiveJson.propertiesJson(extensionData.properties(jobId)));
    archive.setExtensionState(ExtensionArchiveJson.extensionStateJson(extensionData.states(jobId)));
  }
}

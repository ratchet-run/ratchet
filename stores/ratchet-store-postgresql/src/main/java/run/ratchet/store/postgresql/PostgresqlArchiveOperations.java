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
package run.ratchet.store.postgresql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.util.ArchiveHelper;
import run.ratchet.store.util.ArchiveParameterBinder;
import run.ratchet.store.util.ArchiveQuerySupport;
import run.ratchet.store.util.ArchiveRowMapper;
import run.ratchet.store.util.ExtensionArchiveJson;
import run.ratchet.store.util.RowValues;

final class PostgresqlArchiveOperations implements ArchiveStore {

  private static final String MISSING_ARCHIVE_JOB_MESSAGE = "Job not found for archival: ";

  // language=PostgreSQL
  private static final String INSERT_ARCHIVE_SQL =
      """
      INSERT INTO scheduler_job_archive (%s)
      VALUES %s
      """
          .formatted(
              ArchiveParameterBinder.ARCHIVE_COLUMNS,
              ArchiveParameterBinder.ARCHIVE_VALUE_PLACEHOLDERS);

  private final PostgresqlStoreContext ctx;
  private final PostgresqlJobReadOperations reads;

  PostgresqlArchiveOperations(PostgresqlStoreContext ctx, PostgresqlJobReadOperations reads) {
    this.ctx = ctx;
    this.reads = reads;
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
   * @implNote TX: REQUIRED - joins the caller's active store transaction.
   * @return the archived row that was inserted
   */
  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    try {
      JobEntity hydrated = reads.hydrateForArchive(job);
      ArchivedJobEntity archive = ArchiveHelper.buildArchive(hydrated, reason, archivedBy);
      prepareArchive(archive);
      populateExtensionData(archive, hydrated.getId());
      Query query = ctx.em().createNativeQuery(INSERT_ARCHIVE_SQL);
      ArchiveParameterBinder.bind(query, archive, 1, id -> id);
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
   * @implNote TX: REQUIRED - joins the caller's active store transaction.
   * @return number of archive rows inserted
   */
  @Override
  public int archiveJobsBatch(List<JobEntity> jobsToArchive, String reason, String archivedBy) {
    if (jobsToArchive.isEmpty()) {
      return 0;
    }

    try {
      List<UUID> ids = jobsToArchive.stream().map(JobEntity::getId).toList();
      Map<UUID, JobEntity> hydratedById =
          reads.findByIds(ids).stream()
              .collect(Collectors.toMap(JobEntity::getId, Function.identity()));
      List<ArchivedJobEntity> archives = new ArrayList<>(jobsToArchive.size());
      for (UUID id : ids) {
        JobEntity hydrated = hydratedById.get(id);
        if (hydrated == null) {
          throw new IllegalStateException(MISSING_ARCHIVE_JOB_MESSAGE.concat(String.valueOf(id)));
        }
        ArchivedJobEntity archive = ArchiveHelper.buildArchive(hydrated, reason, archivedBy);
        prepareArchive(archive);
        populateExtensionData(archive, hydrated.getId());
        archives.add(archive);
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
        parameter = ArchiveParameterBinder.bind(query, archive, parameter, id -> id);
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
      // language=PostgreSQL
      String sql =
          """
          SELECT %s
          FROM scheduler_job c
          LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
          WHERE c.terminal_status IS NOT NULL
            AND c.terminated_at < ?
          ORDER BY c.terminated_at ASC
          LIMIT ?
          """
              .formatted(PostgresqlJobRowMapper.hydrationSelect());
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, Timestamp.from(olderThan))
              .setParameter(2, limit)
              .getResultList();
      return reads.hydrateRowsWithTags(rows);
    } catch (RatchetTransientStoreException e) {
      throw e;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find jobs for archiving", e);
    }
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    // language=PostgreSQL
    String sql =
        """
        SELECT COUNT(*) FROM scheduler_job
        WHERE terminal_status IS NOT NULL AND terminated_at < ?
        """;
    return ctx.countByNative(sql, Timestamp.from(olderThan));
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
              ArchiveParameterBinder.ARCHIVE_COLUMNS, targetClass, businessKey, from, to, limit);
      Query query = ctx.em().createNativeQuery(searchQuery.sql());
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
      // language=PostgreSQL
      String sql = "DELETE FROM scheduler_job_archive WHERE archived_at < ?";
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, Timestamp.from(olderThan))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("purge archived jobs", e);
    }
  }

  private void populateExtensionData(ArchivedJobEntity archive, UUID jobId) {
    // language=PostgreSQL
    String propertySql =
        """
        SELECT property_key, value
        FROM scheduler_job_properties
        WHERE job_id = ?
        """;
    @SuppressWarnings("unchecked")
    List<Object[]> propertyRows =
        ctx.em().createNativeQuery(propertySql).setParameter(1, jobId).getResultList();
    Map<String, String> properties = new LinkedHashMap<>();
    for (Object[] row : propertyRows) {
      String value = RowValues.stringOrNull(row[1]);
      if (value != null) {
        properties.put((String) row[0], value);
      }
    }
    archive.setProperties(ExtensionArchiveJson.propertiesJson(properties));

    // language=PostgreSQL
    String stateSql =
        """
        SELECT namespace, state, encrypted_state, encryption_key_id, version, updated_at
        FROM scheduler_job_extension_state
        WHERE job_id = ?
        """;
    @SuppressWarnings("unchecked")
    List<Object[]> stateRows =
        ctx.em().createNativeQuery(stateSql).setParameter(1, jobId).getResultList();
    List<ExtensionArchiveJson.StateRow> states = new ArrayList<>(stateRows.size());
    for (Object[] row : stateRows) {
      states.add(
          new ExtensionArchiveJson.StateRow(
              (String) row[0],
              RowValues.stringOrNull(row[1]),
              RowValues.booleanOrFalse(row[2]),
              RowValues.stringOrNull(row[3]),
              ((Number) row[4]).intValue(),
              RowValues.instantOrNull(row[5])));
    }
    archive.setExtensionState(ExtensionArchiveJson.extensionStateJson(states));
  }
}

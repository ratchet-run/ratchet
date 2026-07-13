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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import run.ratchet.api.exception.RatchetTransientStoreException;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.sqlserver.converter.UuidByteArrayConverter;
import run.ratchet.store.util.ArchiveExtensionData;
import run.ratchet.store.util.ArchiveHelper;
import run.ratchet.store.util.ArchiveParameterBinder;
import run.ratchet.store.util.ArchiveQuerySupport;
import run.ratchet.store.util.ArchiveRowMapper;
import run.ratchet.store.util.ExtensionArchiveJson;
import run.ratchet.store.util.RowValues;

final class SqlserverArchiveOperations implements ArchiveStore {

  // SQL Server variant of ArchiveParameterBinder.ARCHIVE_VALUE_PLACEHOLDERS: depended_on (29) and
  // superseded_by (30) are nullable BINARY(16) columns, and mssql-jdbc binds an untyped Java null
  // as
  // nvarchar, which SQL Server refuses to implicitly convert to binary. CAST(? AS BINARY(16)) makes
  // the conversion explicit (a non-null byte[] casts harmlessly). The other 31 placeholders match
  // the shared binder's positional order exactly (tags 31, properties 32, extension_state 33).
  private static final String SQLSERVER_ARCHIVE_VALUE_PLACEHOLDERS =
      "(" + "?, ".repeat(28) + "CAST(? AS BINARY(16)), CAST(? AS BINARY(16)), ?, ?, ?)";

  // language=SQL Server
  private static final String INSERT_ARCHIVE_SQL =
      """
      INSERT INTO scheduler_job_archive (%s)
      VALUES %s
      """
          .formatted(ArchiveParameterBinder.ARCHIVE_COLUMNS, SQLSERVER_ARCHIVE_VALUE_PLACEHOLDERS);

  private final SqlserverStoreContext ctx;
  private final SqlserverJobReadOperations reads;
  private final SqlserverJobDeleteOperations deletes;

  SqlserverArchiveOperations(
      SqlserverStoreContext ctx,
      SqlserverJobReadOperations reads,
      SqlserverJobDeleteOperations deletes) {
    this.ctx = ctx;
    this.reads = reads;
    this.deletes = deletes;
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
      ArchiveExtensionData extensionData = fetchArchiveExtensionData(List.of(hydrated.getId()));
      populateExtensionData(archive, extensionData, hydrated.getId());
      Query query = ctx.em().createNativeQuery(INSERT_ARCHIVE_SQL);
      ArchiveParameterBinder.bind(query, archive, 1, UuidByteArrayConverter::toBytes);
      query.executeUpdate();
      return archive;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("archive job", e);
    }
  }

  /** Atomically archives and terminal-guard-deletes all supplied jobs. */
  @Override
  public int archiveAndDeleteJobsBatch(
      List<JobEntity> jobsToArchive, String reason, String archivedBy) {
    if (jobsToArchive.isEmpty()) {
      return 0;
    }

    try {
      List<UUID> ids = jobsToArchive.stream().map(JobEntity::getId).toList();
      lockJobRowsForArchive(ids);
      List<JobEntity> currentJobs =
          ArchiveHelper.requireCurrentTerminalJobs(ids, reads.findByIds(ids));
      List<ArchivedJobEntity> archives = new ArrayList<>(jobsToArchive.size());
      List<UUID> archiveIds = new ArrayList<>(jobsToArchive.size());
      for (JobEntity hydrated : currentJobs) {
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
              ",", Collections.nCopies(archives.size(), SQLSERVER_ARCHIVE_VALUE_PLACEHOLDERS));
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
            ArchiveParameterBinder.bind(query, archive, parameter, UuidByteArrayConverter::toBytes);
      }
      query.executeUpdate();
      return ArchiveHelper.requireAllDeleted(ids, deletes.deleteTerminalJobsByIds(ids));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("archive and delete jobs batch", e);
    }
  }

  /**
   * Locks the hot rows before the snapshot read so a concurrent resurrect cannot commit between the
   * read and the terminal-guarded delete, which would archive stale content while deleting the
   * newer terminal row.
   */
  private void lockJobRowsForArchive(List<UUID> ids) {
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=SQL Server
    String sql =
        "SELECT job_id FROM scheduler_job WITH (UPDLOCK, ROWLOCK) WHERE job_id IN ("
            + placeholders
            + ")";
    Query lock = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (UUID id : ids) {
      lock.setParameter(parameter++, UuidByteArrayConverter.toBytes(id));
    }
    lock.getResultList();
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
    try {
      // language=SQL Server
      String sql =
          """
          SELECT %s
          FROM scheduler_job c
          LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
          WHERE c.terminal_status IS NOT NULL
            AND c.terminated_at < ?
          ORDER BY c.terminated_at ASC
          OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
          """
              .formatted(SqlserverJobRowMapper.hydrationSelect());
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
    // language=SQL Server
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
      // ArchiveQuerySupport emits the portable-majority "... ORDER BY archived_at DESC LIMIT ?"
      // tail; SQL Server expresses the row cap as OFFSET/FETCH. The bound limit stays last.
      String sql =
          searchQuery
              .sql()
              .replace(
                  "ORDER BY archived_at DESC LIMIT ?",
                  "ORDER BY archived_at DESC OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
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
      // language=SQL Server
      String sql = "DELETE FROM scheduler_job_archive WHERE archived_at < ?";
      return ctx.em()
          .createNativeQuery(sql)
          .setParameter(1, Timestamp.from(olderThan))
          .executeUpdate();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("purge archived jobs", e);
    }
  }

  private ArchiveExtensionData fetchArchiveExtensionData(List<UUID> jobIds) {
    return ArchiveExtensionData.fetch(
        ctx.em(), jobIds, UuidByteArrayConverter::toBytes, RowValues::uuidOrNull);
  }

  private void populateExtensionData(
      ArchivedJobEntity archive, ArchiveExtensionData extensionData, UUID jobId) {
    archive.setProperties(ExtensionArchiveJson.propertiesJson(extensionData.properties(jobId)));
    archive.setExtensionState(ExtensionArchiveJson.extensionStateJson(extensionData.states(jobId)));
  }
}

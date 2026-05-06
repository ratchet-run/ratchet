package run.ratchet.store.postgresql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.util.ArchiveHelper;
import run.ratchet.store.util.ArchiveRowMapper;

final class PostgresqlArchiveOperations implements ArchiveStore {

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

  // language=PostgreSQL
  private static final String INSERT_ARCHIVE_SQL =
      """
      INSERT INTO scheduler_job_archive (%s)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """
          .formatted(ARCHIVE_COLUMNS);

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

  private static void setArchiveParameters(Query query, ArchivedJobEntity archive) {
    int parameter = 1;
    query.setParameter(parameter++, archive.getId());
    query.setParameter(parameter++, archive.getOriginalJobId());
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
    query.setParameter(parameter++, archive.getDependedOn());
    query.setParameter(parameter++, archive.getSupersededBy());
    query.setParameter(parameter, archive.getTags());
  }

  private static Timestamp timestampOrNull(Instant instant) {
    return instant == null ? null : Timestamp.from(instant);
  }

  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    JobEntity hydrated = reads.hydrateForArchive(job);
    ArchivedJobEntity archive = ArchiveHelper.buildArchive(hydrated, reason, archivedBy);
    prepareArchive(archive);
    Query query = ctx.em().createNativeQuery(INSERT_ARCHIVE_SQL);
    setArchiveParameters(query, archive);
    query.executeUpdate();
    return archive;
  }

  @Override
  public int archiveJobsBatch(List<JobEntity> jobsToArchive, String reason, String archivedBy) {
    int count = 0;
    for (JobEntity job : jobsToArchive) {
      archiveJob(job, reason, archivedBy);
      count++;
    }
    return count;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> findJobsForArchiving(Instant olderThan, int limit) {
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

  @Override
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    StringBuilder sql =
        new StringBuilder("SELECT " + ARCHIVE_COLUMNS + " FROM scheduler_job_archive WHERE 1=1");
    List<Object> params = new ArrayList<>();
    if (targetClass != null) {
      sql.append(" AND target_class = ?");
      params.add(targetClass);
    }
    if (businessKey != null) {
      sql.append(" AND business_key = ?");
      params.add(businessKey);
    }
    if (from != null) {
      sql.append(" AND archived_at >= ?");
      params.add(Timestamp.from(from));
    }
    if (to != null) {
      sql.append(" AND archived_at <= ?");
      params.add(Timestamp.from(to));
    }
    sql.append(" ORDER BY archived_at DESC LIMIT ?");
    params.add(limit);

    Query query = ctx.em().createNativeQuery(sql.toString());
    for (int i = 0; i < params.size(); i++) {
      query.setParameter(i + 1, params.get(i));
    }
    @SuppressWarnings("unchecked")
    List<Object[]> rows = query.getResultList();
    return rows.stream()
        .map(row -> ArchiveRowMapper.map(row, PostgresqlJobRowMapper::toInstant))
        .toList();
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    // language=PostgreSQL
    String sql = "DELETE FROM scheduler_job_archive WHERE archived_at < ?";
    return ctx.em()
        .createNativeQuery(sql)
        .setParameter(1, Timestamp.from(olderThan))
        .executeUpdate();
  }
}

package run.ratchet.store.postgresql;

import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.util.ArchiveHelper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

final class PostgresqlArchiveOperations implements ArchiveStore {

  private final PostgresqlStoreContext ctx;
  private final PostgresqlJobCrudOperations jobs;

  PostgresqlArchiveOperations(PostgresqlStoreContext ctx, PostgresqlJobCrudOperations jobs) {
    this.ctx = ctx;
    this.jobs = jobs;
  }

  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    JobEntity hydrated = jobs.hydrateForArchive(job);
    ArchivedJobEntity archive = ArchiveHelper.buildArchive(hydrated, reason, archivedBy);
    ctx.em().persist(archive);
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
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + PostgresqlJobRowMapper.hydrationSelect("j")
                    + " FROM scheduler_job j "
                    + "WHERE j.status IN ('SUCCEEDED','FAILED','CANCELED') "
                    + "AND j.updated_at < ? "
                    + "ORDER BY j.updated_at ASC "
                    + "LIMIT ?")
            .setParameter(1, Timestamp.from(olderThan))
            .setParameter(2, limit)
            .getResultList();
    return jobs.hydrateRowsWithTags(rows);
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    return ctx.countByNative(
        "SELECT COUNT(*) FROM scheduler_job "
            + "WHERE status IN ('SUCCEEDED','FAILED','CANCELED') AND updated_at < ?",
        Timestamp.from(olderThan));
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    StringBuilder sql = new StringBuilder("SELECT * FROM scheduler_job_archive WHERE 1=1");
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

    var query = ctx.em().createNativeQuery(sql.toString(), ArchivedJobEntity.class);
    for (int i = 0; i < params.size(); i++) {
      query.setParameter(i + 1, params.get(i));
    }
    return query.getResultList();
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    return ctx.em()
        .createNativeQuery("DELETE FROM scheduler_job_archive WHERE archived_at < ?")
        .setParameter(1, Timestamp.from(olderThan))
        .executeUpdate();
  }
}

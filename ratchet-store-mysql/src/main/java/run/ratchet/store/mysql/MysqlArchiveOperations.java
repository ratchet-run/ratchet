package run.ratchet.store.mysql;

import run.ratchet.store.entity.ArchivedJobEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.util.ArchiveHelper;
import jakarta.persistence.TypedQuery;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

final class MysqlArchiveOperations implements ArchiveStore {

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

  @Override
  public ArchivedJobEntity archiveJob(JobEntity job, String reason, String archivedBy) {
    JobEntity hydrated = hydrateForArchive(job);
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
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.terminal_status IS NOT NULL AND c.terminated_at < ? "
                    + "ORDER BY c.terminated_at ASC "
                    + "LIMIT ?")
            .setParameter(1, Timestamp.from(olderThan))
            .setParameter(2, limit)
            .getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(mapper.hydrateJobEntity(row));
    }
    tags.hydrateTagsBatch(jobs);
    return jobs;
  }

  @Override
  public long countJobsForArchiving(Instant olderThan) {
    Object result =
        ctx.em()
            .createNativeQuery(
                "SELECT COUNT(*) FROM scheduler_job "
                    + "WHERE terminal_status IS NOT NULL AND terminated_at < ?")
            .setParameter(1, Timestamp.from(olderThan))
            .getSingleResult();
    return ((Number) result).longValue();
  }

  @Override
  public List<ArchivedJobEntity> findArchivedJobs(
      String targetClass, String businessKey, Instant from, Instant to, int limit) {
    StringBuilder jpql = new StringBuilder("SELECT a FROM ArchivedJobEntity a WHERE 1=1");
    if (targetClass != null) {
      jpql.append(" AND a.targetClass = :tc");
    }
    if (businessKey != null) {
      jpql.append(" AND a.businessKey = :bk");
    }
    if (from != null) {
      jpql.append(" AND a.archivedAt >= :from");
    }
    if (to != null) {
      jpql.append(" AND a.archivedAt <= :to");
    }
    jpql.append(" ORDER BY a.archivedAt DESC");

    TypedQuery<ArchivedJobEntity> query =
        ctx.em().createQuery(jpql.toString(), ArchivedJobEntity.class);
    if (targetClass != null) {
      query.setParameter("tc", targetClass);
    }
    if (businessKey != null) {
      query.setParameter("bk", businessKey);
    }
    if (from != null) {
      query.setParameter("from", from);
    }
    if (to != null) {
      query.setParameter("to", to);
    }
    return query.setMaxResults(limit).getResultList();
  }

  @Override
  public int purgeArchivedJobs(Instant olderThan) {
    return ctx.em()
        .createQuery("DELETE FROM ArchivedJobEntity a WHERE a.archivedAt < :cutoff")
        .setParameter("cutoff", olderThan)
        .executeUpdate();
  }

  private JobEntity hydrateForArchive(JobEntity job) {
    return jobs.findById(job.getId())
        .orElseThrow(() -> new IllegalStateException("Job not found for archival: " + job.getId()));
  }
}

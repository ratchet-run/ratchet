package run.ratchet.store.postgresql;

import jakarta.persistence.Query;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;

final class PostgresqlJobReadOperations {

  private static final Logger log = Logger.getLogger(PostgresqlJobReadOperations.class);

  // language=PostgreSQL
  private static final String HYDRATION_FROM =
      """
      FROM scheduler_job c
      LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
      """;

  private final PostgresqlStoreContext ctx;
  private final PostgresqlTagOperations tags;

  PostgresqlJobReadOperations(PostgresqlStoreContext ctx, PostgresqlTagOperations tags) {
    this.ctx = ctx;
    this.tags = tags;
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findById(UUID id) {
    try {
      // language=PostgreSQL
      String sql =
          "SELECT "
              + PostgresqlJobRowMapper.hydrationSelect()
              + " "
              + HYDRATION_FROM
              + " WHERE c.job_id = ?";
      List<Object[]> rows = ctx.em().createNativeQuery(sql).setParameter(1, id).getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      JobEntity job = PostgresqlJobRowMapper.hydrate(rows.get(0));
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
  JobStatus getJobStatus(UUID id) {
    try {
      // language=PostgreSQL
      String sql =
          """
          SELECT q.status, c.rec_status, c.terminal_status
          FROM scheduler_job c
          LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
          WHERE c.job_id = ?
          """;
      List<Object[]> results = ctx.em().createNativeQuery(sql).setParameter(1, id).getResultList();
      if (results.isEmpty()) {
        return null;
      }
      Object[] row = results.get(0);
      String live = (String) row[0];
      if (live != null) {
        return JobStatus.valueOf(live);
      }
      JobStatus rec =
          PostgresqlJobRowMapper.recStatusDecode(PostgresqlJobRowMapper.stringOrNull(row[1]));
      if (rec != null) {
        return rec;
      }
      String terminal = (String) row[2];
      if (terminal != null) {
        return JobStatus.valueOf(terminal);
      }
      throw new IllegalStateException("Job " + id + " has no live, recurring, or terminal status");
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("get job status", e);
    }
  }

  @SuppressWarnings("unchecked")
  List<JobEntity> findByIds(List<UUID> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    try {
      String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
      // language=PostgreSQL
      String sql =
          "SELECT "
              + PostgresqlJobRowMapper.hydrationSelect()
              + " "
              + HYDRATION_FROM
              + " WHERE c.job_id IN ("
              + placeholders
              + ")";
      Query query = ctx.em().createNativeQuery(sql);
      int parameter = 1;
      for (UUID id : ids) {
        query.setParameter(parameter++, id);
      }
      List<Object[]> rows = query.getResultList();
      List<JobEntity> jobs = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        jobs.add(PostgresqlJobRowMapper.hydrate(row));
      }
      tags.hydrateTagsBatch(jobs);
      return jobs;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find jobs by ids", e);
    }
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    try {
      // language=PostgreSQL
      String sql =
          "SELECT br.owner_table, "
              + PostgresqlJobRowMapper.hydrationSelect()
              + " FROM scheduler_business_key_reservation br "
              + "JOIN scheduler_job c ON c.job_id = br.owner_job_id "
              + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
              + "WHERE br.business_key = ? LIMIT 1";
      List<Object[]> rows =
          ctx.em().createNativeQuery(sql).setParameter(1, businessKey).getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      Object[] full = rows.get(0);
      String ownerTable = (String) full[0];
      Object[] hydrationRow = new Object[PostgresqlJobRowMapper.HYDRATION_COL_COUNT];
      System.arraycopy(full, 1, hydrationRow, 0, PostgresqlJobRowMapper.HYDRATION_COL_COUNT);
      JobEntity job = PostgresqlJobRowMapper.hydrate(hydrationRow);
      if (PostgresqlBusinessKeyReservations.OWNER_TABLE_QUEUE.equals(ownerTable)
          && hydrationRow[PostgresqlJobRowMapper.IDX_Q_STATUS] == null) {
        log.errorf(
            "bkres invariant violation: business_key=%s claims QUEUE owner job=%s but no hot row",
            businessKey, job.getId());
        throw new IllegalStateException(
            "Business key reservation "
                + businessKey
                + " claims QUEUE owner job "
                + job.getId()
                + " but no hot row exists");
      }
      tags.hydrateTagsSingle(job);
      return Optional.of(job);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find active job by business key", e);
    }
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    try {
      // language=PostgreSQL
      String sql =
          "SELECT "
              + PostgresqlJobRowMapper.hydrationSelect()
              + " "
              + HYDRATION_FROM
              + " WHERE c.idempotency_key = ? LIMIT 1";
      List<Object[]> rows =
          ctx.em().createNativeQuery(sql).setParameter(1, idempotencyKey).getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      JobEntity job = PostgresqlJobRowMapper.hydrate(rows.get(0));
      tags.hydrateTagsSingle(job);
      return Optional.of(job);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find job by idempotency key", e);
    }
  }

  @SuppressWarnings("unchecked")
  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset) {
    try {
      // language=PostgreSQL
      String sql =
          "SELECT "
              + PostgresqlJobRowMapper.hydrationSelect()
              + " "
              + HYDRATION_FROM
              + " WHERE c.depends_on = ? ORDER BY c.created_at ASC, c.job_id ASC";
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, parentJobId)
              .setFirstResult(offset)
              .setMaxResults(limit)
              .getResultList();
      List<JobEntity> jobs = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        jobs.add(PostgresqlJobRowMapper.hydrate(row));
      }
      tags.hydrateTagsBatch(jobs);
      return jobs;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find dependant jobs", e);
    }
  }

  @SuppressWarnings("unchecked")
  Optional<Instant> findEarliestRecurringNextFire() {
    try {
      // language=PostgreSQL
      String sql =
          """
          SELECT MIN(next_fire) FROM scheduler_job
          WHERE job_type = 'RECURRING' AND rec_status = 'P'
            AND next_fire IS NOT NULL
          """;
      List<Object> results = ctx.em().createNativeQuery(sql).getResultList();
      if (results.isEmpty() || results.get(0) == null) {
        return Optional.empty();
      }
      return Optional.ofNullable(PostgresqlJobRowMapper.toInstant(results.get(0)));
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find earliest recurring next fire", e);
    }
  }

  JobEntity hydrateForArchive(JobEntity job) {
    return findById(job.getId())
        .orElseThrow(() -> new IllegalStateException("Job not found for archival: " + job.getId()));
  }

  List<JobEntity> hydrateRowsWithTags(List<Object[]> rows) {
    List<JobEntity> jobs = PostgresqlJobRowMapper.hydrateRows(rows);
    tags.hydrateTagsBatch(jobs);
    return jobs;
  }
}

package run.ratchet.store.mysql;

import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobStatus;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

final class MysqlJobReadOperations {

  private static final Logger log = Logger.getLogger(MysqlJobReadOperations.class);

  private final MysqlStoreContext ctx;
  private final MysqlJobRowMapper mapper;
  private final MysqlTagOperations tags;

  MysqlJobReadOperations(MysqlStoreContext ctx, MysqlJobRowMapper mapper, MysqlTagOperations tags) {
    this.ctx = ctx;
    this.mapper = mapper;
    this.tags = tags;
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findById(long id) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    JobEntity job = mapper.hydrateJobEntity(rows.get(0));
    tags.hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findByIdLatest(long id) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    JobEntity job = mapper.hydrateJobEntity(rows.get(0));
    tags.hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @SuppressWarnings("unchecked")
  JobStatus getJobStatus(long id) {
    List<Object[]> results =
        ctx.em()
            .createNativeQuery(
                "SELECT q.status, c.rec_status, c.terminal_status "
                    + "FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id = ?")
            .setParameter(1, id)
            .getResultList();
    if (results.isEmpty()) {
      return null;
    }
    Object[] row = results.get(0);
    String live = (String) row[0];
    if (live != null) {
      return JobStatus.valueOf(live);
    }
    JobStatus rec = MysqlJobRowMapper.recStatusDecode(MysqlJobRowMapper.stringOrNull(row[1]));
    if (rec != null) {
      return rec;
    }
    String terminal = (String) row[2];
    if (terminal != null) {
      return JobStatus.valueOf(terminal);
    }
    log.errorf("Job %d has no live, recurring, or terminal status — invariant violation", id);
    return null;
  }

  @SuppressWarnings("unchecked")
  List<JobEntity> findByIds(List<Long> ids) {
    if (ids.isEmpty()) {
      return List.of();
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    Query idsQuery =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.job_id IN ("
                    + placeholders
                    + ")");
    int parameter = 1;
    for (Long id : ids) {
      idsQuery.setParameter(parameter++, id);
    }
    List<Object[]> rows = idsQuery.getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(mapper.hydrateJobEntity(row));
    }
    tags.hydrateTagsBatch(jobs);
    return jobs;
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT br.owner_table, "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_business_key_reservation br "
                    + "JOIN scheduler_job c ON c.job_id = br.owner_job_id "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE br.business_key = ? LIMIT 1")
            .setParameter(1, businessKey)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    Object[] full = rows.get(0);
    String ownerTable = (String) full[0];
    Object[] hydrationRow = new Object[MysqlJobRowMapper.HYDRATION_COL_COUNT];
    System.arraycopy(full, 1, hydrationRow, 0, MysqlJobRowMapper.HYDRATION_COL_COUNT);
    JobEntity job = mapper.hydrateJobEntity(hydrationRow);
    if (MysqlBusinessKeyReservations.OWNER_TABLE_QUEUE.equals(ownerTable)
        && hydrationRow[MysqlJobRowMapper.IDX_Q_STATUS] == null) {
      log.errorf(
          "bkres invariant violation: business_key=%s claims QUEUE owner job=%d but no hot row",
          businessKey, job.getId());
      return Optional.empty();
    }
    tags.hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.idempotency_key = ? LIMIT 1")
            .setParameter(1, idempotencyKey)
            .getResultList();
    if (rows.isEmpty()) {
      return Optional.empty();
    }
    JobEntity job = mapper.hydrateJobEntity(rows.get(0));
    tags.hydrateTagsSingle(job);
    return Optional.of(job);
  }

  @SuppressWarnings("unchecked")
  List<JobEntity> findDependants(long parentJobId) {
    List<Object[]> rows =
        ctx.em()
            .createNativeQuery(
                "SELECT "
                    + MysqlJobRowMapper.HYDRATION_SELECT
                    + " FROM scheduler_job c "
                    + "LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id "
                    + "WHERE c.depends_on = ?")
            .setParameter(1, parentJobId)
            .getResultList();
    List<JobEntity> jobs = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      jobs.add(mapper.hydrateJobEntity(row));
    }
    tags.hydrateTagsBatch(jobs);
    return jobs;
  }

  Optional<Instant> findEarliestRecurringNextFire() {
    List<?> results =
        ctx.em()
            .createNativeQuery(
                "SELECT MIN(next_fire) FROM scheduler_job "
                    + "WHERE job_type = 'RECURRING' AND rec_status = 'P' "
                    + "AND next_fire IS NOT NULL")
            .getResultList();
    if (results.isEmpty() || results.get(0) == null) {
      return Optional.empty();
    }
    Object val = results.get(0);
    if (val instanceof Timestamp ts) {
      return Optional.of(ts.toInstant());
    }
    return Optional.empty();
  }
}

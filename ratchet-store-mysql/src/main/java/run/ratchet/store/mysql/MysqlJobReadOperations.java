package run.ratchet.store.mysql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;

final class MysqlJobReadOperations {

  private static final Logger log = Logger.getLogger(MysqlJobReadOperations.class);
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

  @SuppressWarnings("unchecked")
  Optional<JobEntity> findById(UUID id) {
    try {
      // language=MySQL
      String sql =
          "SELECT "
              + MysqlJobRowMapper.HYDRATION_SELECT
              + " "
              + HYDRATION_FROM
              + " WHERE c.job_id = ?";
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(id))
              .getResultList();
      if (rows.isEmpty()) {
        return Optional.empty();
      }
      JobEntity job = mapper.hydrateJobEntity(rows.get(0));
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
      // language=MySQL
      String sql =
          """
          SELECT q.status, c.rec_status, c.terminal_status
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
      JobStatus rec = MysqlJobRowMapper.recStatusDecode(MysqlJobRowMapper.stringOrNull(row[1]));
      if (rec != null) {
        return rec;
      }
      String terminal = (String) row[2];
      if (terminal != null) {
        return JobStatus.valueOf(terminal);
      }
      log.errorf("Job %s has no live, recurring, or terminal status — invariant violation", id);
      return null;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("get job status", e);
    }
  }

  @SuppressWarnings("unchecked")
  List<JobEntity> findByIds(List<UUID> ids) {
    try {
      if (ids.isEmpty()) {
        return List.of();
      }
      List<JobEntity> jobs = new ArrayList<>(ids.size());
      for (int start = 0; start < ids.size(); start += FIND_BY_IDS_CHUNK_SIZE) {
        jobs.addAll(
            findByIdsChunk(
                ids.subList(start, Math.min(start + FIND_BY_IDS_CHUNK_SIZE, ids.size()))));
      }
      tags.hydrateTagsBatch(jobs);
      return jobs;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find jobs by ids", e);
    }
  }

  @SuppressWarnings("unchecked")
  private List<JobEntity> findByIdsChunk(List<UUID> ids) {
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=MySQL
    String sql =
        "SELECT "
            + MysqlJobRowMapper.HYDRATION_SELECT
            + " "
            + HYDRATION_FROM
            + " WHERE c.job_id IN ("
            + placeholders
            + ")";
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
  Optional<JobEntity> findActiveByBusinessKey(String businessKey) {
    try {
      // language=MySQL
      String sql =
          "SELECT br.owner_table, "
              + MysqlJobRowMapper.HYDRATION_SELECT
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
      Object[] hydrationRow = businessKeyHydrationRow(full);
      JobEntity job = mapper.hydrateJobEntity(hydrationRow);
      if (MysqlBusinessKeyReservations.OWNER_TABLE_QUEUE.equals(ownerTable)
          && hydrationRow[MysqlJobRowMapper.IDX_Q_STATUS] == null) {
        log.errorf(
            "bkres invariant violation: business_key=%s claims QUEUE owner job=%s but no hot row",
            businessKey, job.getId());
        return Optional.empty();
      }
      tags.hydrateTagsSingle(job);
      return Optional.of(job);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find active by business key", e);
    }
  }

  private static Object[] businessKeyHydrationRow(Object[] reservationAndJobRow) {
    return Arrays.copyOfRange(reservationAndJobRow, 1, 1 + MysqlJobRowMapper.HYDRATION_COL_COUNT);
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

  @SuppressWarnings("unchecked")
  List<JobEntity> findDependants(UUID parentJobId, int limit, int offset) {
    try {
      // language=MySQL
      String sql =
          "SELECT "
              + MysqlJobRowMapper.HYDRATION_SELECT
              + " "
              + HYDRATION_FROM
              + " WHERE c.depends_on = ? ORDER BY c.created_at ASC, c.job_id ASC";
      List<Object[]> rows =
          ctx.em()
              .createNativeQuery(sql)
              .setParameter(1, UuidByteArrayConverter.toBytes(parentJobId))
              .setFirstResult(offset)
              .setMaxResults(limit)
              .getResultList();
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

  Optional<Instant> findEarliestRecurringNextFire() {
    try {
      // language=MySQL
      String sql =
          """
          SELECT MIN(next_fire) FROM scheduler_job
          WHERE job_type = 'RECURRING' AND rec_status = 'P'
            AND next_fire IS NOT NULL
          """;
      List<?> results = ctx.em().createNativeQuery(sql).getResultList();
      if (results.isEmpty() || results.get(0) == null) {
        return Optional.empty();
      }
      Object val = results.get(0);
      if (val instanceof Timestamp ts) {
        return Optional.of(ts.toInstant());
      }
      return Optional.empty();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("find earliest recurring next fire", e);
    }
  }
}

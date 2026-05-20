package run.ratchet.store.mysql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.util.JobClaimSqlSupport;

final class MysqlJobClaimOperations implements JobClaimStore {

  private static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";

  private static final String CLAIM_SELECT_COLUMNS = ClaimColumn.selectClause();

  private final MysqlStoreContext ctx;
  private final MysqlJobCrudOperations jobs;

  MysqlJobClaimOperations(MysqlStoreContext ctx, MysqlJobCrudOperations jobs) {
    this.ctx = ctx;
    this.jobs = jobs;
  }

  // language=MySQL
  private static String buildClaimSql(
      String selectClause,
      String typeFilter,
      String tagFilterSql,
      String timeColumn,
      int boostInterval) {
    return """
        SELECT %s FROM scheduler_job_queue FORCE INDEX (idx_claim_executable)
        WHERE status = 'PENDING'
          AND %s <= NOW(3)
          AND %s%s
        ORDER BY %s
        LIMIT ?
        FOR UPDATE SKIP LOCKED"""
        .formatted(
            selectClause,
            timeColumn,
            typeFilter,
            tagFilterSql,
            buildMysqlBoostedOrderBy(timeColumn, boostInterval));
  }

  private static String buildMysqlBoostedOrderBy(String timeColumn, int boostInterval) {
    return JobClaimSqlSupport.buildBoostedOrderBy(
        timeColumn, "TIMESTAMPDIFF(MINUTE, " + timeColumn + ", NOW(3))", boostInterval);
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter) {
    List<Object[]> candidateRows;
    try {
      candidateRows = selectClaimCandidates(limit, tagFilter);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim jobs select", e);
    }

    if (candidateRows.isEmpty()) {
      return List.of();
    }

    try {
      List<UUID> candidateIds = new ArrayList<>(candidateRows.size());
      for (Object[] row : candidateRows) {
        candidateIds.add(new ClaimRow(row).jobId());
      }
      boolean[] updated = batchClaimRowsJpa(candidateIds, nodeId, Instant.now());

      List<UUID> claimedIds = new ArrayList<>(candidateIds.size());
      for (int i = 0; i < candidateIds.size(); i++) {
        if (updated[i]) {
          claimedIds.add(candidateIds.get(i));
        }
      }
      if (claimedIds.isEmpty()) {
        return List.of();
      }
      return JobClaimSqlSupport.reorderById(
          jobs.findByIds(claimedIds), claimedIds, JobEntity::getId);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim jobs update", e);
    }
  }

  @SuppressWarnings("unchecked")
  private List<Object[]> selectClaimCandidates(int limit, NodeTagFilter tagFilter) {
    int boostInterval = ctx.priorityBoostIntervalMinutes();
    String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "scheduler_job_queue");
    var query =
        ctx.em()
            .createNativeQuery(
                buildClaimSql(
                    CLAIM_SELECT_COLUMNS,
                    EXECUTABLE_JOB_TYPE_FILTER,
                    tagSql,
                    "scheduled_time",
                    boostInterval));
    int parameter = 1;
    parameter = JobClaimSqlSupport.bindTagFilter(query, tagFilter, parameter);
    if (boostInterval > 0) {
      query.setParameter(parameter++, boostInterval);
    }
    query.setParameter(parameter, limit);
    return query.getResultList();
  }

  // SQL template is a compile-time constant defined in this package; runtime values are bound as
  // JDBC parameters via setParameter.
  @Override
  @SuppressWarnings({"unchecked", "SqlSourceToSinkFlow"})
  public List<JobClaimDto> claimNextBatchOptimized(
      JobExecutionType jobType, int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0 || !MysqlJobRowMapper.isPollerExecutable(jobType)) {
      return List.of();
    }

    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "scheduler_job_queue");
      var query =
          ctx.em()
              .createNativeQuery(
                  buildClaimSql(
                      CLAIM_SELECT_COLUMNS,
                      "job_type = ?",
                      tagSql,
                      "scheduled_time",
                      boostInterval));
      int parameter = 1;
      query.setParameter(parameter++, jobType.name());
      parameter = JobClaimSqlSupport.bindTagFilter(query, tagFilter, parameter);
      if (boostInterval > 0) {
        query.setParameter(parameter++, boostInterval);
      }
      query.setParameter(parameter, limit);

      List<Object[]> rows =
          ctx.timedStoreOperation(
              "claim_lookup", query::getResultList, result -> result.isEmpty() ? "empty" : "hit");

      if (rows.isEmpty()) {
        return List.of();
      }

      return claimOptimizedRows(rows, nodeId);
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("optimized claim", e);
    }
  }

  private List<JobClaimDto> claimOptimizedRows(List<Object[]> rows, String nodeId) {
    return ctx.timedStoreOperation(
        "claim_mark_running_batch",
        () -> {
          Instant now = Instant.now();
          List<UUID> jobIds = new ArrayList<>(rows.size());
          for (Object[] row : rows) {
            jobIds.add(new ClaimRow(row).jobId());
          }
          boolean[] updated = batchClaimRowsJpa(jobIds, nodeId, now);
          List<JobClaimDto> claims = new ArrayList<>(rows.size());
          for (int i = 0; i < rows.size(); i++) {
            if (!updated[i]) {
              continue;
            }
            ClaimRow row = new ClaimRow(rows.get(i));
            claims.add(
                new JobClaimDto(
                    jobIds.get(i),
                    JobStatus.RUNNING,
                    row.jobType(),
                    row.priority(),
                    row.scheduledTime(),
                    row.version(),
                    row.timeoutSeconds(),
                    nodeId,
                    now,
                    row.businessKey(),
                    row.attempts(),
                    row.maxRetries()));
          }
          return claims;
        },
        claims -> claims.isEmpty() ? "miss" : "updated");
  }

  static List<String> claimSelectColumnNames() {
    return ClaimColumn.names();
  }

  static Map<String, Integer> claimSelectColumnIndexes() {
    return ClaimColumn.indexesByName();
  }

  private enum ClaimColumn {
    JOB_ID("job_id"),
    STATUS("status"),
    JOB_TYPE("job_type"),
    PRIORITY("priority"),
    SCHEDULED_TIME("scheduled_time"),
    VERSION("version"),
    TIMEOUT_SEC("timeout_sec"),
    PICKED_BY("picked_by"),
    PICKED_AT("picked_at"),
    BUSINESS_KEY("business_key"),
    ATTEMPTS("attempts"),
    MAX_RETRIES("max_retries");

    private final String sqlName;

    ClaimColumn(String sqlName) {
      this.sqlName = sqlName;
    }

    static String selectClause() {
      return Arrays.stream(values()).map(ClaimColumn::sqlName).collect(Collectors.joining(", "));
    }

    static List<String> names() {
      return Arrays.stream(values()).map(ClaimColumn::sqlName).toList();
    }

    static Map<String, Integer> indexesByName() {
      Map<String, Integer> indexes = new HashMap<>(values().length);
      for (ClaimColumn column : values()) {
        indexes.put(column.sqlName(), column.ordinal());
      }
      return Map.copyOf(indexes);
    }

    String sqlName() {
      return sqlName;
    }
  }

  private record ClaimRow(Object[] values) {
    ClaimRow {
      Objects.requireNonNull(values, "values");
    }

    UUID jobId() {
      return MysqlJobRowMapper.uuidOrNull(value(ClaimColumn.JOB_ID));
    }

    JobExecutionType jobType() {
      return JobExecutionType.valueOf((String) value(ClaimColumn.JOB_TYPE));
    }

    JobPriority priority() {
      return MysqlJobRowMapper.safeJobPriority(number(ClaimColumn.PRIORITY).intValue());
    }

    Instant scheduledTime() {
      return MysqlJobRowMapper.toInstant(value(ClaimColumn.SCHEDULED_TIME));
    }

    int version() {
      return number(ClaimColumn.VERSION).intValue();
    }

    int timeoutSeconds() {
      return number(ClaimColumn.TIMEOUT_SEC).intValue();
    }

    String businessKey() {
      return (String) value(ClaimColumn.BUSINESS_KEY);
    }

    int attempts() {
      return number(ClaimColumn.ATTEMPTS).intValue();
    }

    int maxRetries() {
      return number(ClaimColumn.MAX_RETRIES).intValue();
    }

    private Number number(ClaimColumn column) {
      return (Number) value(column);
    }

    private Object value(ClaimColumn column) {
      return values[column.ordinal()];
    }
  }

  private boolean[] batchClaimRowsJpa(List<UUID> jobIds, String nodeId, Instant now) {
    Timestamp nowTs = Timestamp.from(now);
    try {
      String placeholders = String.join(",", Collections.nCopies(jobIds.size(), "?"));
      /*
       * MySQL has no PostgreSQL-style UPDATE ... RETURNING for claiming and hydrating rows in one
       * statement. The caller already holds candidate row locks from FOR UPDATE SKIP LOCKED, so this
       * dialect keeps the portable MySQL pattern: UPDATE the locked PENDING rows, then reselect the
       * ids owned by this node to report which candidates were actually claimed.
       */
      // language=MySQL
      String updateSql =
          """
          UPDATE scheduler_job_queue
          SET status = 'RUNNING', picked_by = ?, picked_at = ?, updated_at = ?,
              version = version + 1
          WHERE job_id IN (%s) AND status = 'PENDING'
          ORDER BY job_id ASC
          """
              .formatted(placeholders);
      Query updateQuery = ctx.em().createNativeQuery(updateSql);
      int parameter = 1;
      updateQuery.setParameter(parameter++, nodeId);
      updateQuery.setParameter(parameter++, nowTs);
      updateQuery.setParameter(parameter++, nowTs);
      for (UUID id : jobIds) {
        updateQuery.setParameter(parameter++, UuidByteArrayConverter.toBytes(id));
      }
      updateQuery.executeUpdate();

      // language=MySQL
      String selectSql =
          """
          SELECT job_id FROM scheduler_job_queue
          WHERE job_id IN (%s) AND status = 'RUNNING' AND picked_by = ?
          ORDER BY job_id ASC
          """
              .formatted(placeholders);
      Query selectQuery = ctx.em().createNativeQuery(selectSql);
      parameter = 1;
      for (UUID id : jobIds) {
        selectQuery.setParameter(parameter++, UuidByteArrayConverter.toBytes(id));
      }
      selectQuery.setParameter(parameter, nodeId);
      @SuppressWarnings("unchecked")
      List<?> claimedRows = selectQuery.getResultList();

      Set<UUID> claimedIds = new HashSet<>(claimedRows.size());
      for (Object claimedRow : claimedRows) {
        claimedIds.add(MysqlJobRowMapper.uuidOrNull(claimedRow));
      }

      boolean[] updated = new boolean[jobIds.size()];
      for (int i = 0; i < jobIds.size(); i++) {
        updated[i] = claimedIds.contains(jobIds.get(i));
      }
      return updated;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("claim jobs", e);
    }
  }
}

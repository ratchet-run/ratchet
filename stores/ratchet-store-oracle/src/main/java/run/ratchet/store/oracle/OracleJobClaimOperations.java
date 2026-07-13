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
import run.ratchet.store.oracle.converter.UuidRawConverter;
import run.ratchet.store.spi.ExecutionTargetFilter;
import run.ratchet.store.spi.JobClaimStore;
import run.ratchet.store.util.JobClaimSqlSupport;
import run.ratchet.store.util.RowValues;

final class OracleJobClaimOperations implements JobClaimStore {

  private static final String EXECUTABLE_JOB_TYPE_FILTER =
      "job_type IN ('SINGLE','BATCH_CHILD','CHAIN_STEP','WORKFLOW_BRANCH')";

  private static final String CLAIM_SELECT_COLUMNS = ClaimColumn.selectClause();

  private final OracleStoreContext ctx;
  private final OracleJobCrudOperations jobs;

  OracleJobClaimOperations(OracleStoreContext ctx, OracleJobCrudOperations jobs) {
    this.ctx = ctx;
    this.jobs = jobs;
  }

  // Phase A of the two-phase Oracle claim: pick the top-N due candidates ordered by effective
  // priority WITHOUT locking. Oracle rejects FETCH FIRST combined with FOR UPDATE SKIP LOCKED
  // (ORA-02014), so the row-limited select cannot also lock. Phase B (lockClaimCandidates) locks
  // the resulting id set with FOR UPDATE SKIP LOCKED; the CAS UPDATE in batchClaimRowsJpa remains
  // the authoritative claim, so a candidate taken by another node between the phases is simply
  // dropped by the CAS, never double-claimed.
  // language=Oracle
  private static String buildClaimSql(
      String selectClause,
      String typeFilter,
      String executionTargetFilterSql,
      String tagFilterSql,
      String timeColumn,
      int boostInterval) {
    return """
        SELECT %s FROM scheduler_job_queue
        WHERE status = 'PENDING'
          AND %s <= CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
          AND %s%s%s
        ORDER BY %s
        FETCH FIRST ? ROWS ONLY"""
        .formatted(
            selectClause,
            timeColumn,
            typeFilter,
            executionTargetFilterSql,
            tagFilterSql,
            buildOracleBoostedOrderBy(timeColumn, boostInterval));
  }

  private static String buildOracleBoostedOrderBy(String timeColumn, int boostInterval) {
    // Minutes overdue = (now - scheduled) in days * 1440. Oracle has no TIMESTAMPDIFF; date
    // subtraction yields a day fraction. No quotes/colons so it passes the safe-fragment guard.
    return JobClaimSqlSupport.buildBoostedOrderBy(
        timeColumn,
        "(CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - CAST(" + timeColumn + " AS DATE)) * 1440",
        boostInterval);
  }

  /**
   * Phase B of the two-phase claim: lock the still-PENDING candidates with FOR UPDATE SKIP LOCKED
   * and return the Phase-A rows for the locked subset, preserving the boost order. Rows taken or
   * locked by another node are skipped and never reach the CAS update.
   */
  private List<Object[]> lockClaimCandidates(List<Object[]> candidateRows) {
    if (candidateRows.isEmpty()) {
      return List.of();
    }
    List<UUID> ids = new ArrayList<>(candidateRows.size());
    for (Object[] row : candidateRows) {
      ids.add(new ClaimRow(row).jobId());
    }
    String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
    // language=Oracle
    String sql =
        "SELECT job_id FROM scheduler_job_queue WHERE job_id IN ("
            + placeholders
            + ") AND status = 'PENDING' FOR UPDATE SKIP LOCKED";
    Query query = ctx.em().createNativeQuery(sql);
    int parameter = 1;
    for (UUID id : ids) {
      query.setParameter(parameter++, UuidRawConverter.toBytes(id));
    }
    @SuppressWarnings("unchecked")
    List<Object> lockedRows = query.getResultList();
    Set<UUID> locked = new HashSet<>(lockedRows.size());
    for (Object lockedRow : lockedRows) {
      locked.add(OracleJobRowMapper.uuidOrNull(lockedRow));
    }
    List<Object[]> result = new ArrayList<>(candidateRows.size());
    for (Object[] row : candidateRows) {
      if (locked.contains(new ClaimRow(row).jobId())) {
        result.add(row);
      }
    }
    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<JobEntity> claimNextBatch(int limit, String nodeId, NodeTagFilter tagFilter) {
    if (limit <= 0) {
      return List.of();
    }
    List<Object[]> candidateRows;
    try {
      candidateRows = lockClaimCandidates(selectClaimCandidates(limit, tagFilter));
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
                    "",
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
      JobExecutionType jobType,
      int limit,
      String nodeId,
      NodeTagFilter tagFilter,
      ExecutionTargetFilter executionTargetFilter) {
    if (limit <= 0 || !OracleJobRowMapper.isPollerExecutable(jobType)) {
      return List.of();
    }

    try {
      int boostInterval = ctx.priorityBoostIntervalMinutes();
      String tagSql = JobClaimSqlSupport.buildTagFilterSql(tagFilter, "scheduler_job_queue");
      String executionTargetSql =
          JobClaimSqlSupport.buildExecutionTargetFilterSql(
              executionTargetFilter, "execution_target");
      var query =
          ctx.em()
              .createNativeQuery(
                  buildClaimSql(
                      CLAIM_SELECT_COLUMNS,
                      "job_type = ?",
                      executionTargetSql,
                      tagSql,
                      "scheduled_time",
                      boostInterval));
      int parameter = 1;
      query.setParameter(parameter++, jobType.name());
      parameter =
          JobClaimSqlSupport.bindExecutionTargetFilter(query, executionTargetFilter, parameter);
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

      rows = lockClaimCandidates(rows);
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
                    row.maxRetries(),
                    row.executionTarget(),
                    row.dependsOn()));
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

  static String claimSelectClause() {
    return CLAIM_SELECT_COLUMNS;
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
    MAX_RETRIES("max_retries"),
    EXECUTION_TARGET("execution_target"),
    DEPENDS_ON("depends_on");

    private final String sqlName;

    ClaimColumn(String sqlName) {
      this.sqlName = sqlName;
    }

    static String selectClause() {
      return Arrays.stream(values())
          .map(ClaimColumn::selectExpression)
          .collect(Collectors.joining(", "));
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

    String selectExpression() {
      if (this == DEPENDS_ON) {
        // Phase A intentionally does not lock cold metadata. Project the immutable parent pointer
        // without adding a nonexistent column to scheduler_job_queue.
        return "(SELECT cold_job.depends_on FROM scheduler_job cold_job"
            + " WHERE cold_job.job_id = scheduler_job_queue.job_id) AS depends_on";
      }
      return sqlName;
    }
  }

  private record ClaimRow(Object[] values) {
    ClaimRow {
      Objects.requireNonNull(values, "values");
    }

    UUID jobId() {
      return OracleJobRowMapper.uuidOrNull(value(ClaimColumn.JOB_ID));
    }

    JobExecutionType jobType() {
      return JobExecutionType.valueOf((String) value(ClaimColumn.JOB_TYPE));
    }

    JobPriority priority() {
      return OracleJobRowMapper.safeJobPriority(number(ClaimColumn.PRIORITY).intValue());
    }

    Instant scheduledTime() {
      return RowValues.instantOrNull(value(ClaimColumn.SCHEDULED_TIME));
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

    String executionTarget() {
      return (String) value(ClaimColumn.EXECUTION_TARGET);
    }

    UUID dependsOn() {
      return OracleJobRowMapper.uuidOrNull(value(ClaimColumn.DEPENDS_ON));
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
       * Oracle has no PostgreSQL-style UPDATE ... RETURNING for claiming and hydrating rows in one
       * statement. The caller already holds candidate row locks from FOR UPDATE SKIP LOCKED, so every
       * locked candidate is still PENDING at UPDATE time and the affected-row count equals the
       * candidate count: the whole candidate set is claimed. A read-back SELECT is kept only as a
       * defensive fallback for the lock-impossible count mismatch, matching PostgreSQL's two
       * round-trip claim instead of paying a third.
       */
      // language=Oracle
      String updateSql =
          """
          UPDATE scheduler_job_queue
          SET status = 'RUNNING', picked_by = ?, picked_at = ?, updated_at = ?,
              version = version + 1
          WHERE job_id IN (%s) AND status = 'PENDING'
          """
              .formatted(placeholders);
      Query updateQuery = ctx.em().createNativeQuery(updateSql);
      int parameter = 1;
      updateQuery.setParameter(parameter++, nodeId);
      updateQuery.setParameter(parameter++, nowTs);
      updateQuery.setParameter(parameter++, nowTs);
      for (UUID id : jobIds) {
        updateQuery.setParameter(parameter++, UuidRawConverter.toBytes(id));
      }
      int affected = updateQuery.executeUpdate();
      if (affected == jobIds.size()) {
        boolean[] claimed = new boolean[jobIds.size()];
        Arrays.fill(claimed, true);
        return claimed;
      }

      // language=Oracle
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
        selectQuery.setParameter(parameter++, UuidRawConverter.toBytes(id));
      }
      selectQuery.setParameter(parameter, nodeId);
      @SuppressWarnings("unchecked")
      List<?> claimedRows = selectQuery.getResultList();

      Set<UUID> claimedIds = new HashSet<>(claimedRows.size());
      for (Object claimedRow : claimedRows) {
        claimedIds.add(OracleJobRowMapper.uuidOrNull(claimedRow));
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

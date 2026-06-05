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
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.query.JobQueryCursor;

/**
 * Dashboard-oriented search and count queries over the PostgreSQL store.
 *
 * <p>Uses the same LEFT JOIN projection as {@link PostgresqlJobReadOperations} so that {@link
 * PostgresqlJobRowMapper#hydrate} can reconstruct both live and terminal jobs from a single result
 * set. WHERE clause conditions are built with a parameterized {@link StringBuilder}; no
 * user-supplied values are concatenated into SQL strings.
 *
 * <p>When {@link JobFilter#includeArchived()} is true and no principal filter is active, a {@code
 * UNION ALL} pulls matching rows from {@code scheduler_job_archive} into the same result set. See
 * MySQL counterpart for column-mapping details; the PostgreSQL projection is identical in
 * structure.
 */
final class PostgresqlJobQueryOperations {

  /*
   * Keep this builder dialect-local. It mirrors the MySQL builder in shape, but the common-looking
   * clauses sit next to PostgreSQL-specific hydration columns, trace JSON extraction, archive UNION
   * positions, and native UUID binding. Limit future sharing to small pure helpers with contract
   * tests.
   */

  // language=PostgreSQL
  private static final String HYDRATION_FROM =
      """
      FROM scheduler_job c
      LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
      """;

  /**
   * Archive rows projected to match {@link PostgresqlJobRowMapper#hydrationSelect()} column
   * positions. NULL placeholders occupy columns not present in the archive.
   *
   * <p>This projection MUST emit exactly the same number of columns as {@code hydrationSelect()}.
   * The two SELECTs are combined with {@code UNION ALL}; a column-count mismatch is rejected by
   * PostgreSQL before any row is returned, breaking every archive-inclusive search. The leading two
   * NULLs cover {@code payload}/{@code params}, which the archive does not retain.
   */
  // language=PostgreSQL
  private static final String ARCHIVE_PROJECTION =
      """
      a.original_job_id,
      a.job_type,
      a.priority,
      a.max_retries,
      a.backoff_policy,
      a.backoff_param_ms,
      a.timeout_sec,
      a.cron_expr,
      a.zone_id,
      NULL,
      NULL,
      a.target_class,
      a.method_name,
      NULL,
      a.business_key,
      NULL,
      NULL,
      NULL,
      a.depended_on,
      a.superseded_by,
      a.original_created_at,
      NULL,
      a.final_status,
      a.final_error,
      a.total_attempts,
      a.completion_time,
      a.first_execution_time,
      NULL,
      a.total_execution_time_ms,
      a.queue_wait_ms,
      a.job_result,
      a.result_type,
      NULL,
      NULL,
      NULL::text,
      a.original_scheduled_time,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      a.archived_at,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL\
      """;

  private static final int MAX_RESULT_LIMIT = 1000;

  /*
   * PostgreSQL allows ORDER BY by output-column position after UNION. These constants are mapped
   * to PostgresqlJobRowMapper.hydrationSelect()/ARCHIVE_PROJECTION positions, which must stay in
   * lock-step. They are internal constants, not caller input; dynamic ORDER BY text is bounded to
   * this mapping plus ASC/DESC derived from a boolean.
   */
  private static final int POS_JOB_ID = 1;
  private static final int POS_PRIORITY = 3;
  private static final int POS_CREATED_AT = 21;
  private static final int POS_TERMINAL_STATUS = 23;
  private static final int POS_Q_SCHEDULED_TIME = 36;
  private static final int POS_Q_UPDATED_AT = 43;

  private final PostgresqlStoreContext ctx;
  private final PostgresqlTagOperations tags;

  PostgresqlJobQueryOperations(PostgresqlStoreContext ctx, PostgresqlTagOperations tags) {
    this.ctx = ctx;
    this.tags = tags;
  }

  private static Object parseSortValue(JobQueryCursor cursor) {
    return switch (cursor.sortField()) {
      case CREATED_AT, SCHEDULED_TIME, UPDATED_AT ->
          Timestamp.from(Instant.parse(cursor.sortValue()));
      case PRIORITY -> Integer.parseInt(cursor.sortValue());
      case STATUS -> cursor.sortValue();
    };
  }

  private static ParsedCursor parseCursor(JobFilter filter) {
    if (filter == null || filter.cursor() == null) {
      return null;
    }
    try {
      JobQueryCursor cursor = JobQueryCursor.decode(filter.cursor());
      if (!cursor.matchesFilterSort(filter)) {
        // Cursor was minted for a different sort field/direction; seeking on it while ORDER BY
        // uses the live sort would skip or repeat rows. Fall back to offset pagination instead.
        return null;
      }
      return new ParsedCursor(cursor, parseSortValue(cursor));
    } catch (IllegalArgumentException | DateTimeParseException ignored) {
      return null;
    }
  }

  private static void appendStringEq(
      String col, String value, StringBuilder where, List<Object> params) {
    if (value == null || value.isEmpty()) {
      return;
    }
    and(where, col + " = ?");
    params.add(value);
  }

  // ── WHERE clause builder ────────────────────────────────────────────────

  private static void appendInstantGte(
      String col, Instant value, StringBuilder where, List<Object> params) {
    if (value == null) {
      return;
    }
    and(where, col + " >= ?");
    params.add(Timestamp.from(value));
  }

  private static void appendInstantLt(
      String col, Instant value, StringBuilder where, List<Object> params) {
    if (value == null) {
      return;
    }
    and(where, col + " < ?");
    params.add(Timestamp.from(value));
  }

  private static String buildOrderBy(JobFilter filter) {
    if (filter == null) {
      return " ORDER BY c.created_at DESC, c.job_id ASC";
    }
    JobQuerySortField field =
        filter.sortField() != null ? filter.sortField() : JobQuerySortField.CREATED_AT;
    String dir = filter.sortAscending() ? "ASC" : "DESC";
    return " ORDER BY " + sortColumn(field) + " " + dir + ", c.job_id ASC";
  }

  private static String sortColumn(JobQuerySortField field) {
    return switch (field) {
      case CREATED_AT -> "c.created_at";
      case SCHEDULED_TIME -> "COALESCE(q.scheduled_time, c.execution_start_time, c.created_at)";
      case UPDATED_AT -> "COALESCE(q.updated_at, c.terminated_at, c.created_at)";
      case PRIORITY -> "c.priority";
      case STATUS -> "COALESCE(q.status, c.terminal_status)";
    };
  }

  private static String archiveSortColumn(JobQuerySortField field) {
    return switch (field) {
      case CREATED_AT -> "a.original_created_at";
      case SCHEDULED_TIME -> "a.original_scheduled_time";
      case UPDATED_AT -> "a.archived_at";
      case PRIORITY -> "a.priority";
      case STATUS -> "a.final_status";
    };
  }

  private static int unionSortColumnPosition(JobFilter filter) {
    JobQuerySortField field =
        (filter == null || filter.sortField() == null)
            ? JobQuerySortField.CREATED_AT
            : filter.sortField();
    return switch (field) {
      case CREATED_AT -> POS_CREATED_AT;
      case SCHEDULED_TIME -> POS_Q_SCHEDULED_TIME;
      case UPDATED_AT -> POS_Q_UPDATED_AT;
      case PRIORITY -> POS_PRIORITY;
      case STATUS -> POS_TERMINAL_STATUS;
    };
  }

  /**
   * Decides whether the archive UNION should be appended to the live-row query.
   *
   * <p>The archive UNION is intentionally skipped when {@code callerPrincipal} is non-null because
   * principal-scoped queries do not span archived rows: the archive table does not carry the
   * principal column, so appending the UNION would silently return archived rows belonging to other
   * principals. Callers that need both principal scoping and archive inclusion must handle that at
   * the policy or service layer; see {@code JobAuthorizationPolicy.filterForPrincipal} for the
   * rationale.
   */
  private static boolean useArchive(JobFilter filter) {
    return filter != null
        && filter.includeArchived()
        && (filter.callerPrincipal() == null || filter.callerPrincipal().isEmpty());
  }

  private static void and(StringBuilder where, String condition) {
    if (where.length() > 0) {
      where.append(" AND ");
    }
    where.append(condition);
  }

  private static String placeholders(int count) {
    return "?,".repeat(count - 1) + "?";
  }

  private static void bindParams(Query q, List<Object> params) {
    for (int i = 0; i < params.size(); i++) {
      q.setParameter(i + 1, params.get(i));
    }
  }

  /*
   * LIMIT/OFFSET are concatenated because JPA providers vary in native-query support for binding
   * them. The values are primitive ints computed by this store layer (limit is clamped by caller
   * before use), so they cannot carry SQL tokens.
   */
  private static String limitOffsetClause(int limit, int offset) {
    return " LIMIT " + limit + " OFFSET " + offset;
  }

  @SuppressWarnings("unchecked")
  List<JobEntity> searchJobs(JobFilter filter, int limit, int offset) {
    boolean archive = useArchive(filter);
    int safeLimit = Math.min(limit, MAX_RESULT_LIMIT);
    ParsedCursor cursor = parseCursor(filter);
    // A valid cursor uses keyset pagination, so offset applies only without a usable cursor.
    int effectiveOffset = cursor != null ? 0 : offset;

    List<Object> params = new ArrayList<>();
    String sql;
    if (archive) {
      sql = buildUnionSearchSql(filter, params, safeLimit, effectiveOffset);
    } else {
      sql =
          "SELECT "
              + PostgresqlJobRowMapper.hydrationSelect()
              + " "
              + HYDRATION_FROM
              + buildWhere(filter, params)
              + buildOrderBy(filter)
              + limitOffsetClause(safeLimit, effectiveOffset);
    }

    try {
      Query q = ctx.em().createNativeQuery(sql);
      bindParams(q, params);
      List<Object[]> rows = q.getResultList();
      List<JobEntity> result = new ArrayList<>(rows.size());
      for (Object[] row : rows) {
        JobEntity job = PostgresqlJobRowMapper.hydrate(row);
        if (job != null) {
          result.add(job);
        }
      }
      tags.hydrateTagsBatch(result);
      return result;
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("search jobs", e);
    }
  }

  long countJobs(JobFilter filter) {
    boolean archive = useArchive(filter);
    List<Object> params = new ArrayList<>();
    String sql;
    if (archive) {
      sql =
          "SELECT COUNT(*) FROM ("
              + "SELECT 1 "
              + HYDRATION_FROM
              + buildWhere(filter, params)
              + " UNION ALL "
              + "SELECT 1 FROM scheduler_job_archive a"
              + buildArchiveWhere(filter, params)
              + ") AS combined";
    } else {
      // language=PostgreSQL
      sql = "SELECT COUNT(*) " + HYDRATION_FROM + buildWhere(filter, params);
    }
    try {
      Query q = ctx.em().createNativeQuery(sql);
      bindParams(q, params);
      return ((Number) q.getSingleResult()).longValue();
    } catch (RuntimeException e) {
      throw ctx.translateTransientStoreException("count jobs", e);
    }
  }

  private String buildWhere(JobFilter filter, List<Object> params) {
    if (filter == null) {
      return "";
    }
    StringBuilder where = new StringBuilder();

    appendStatusCondition(filter, where, params);
    appendJobTypeCondition(filter, where, params);
    appendPriorityCondition(filter, where, params);
    appendStringEq("c.business_key", filter.businessKey(), where, params);
    appendStringEq("c.idempotency_key", filter.idempotencyKey(), where, params);
    appendStringEq("c.target_class", filter.targetClass(), where, params);
    appendStringEq("c.caller_principal", filter.callerPrincipal(), where, params);
    appendStringEq("q.picked_by", filter.pickedBy(), where, params);
    appendStringEq("c.resource_name", filter.resourceName(), where, params);
    appendStringEq("c.trace_context->>'traceparent'", filter.traceCorrelationId(), where, params);
    appendParentJobId(filter, where, params);
    appendTagCondition(filter, where, params);
    appendInstantGte("c.created_at", filter.createdAfter(), where, params);
    appendInstantLt("c.created_at", filter.createdBefore(), where, params);
    appendInstantGte(
        "COALESCE(q.scheduled_time, c.execution_start_time)",
        filter.scheduledAfter(),
        where,
        params);
    appendInstantLt(
        "COALESCE(q.scheduled_time, c.execution_start_time)",
        filter.scheduledBefore(),
        where,
        params);
    appendInstantGte(
        "COALESCE(q.updated_at, c.terminated_at, c.created_at)",
        filter.updatedAfter(),
        where,
        params);
    appendCursorCondition(filter, where, params);

    if (where.length() == 0) {
      return "";
    }
    return " WHERE " + where;
  }

  private String buildArchiveWhere(JobFilter filter, List<Object> params) {
    if (filter == null) {
      return "";
    }
    StringBuilder where = new StringBuilder();

    appendArchiveStatusCondition(filter, where, params);
    appendArchiveJobTypeCondition(filter, where, params);
    appendArchivePriorityCondition(filter, where, params);
    appendStringEq("a.business_key", filter.businessKey(), where, params);
    appendStringEq("a.target_class", filter.targetClass(), where, params);
    appendArchiveParentJobId(filter, where, params);
    appendInstantGte("a.original_created_at", filter.createdAfter(), where, params);
    appendInstantLt("a.original_created_at", filter.createdBefore(), where, params);
    appendInstantGte("a.original_scheduled_time", filter.scheduledAfter(), where, params);
    appendInstantLt("a.original_scheduled_time", filter.scheduledBefore(), where, params);
    appendInstantGte("a.archived_at", filter.updatedAfter(), where, params);
    appendArchiveCursorCondition(filter, where, params);

    return where.length() == 0 ? "" : " WHERE " + where;
  }

  private String buildUnionSearchSql(JobFilter filter, List<Object> params, int limit, int offset) {
    List<Object> liveParams = new ArrayList<>();
    String liveWhere = buildWhere(filter, liveParams);
    List<Object> archiveParams = new ArrayList<>();
    String archiveWhere = buildArchiveWhere(filter, archiveParams);
    params.addAll(liveParams);
    params.addAll(archiveParams);

    int sortPos = unionSortColumnPosition(filter);
    String dir = (filter != null && filter.sortAscending()) ? "ASC" : "DESC";

    return "SELECT * FROM ("
        + "SELECT "
        + PostgresqlJobRowMapper.hydrationSelect()
        + " "
        + HYDRATION_FROM
        + liveWhere
        + " UNION ALL "
        + "SELECT "
        + ARCHIVE_PROJECTION
        + " FROM scheduler_job_archive a"
        + archiveWhere
        + ") AS combined"
        + " ORDER BY "
        + sortPos
        + " "
        + dir
        + ", "
        + POS_JOB_ID
        + " ASC"
        + limitOffsetClause(limit, offset);
  }

  private void appendStatusCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    Set<JobStatus> statuses = filter.statuses();
    if (statuses == null || statuses.isEmpty()) {
      return;
    }
    Set<JobStatus> live = EnumSet.noneOf(JobStatus.class);
    Set<JobStatus> terminal = EnumSet.noneOf(JobStatus.class);
    for (JobStatus s : statuses) {
      if (PostgresqlJobRowMapper.isLiveStatus(s)) {
        live.add(s);
      } else {
        terminal.add(s);
      }
    }

    if (!live.isEmpty() && !terminal.isEmpty()) {
      String livePh = placeholders(live.size());
      String termPh = placeholders(terminal.size());
      and(
          where,
          "(q.status IN ("
              + livePh
              + ") OR (q.job_id IS NULL AND c.terminal_status IN ("
              + termPh
              + ")))");
      live.stream().map(JobStatus::name).forEach(params::add);
      terminal.stream().map(JobStatus::name).forEach(params::add);
    } else if (!live.isEmpty()) {
      and(where, "q.status IN (" + placeholders(live.size()) + ")");
      live.stream().map(JobStatus::name).forEach(params::add);
    } else {
      and(
          where,
          "(q.job_id IS NULL AND c.terminal_status IN (" + placeholders(terminal.size()) + "))");
      terminal.stream().map(JobStatus::name).forEach(params::add);
    }
  }

  private void appendArchiveStatusCondition(
      JobFilter filter, StringBuilder where, List<Object> params) {
    Set<JobStatus> statuses = filter.statuses();
    if (statuses == null || statuses.isEmpty()) {
      return;
    }
    Set<JobStatus> terminal =
        statuses.stream()
            .filter(s -> !PostgresqlJobRowMapper.isLiveStatus(s))
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(JobStatus.class)));
    if (terminal.isEmpty()) {
      and(where, "1 = 0");
      return;
    }
    and(where, "a.final_status IN (" + placeholders(terminal.size()) + ")");
    terminal.stream().map(JobStatus::name).forEach(params::add);
  }

  private void appendJobTypeCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    appendJobTypeCondition("c.job_type", filter.types(), where, params);
  }

  // ── ORDER BY builder ────────────────────────────────────────────────────

  private void appendArchiveJobTypeCondition(
      JobFilter filter, StringBuilder where, List<Object> params) {
    appendJobTypeCondition("a.job_type", filter.types(), where, params);
  }

  private void appendPriorityCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    appendPriorityCondition("c.priority", filter.priorities(), where, params);
  }

  private void appendArchivePriorityCondition(
      JobFilter filter, StringBuilder where, List<Object> params) {
    appendPriorityCondition("a.priority", filter.priorities(), where, params);
  }

  // ── Helpers ─────────────────────────────────────────────────────────────

  private void appendParentJobId(JobFilter filter, StringBuilder where, List<Object> params) {
    UUID parentJobId = filter.parentJobId();
    if (parentJobId == null) {
      return;
    }
    appendUuidEq("c.depends_on", parentJobId, where, params);
  }

  private void appendArchiveParentJobId(
      JobFilter filter, StringBuilder where, List<Object> params) {
    UUID parentJobId = filter.parentJobId();
    if (parentJobId == null) {
      return;
    }
    appendUuidEq("a.depended_on", parentJobId, where, params);
  }

  private void appendJobTypeCondition(
      String column, Set<JobType> types, StringBuilder where, List<Object> params) {
    if (types == null || types.isEmpty()) {
      return;
    }
    List<String> execTypeNames =
        Stream.of(JobExecutionType.values())
            .filter(e -> types.contains(e.toPublicType()))
            .map(Enum::name)
            .collect(Collectors.toList());
    if (execTypeNames.isEmpty()) {
      return;
    }
    and(where, column + " IN (" + placeholders(execTypeNames.size()) + ")");
    params.addAll(execTypeNames);
  }

  private void appendPriorityCondition(
      String column, Set<JobPriority> priorities, StringBuilder where, List<Object> params) {
    if (priorities == null || priorities.isEmpty()) {
      return;
    }
    and(where, column + " IN (" + placeholders(priorities.size()) + ")");
    priorities.stream().map(JobPriority::ordinal).forEach(params::add);
  }

  private void appendUuidEq(String column, UUID value, StringBuilder where, List<Object> params) {
    and(where, column + " = ?");
    params.add(value);
  }

  private void appendTagCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    Set<String> filterTags = filter.tags();
    if (filterTags == null || filterTags.isEmpty()) {
      return;
    }
    // JobFilter tags use any-tag (OR) semantics: a job matches when it has at least one requested
    // tag. All-tag semantics would need grouped counts or repeated EXISTS predicates.
    and(
        where,
        "c.job_id IN (SELECT job_id FROM scheduler_job_tag WHERE tag IN ("
            + placeholders(filterTags.size())
            + "))");
    params.addAll(filterTags);
  }

  private void appendCursorCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    ParsedCursor parsed = parseCursor(filter);
    if (parsed == null) {
      return;
    }
    JobQueryCursor cursor = parsed.cursor();
    String sortCol = sortColumn(cursor.sortField());
    String op = filter.sortAscending() ? ">" : "<";
    and(where, "(" + sortCol + " " + op + " ? OR (" + sortCol + " = ? AND c.job_id > ?))");
    params.add(parsed.sortValue());
    params.add(parsed.sortValue());
    params.add(cursor.jobId());
  }

  private void appendArchiveCursorCondition(
      JobFilter filter, StringBuilder where, List<Object> params) {
    ParsedCursor parsed = parseCursor(filter);
    if (parsed == null) {
      return;
    }
    JobQueryCursor cursor = parsed.cursor();
    String sortCol = archiveSortColumn(cursor.sortField());
    String op = filter.sortAscending() ? ">" : "<";
    and(where, "(" + sortCol + " " + op + " ? OR (" + sortCol + " = ? AND a.original_job_id > ?))");
    params.add(parsed.sortValue());
    params.add(parsed.sortValue());
    params.add(cursor.jobId());
  }

  private record ParsedCursor(JobQueryCursor cursor, Object sortValue) {}
}

package run.ratchet.store.mysql;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jboss.logging.Logger;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.mysql.converter.UuidByteArrayConverter;
import run.ratchet.store.query.JobQueryCursor;

/**
 * Dashboard-oriented search and count queries over the MySQL hot/cold store.
 *
 * <p>Uses the same LEFT JOIN projection as {@link MysqlJobReadOperations} so that {@link
 * MysqlJobRowMapper#hydrateJobEntity} can reconstruct both live and terminal jobs from a single
 * result set. WHERE clause conditions are built with a parameterized {@link StringBuilder}; no
 * user-supplied values are concatenated into SQL strings.
 *
 * <p>When {@link JobFilter#includeArchived()} is true and no principal filter is active, a {@code
 * UNION ALL} pulls matching rows from {@code scheduler_job_archive} into the same result set. The
 * archive projection maps its columns to the same positions as the live hydration SELECT so that
 * the existing row mapper reconstructs archived jobs correctly (with NULL queue fields). Note: tag
 * filtering and traceCorrelationId filtering are not applied to archived rows since those columns
 * are not present in the archive table. The caller-principal check is intentionally skipped for
 * archived rows — callers that require strict per-principal scoping should keep {@code
 * includeArchived=false} (the default).
 */
final class MysqlJobQueryOperations {

  private static final Logger log = Logger.getLogger(MysqlJobQueryOperations.class);

  /*
   * Keep this builder dialect-local. It mirrors the PostgreSQL builder in shape, but the
   * common-looking clauses sit next to MySQL-specific hydration columns, trace extraction,
   * byte-array UUID binding, and archive tag hydration rules. Limit future sharing to small pure
   * helpers with contract tests.
   */

  // language=MySQL
  private static final String HYDRATION_FROM =
      """
      FROM scheduler_job c
      LEFT JOIN scheduler_job_queue q ON q.job_id = c.job_id
      """;

  /**
   * Archive rows projected to match {@link MysqlJobRowMapper#HYDRATION_SELECT} column positions.
   * NULL placeholders occupy positions whose data is unavailable in the archive.
   */
  // language=MySQL
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
      NULL,
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
      NULL\
      """;

  private static final int MAX_IN_CLAUSE = 1000;

  // Positional column numbers (1-indexed) in the hydration SELECT for UNION ORDER BY
  private static final int POS_JOB_ID = 1;
  private static final int POS_PRIORITY = 3;
  private static final int POS_CREATED_AT = 22;
  private static final int POS_TERMINAL_STATUS = 24;
  private static final int POS_Q_SCHEDULED_TIME = 37;
  private static final int POS_Q_UPDATED_AT = 44;

  private final MysqlStoreContext ctx;
  private final MysqlJobRowMapper mapper;
  private final MysqlTagOperations tags;

  MysqlJobQueryOperations(
      MysqlStoreContext ctx, MysqlJobRowMapper mapper, MysqlTagOperations tags) {
    this.ctx = ctx;
    this.mapper = mapper;
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

  private static boolean useArchive(JobFilter filter) {
    // Skip archive when principal scoping is active to prevent auth bypass
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

  @SuppressWarnings("unchecked")
  List<JobEntity> searchJobs(JobFilter filter, int limit, int offset) {
    boolean archive = useArchive(filter);
    int safeLimit = Math.min(limit, MAX_IN_CLAUSE);
    int effectiveOffset = (filter != null && filter.cursor() != null) ? 0 : offset;

    List<Object> params = new ArrayList<>();
    String sql;
    if (archive) {
      sql = buildUnionSearchSql(filter, params, safeLimit, effectiveOffset);
    } else {
      sql =
          "SELECT "
              + MysqlJobRowMapper.HYDRATION_SELECT
              + " "
              + HYDRATION_FROM
              + buildWhere(filter, params)
              + buildOrderBy(filter)
              + " LIMIT "
              + safeLimit
              + " OFFSET "
              + effectiveOffset;
    }

    Query q = ctx.em().createNativeQuery(sql);
    bindParams(q, params);
    List<Object[]> rows = q.getResultList();
    List<JobEntity> result = new ArrayList<>(rows.size());
    List<JobEntity> jobsToHydrate = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      JobEntity job = mapper.hydrateJobEntity(row);
      if (job != null) {
        // Skip tag hydration for archive rows (null q.status marks terminal-only rows from archive)
        if (!archive || row[MysqlJobRowMapper.IDX_Q_STATUS] != null) {
          jobsToHydrate.add(job);
        }
        result.add(job);
      }
    }
    tags.hydrateTagsBatch(jobsToHydrate);
    return result;
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
      // language=MySQL
      sql = "SELECT COUNT(*) " + HYDRATION_FROM + buildWhere(filter, params);
    }
    Query q = ctx.em().createNativeQuery(sql);
    bindParams(q, params);
    return ((Number) q.getSingleResult()).longValue();
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
    appendStringEq("c.trace_id_extracted", filter.traceCorrelationId(), where, params);
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

  /** Builds a WHERE clause restricted to columns available in {@code scheduler_job_archive}. */
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
        + MysqlJobRowMapper.HYDRATION_SELECT
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
        + " LIMIT "
        + limit
        + " OFFSET "
        + offset;
  }

  private void appendStatusCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    Set<JobStatus> statuses = filter.statuses();
    if (statuses == null || statuses.isEmpty()) {
      return;
    }
    Set<JobStatus> live = EnumSet.noneOf(JobStatus.class);
    Set<JobStatus> terminal = EnumSet.noneOf(JobStatus.class);
    for (JobStatus s : statuses) {
      if (MysqlJobRowMapper.isLiveStatus(s)) {
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
          "("
              + statusInCondition("q.status", livePh)
              + " OR (q.job_id IS NULL AND "
              + statusInCondition("c.terminal_status", termPh)
              + "))");
      addStatusParams(live, params);
      addStatusParams(terminal, params);
    } else if (!live.isEmpty()) {
      and(where, statusInCondition("q.status", placeholders(live.size())));
      addStatusParams(live, params);
    } else {
      and(
          where,
          "(q.job_id IS NULL AND "
              + statusInCondition("c.terminal_status", placeholders(terminal.size()))
              + ")");
      addStatusParams(terminal, params);
    }
  }

  private void appendArchiveStatusCondition(
      JobFilter filter, StringBuilder where, List<Object> params) {
    Set<JobStatus> statuses = filter.statuses();
    if (statuses == null || statuses.isEmpty()) {
      return;
    }
    // Archive only contains terminal statuses; filter to the terminal subset
    Set<JobStatus> terminal = terminalStatuses(statuses);
    if (terminal.isEmpty()) {
      // Caller wants only live statuses; exclude all archive rows
      and(where, "1 = 0");
      return;
    }
    and(where, statusInCondition("a.final_status", placeholders(terminal.size())));
    addStatusParams(terminal, params);
  }

  private void appendJobTypeCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    appendJobTypeCondition(filter, "c.job_type", where, params);
  }

  // ── ORDER BY builder ────────────────────────────────────────────────────

  private void appendArchiveJobTypeCondition(
      JobFilter filter, StringBuilder where, List<Object> params) {
    appendJobTypeCondition(filter, "a.job_type", where, params);
  }

  private void appendJobTypeCondition(
      JobFilter filter, String column, StringBuilder where, List<Object> params) {
    Set<JobType> types = filter.types();
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

  private void appendPriorityCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    appendPriorityCondition(filter, "c.priority", where, params);
  }

  private void appendArchivePriorityCondition(
      JobFilter filter, StringBuilder where, List<Object> params) {
    appendPriorityCondition(filter, "a.priority", where, params);
  }

  private void appendPriorityCondition(
      JobFilter filter, String column, StringBuilder where, List<Object> params) {
    Set<JobPriority> priorities = filter.priorities();
    if (priorities == null || priorities.isEmpty()) {
      return;
    }
    and(where, column + " IN (" + placeholders(priorities.size()) + ")");
    priorities.stream().map(JobPriority::ordinal).forEach(params::add);
  }

  private static Set<JobStatus> terminalStatuses(Set<JobStatus> statuses) {
    return statuses.stream()
        .filter(s -> !MysqlJobRowMapper.isLiveStatus(s))
        .collect(Collectors.toCollection(() -> EnumSet.noneOf(JobStatus.class)));
  }

  private static String statusInCondition(String column, String placeholders) {
    return column + " IN (" + placeholders + ")";
  }

  private static void addStatusParams(Set<JobStatus> statuses, List<Object> params) {
    statuses.stream().map(JobStatus::name).forEach(params::add);
  }

  // ── Helpers ─────────────────────────────────────────────────────────────

  private void appendParentJobId(JobFilter filter, StringBuilder where, List<Object> params) {
    UUID parentJobId = filter.parentJobId();
    if (parentJobId == null) {
      return;
    }
    and(where, "c.depends_on = ?");
    params.add(UuidByteArrayConverter.toBytes(parentJobId));
  }

  private void appendArchiveParentJobId(
      JobFilter filter, StringBuilder where, List<Object> params) {
    UUID parentJobId = filter.parentJobId();
    if (parentJobId == null) {
      return;
    }
    and(where, "a.depended_on = ?");
    params.add(UuidByteArrayConverter.toBytes(parentJobId));
  }

  private void appendTagCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    Set<String> filterTags = filter.tags();
    if (filterTags == null || filterTags.isEmpty()) {
      return;
    }
    // ANY-of match: job must have at least one of the specified tags
    and(
        where,
        "c.job_id IN (SELECT job_id FROM scheduler_job_tag WHERE tag IN ("
            + placeholders(filterTags.size())
            + "))");
    params.addAll(filterTags);
  }

  private void appendCursorCondition(JobFilter filter, StringBuilder where, List<Object> params) {
    if (filter == null || filter.cursor() == null) {
      return;
    }
    try {
      JobQueryCursor c = JobQueryCursor.decode(filter.cursor());
      String sortCol = sortColumn(c.sortField());
      String op = filter.sortAscending() ? ">" : "<";
      and(where, "(" + sortCol + " " + op + " ? OR (" + sortCol + " = ? AND c.job_id > ?))");
      Object sortVal = parseSortValue(c);
      params.add(sortVal);
      params.add(sortVal);
      params.add(UuidByteArrayConverter.toBytes(c.jobId()));
    } catch (IllegalArgumentException e) {
      log.warnf(e, "Ignoring malformed job-query cursor; falling back to offset pagination");
    }
  }
}

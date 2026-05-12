package run.ratchet.store.util;

import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import run.ratchet.api.NodeTagFilter;

/** Shared SQL helpers for JDBC/JPA job-claim implementations. */
public final class JobClaimSqlSupport {

  private static final String SAFE_SQL_EXPRESSION_PATTERN = "[A-Za-z0-9_().,\\s+\\-*/]+";

  private JobClaimSqlSupport() {}

  /**
   * Builds a SQL fragment (empty string or starting with newline+AND) for tag affinity filtering.
   * Guards each list independently to avoid empty {@code IN ()}.
   */
  public static String buildTagFilterSql(NodeTagFilter filter, String tableAlias) {
    if (filter.isUnfiltered()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    if (!filter.requireTags().isEmpty()) {
      String placeholders = "?,".repeat(filter.requireTags().size());
      sb.append("\n  AND EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = ")
          .append(tableAlias)
          .append(".job_id AND t.tag IN (")
          .append(placeholders, 0, placeholders.length() - 1)
          .append("))");
    }
    if (!filter.excludeTags().isEmpty()) {
      String placeholders = "?,".repeat(filter.excludeTags().size());
      sb.append("\n  AND NOT EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = ")
          .append(tableAlias)
          .append(".job_id AND t.tag IN (")
          .append(placeholders, 0, placeholders.length() - 1)
          .append("))");
    }
    return sb.toString();
  }

  public static int bindTagFilter(Query query, NodeTagFilter filter, int startParam) {
    int p = startParam;
    for (String tag : filter.requireTags()) {
      query.setParameter(p++, tag);
    }
    for (String tag : filter.excludeTags()) {
      query.setParameter(p++, tag);
    }
    return p;
  }

  public static String buildBoostedOrderBy(
      String timeColumn, String overdueMinutesExpression, int boostInterval) {
    requireSafeSqlFragment(timeColumn, "timeColumn");
    requireSafeSqlFragment(overdueMinutesExpression, "overdueMinutesExpression");
    return boostInterval > 0
        ? "(priority + FLOOR(GREATEST(0, "
            + overdueMinutesExpression
            + ") / ?)) DESC, "
            + timeColumn
            + " ASC, job_id ASC"
        : "priority DESC, " + timeColumn + " ASC, job_id ASC";
  }

  private static void requireSafeSqlFragment(String value, String name) {
    if (value == null || !value.matches(SAFE_SQL_EXPRESSION_PATTERN)) {
      throw new IllegalArgumentException(name + " must be a store-defined SQL fragment");
    }
  }

  public static <T> List<T> reorderById(
      List<T> rows, List<UUID> orderedIds, Function<T, UUID> idExtractor) {
    Map<UUID, T> byId = new HashMap<>(rows.size());
    for (T row : rows) {
      byId.put(idExtractor.apply(row), row);
    }
    List<T> ordered = new ArrayList<>(rows.size());
    for (UUID id : orderedIds) {
      T row = byId.get(id);
      if (row != null) {
        ordered.add(row);
      }
    }
    return ordered;
  }
}

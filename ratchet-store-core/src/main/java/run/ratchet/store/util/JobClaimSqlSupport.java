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
package run.ratchet.store.util;

import jakarta.persistence.Query;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.spi.ExecutionTargetFilter;

/** Shared SQL helpers for JDBC/JPA job-claim implementations. */
public final class JobClaimSqlSupport {

  private static final String SAFE_SQL_EXPRESSION_PATTERN = "[A-Za-z0-9_().,\\s+\\-*/]+";
  private static final String SAFE_COLUMN_PREFIX_PATTERN = "|[A-Za-z_][A-Za-z0-9_]*\\.";

  private JobClaimSqlSupport() {}

  /**
   * Builds a SQL fragment (empty string or starting with newline+AND) for tag affinity filtering.
   * Guards each list independently to avoid empty {@code IN ()}. Assumes the joined table's
   * primary-key column is named {@code job_id}.
   */
  public static String buildTagFilterSql(NodeTagFilter filter, String tableAlias) {
    return buildTagFilterSql(filter, tableAlias, "job_id");
  }

  /**
   * Builds a tag-affinity SQL fragment against the join table {@code scheduler_job_tag} where the
   * joined table aliased by {@code tableAlias} carries its primary key in {@code idColumn}.
   *
   * <p>The recurring-master table uses {@code id} as its PK column, not {@code job_id}; callers
   * filtering recurring rows must pass {@code "id"}.
   */
  public static String buildTagFilterSql(NodeTagFilter filter, String tableAlias, String idColumn) {
    requireSafeSqlFragment(tableAlias, "tableAlias");
    requireSafeSqlFragment(idColumn, "idColumn");
    if (filter.isUnfiltered()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    if (!filter.requireTags().isEmpty()) {
      String placeholders = "?,".repeat(filter.requireTags().size());
      sb.append("\n  AND EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = ")
          .append(tableAlias)
          .append('.')
          .append(idColumn)
          .append(" AND t.tag IN (")
          .append(placeholders, 0, placeholders.length() - 1)
          .append("))");
    }
    if (!filter.excludeTags().isEmpty()) {
      String placeholders = "?,".repeat(filter.excludeTags().size());
      sb.append("\n  AND NOT EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = ")
          .append(tableAlias)
          .append('.')
          .append(idColumn)
          .append(" AND t.tag IN (")
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

  /**
   * Builds a SQL fragment (empty string or starting with newline+AND) for execution-target claim
   * filtering.
   */
  public static String buildExecutionTargetFilterSql(
      ExecutionTargetFilter filter, String columnName) {
    requireSafeSqlFragment(columnName, "columnName");
    if (filter == null || filter.isAny()) {
      return "";
    }
    if (filter.matchesNothing()) {
      return "\n  AND 1 = 0";
    }
    if (filter.isExclusion()) {
      String excludedSql = "";
      if (!filter.excludedTargets().isEmpty()) {
        String placeholders = "?,".repeat(filter.excludedTargets().size());
        excludedSql =
            columnName + " NOT IN (" + placeholders.substring(0, placeholders.length() - 1) + ")";
      }
      if (filter.includeNull() && excludedSql.isEmpty()) {
        return "";
      }
      if (filter.includeNull()) {
        return "\n  AND (" + excludedSql + " OR " + columnName + " IS NULL)";
      }
      if (excludedSql.isEmpty()) {
        return "\n  AND " + columnName + " IS NOT NULL";
      }
      return "\n  AND " + excludedSql;
    }
    String explicitSql = "";
    if (!filter.explicitTargets().isEmpty()) {
      String placeholders = "?,".repeat(filter.explicitTargets().size());
      explicitSql =
          columnName + " IN (" + placeholders.substring(0, placeholders.length() - 1) + ")";
    }
    if (filter.includeNull() && !explicitSql.isEmpty()) {
      return "\n  AND (" + explicitSql + " OR " + columnName + " IS NULL)";
    }
    if (filter.includeNull()) {
      return "\n  AND " + columnName + " IS NULL";
    }
    return "\n  AND " + explicitSql;
  }

  public static int bindExecutionTargetFilter(
      Query query, ExecutionTargetFilter filter, int startParam) {
    int p = startParam;
    if (filter != null && !filter.isAny()) {
      List<String> targets =
          filter.isExclusion() ? filter.excludedTargets() : filter.explicitTargets();
      for (String target : targets) {
        query.setParameter(p++, target);
      }
    }
    return p;
  }

  public static String buildBoostedOrderBy(
      String timeColumn, String overdueMinutesExpression, int boostInterval) {
    return buildBoostedOrderBy(timeColumn, overdueMinutesExpression, boostInterval, "");
  }

  /**
   * Builds the effective-priority ORDER BY, qualifying the {@code priority} and {@code job_id}
   * columns with {@code columnPrefix} (for example {@code "q."} when the query joins the queue
   * table under an alias, or {@code ""} for an unqualified single-table select).
   */
  public static String buildBoostedOrderBy(
      String timeColumn, String overdueMinutesExpression, int boostInterval, String columnPrefix) {
    requireSafeSqlFragment(timeColumn, "timeColumn");
    requireSafeSqlFragment(overdueMinutesExpression, "overdueMinutesExpression");
    requireSafeColumnPrefix(columnPrefix);
    return boostInterval > 0
        ? "("
            + columnPrefix
            + "priority + FLOOR(GREATEST(0, "
            + overdueMinutesExpression
            + ") / ?)) DESC, "
            + timeColumn
            + " ASC, "
            + columnPrefix
            + "job_id ASC"
        : columnPrefix + "priority DESC, " + timeColumn + " ASC, " + columnPrefix + "job_id ASC";
  }

  private static void requireSafeSqlFragment(String value, String name) {
    if (value == null || !value.matches(SAFE_SQL_EXPRESSION_PATTERN)) {
      throw new IllegalArgumentException(name + " must be a store-defined SQL fragment");
    }
  }

  private static void requireSafeColumnPrefix(String value) {
    if (value == null || !value.matches(SAFE_COLUMN_PREFIX_PATTERN)) {
      throw new IllegalArgumentException("columnPrefix must be empty or a store-defined alias");
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

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
package run.ratchet.store.query;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import run.ratchet.api.JobFilter;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobQuerySortField;
import run.ratchet.api.JobType;
import run.ratchet.store.spi.RecurringJobDefinition;

/** Shared, parameterized recurring-master predicates and pagination for SQL backends. */
public final class RecurringSqlQuery {
  public enum Dialect {
    POSTGRESQL,
    MYSQL,
    ORACLE,
    SQLSERVER
  }

  private RecurringSqlQuery() {}

  @SuppressWarnings("unchecked")
  public static List<RecurringJobDefinition> search(
      EntityManager em,
      String columns,
      JobFilter filter,
      int limit,
      int offset,
      Dialect dialect,
      Function<UUID, Object> uuid,
      Function<Object[], RecurringJobDefinition> hydrate) {
    if (limit < 1 || offset < 0) throw new IllegalArgumentException("Invalid page bounds");
    Parts parts = parts(filter, dialect, true);
    String direction = filter.sortAscending() ? " ASC" : " DESC";
    Query query =
        bind(
            em.createNativeQuery(
                "SELECT "
                    + columns
                    + " FROM scheduler_recurring_job r WHERE "
                    + parts.where
                    + " ORDER BY "
                    + sort(filter, dialect)
                    + direction
                    + ", r.id"
                    + direction),
            parts,
            uuid);
    query.setFirstResult(parts.cursor ? 0 : offset);
    query.setMaxResults(limit);
    List<Object[]> rows = query.getResultList();
    return rows.stream().map(hydrate).toList();
  }

  public static long count(
      EntityManager em, JobFilter filter, Dialect dialect, Function<UUID, Object> uuid) {
    Parts parts = parts(filter, dialect, false);
    return ((Number)
            bind(
                    em.createNativeQuery(
                        "SELECT COUNT(*) FROM scheduler_recurring_job r WHERE " + parts.where),
                    parts,
                    uuid)
                .getSingleResult())
        .longValue();
  }

  private static Query bind(Query query, Parts parts, Function<UUID, Object> uuid) {
    for (int i = 0; i < parts.values.size(); i++) {
      Object value = parts.values.get(i);
      if (value instanceof UUID id) value = uuid.apply(id);
      if (value instanceof Instant instant) value = Timestamp.from(instant);
      query.setParameter(i + 1, value);
    }
    return query;
  }

  private static Parts parts(JobFilter f, Dialect dialect, boolean seek) {
    Parts p = new Parts();
    if ((f.types() != null && !f.types().isEmpty() && !f.types().contains(JobType.RECURRING))
        || f.idempotencyKey() != null
        || f.pickedBy() != null
        || f.traceCorrelationId() != null
        || f.parentJobId() != null) p.and("1 = 0");
    p.in(
        status(dialect),
        f.statuses() == null ? null : f.statuses().stream().map(Enum::name).toList());
    p.in(
        "r.priority",
        f.priorities() == null
            ? null
            : f.priorities().stream().map(JobPriority::persistedCode).toList());
    p.eq("r.business_key", f.businessKey());
    p.eq("r.resource_name", f.resourceName());
    p.eq("r.caller_principal", f.callerPrincipal());
    String target =
        switch (dialect) {
          case POSTGRESQL -> "r.payload->>'target'";
          case MYSQL -> "JSON_UNQUOTE(JSON_EXTRACT(r.payload, '$.target'))";
          case ORACLE, SQLSERVER -> "JSON_VALUE(r.payload, '$.target')";
        };
    p.eq(target, f.targetClass());
    p.bound("r.created_at", ">=", f.createdAfter());
    p.bound("r.created_at", "<=", f.createdBefore());
    p.bound("r.next_fire", ">=", f.scheduledAfter());
    p.bound("r.next_fire", "<=", f.scheduledBefore());
    p.bound("r.created_at", ">=", f.updatedAfter());
    if (f.tags() != null && !f.tags().isEmpty()) {
      p.and(
          "EXISTS (SELECT 1 FROM scheduler_job_tag t WHERE t.job_id = r.id AND t.tag IN ("
              + placeholders(f.tags().size())
              + "))");
      p.values.addAll(f.tags());
    }
    if (f.propertyFilters() != null)
      f.propertyFilters()
          .forEach(
              (key, values) -> {
                if (values == null || values.isEmpty()) {
                  p.and("1 = 0");
                  return;
                }
                p.and(
                    "EXISTS (SELECT 1 FROM scheduler_job_properties x WHERE x.job_id = r.id"
                        + " AND x.property_key = ? AND x.value IN ("
                        + placeholders(values.size())
                        + "))");
                p.values.add(key);
                p.values.addAll(values);
              });
    if (seek && f.cursor() != null && !f.cursor().isBlank()) {
      JobQueryCursor cursor = JobQueryCursor.decode(f.cursor());
      if (cursor.matchesFilterSort(f)) {
        String expression = sort(f, dialect);
        String op = f.sortAscending() ? ">" : "<";
        Object value =
            switch (cursor.sortField()) {
              case CREATED_AT, SCHEDULED_TIME, UPDATED_AT -> Instant.parse(cursor.sortValue());
              case PRIORITY -> Integer.valueOf(cursor.sortValue());
              case STATUS -> cursor.sortValue();
            };
        p.and(
            "(" + expression + " " + op + " ? OR (" + expression + " = ? AND r.id " + op + " ?))");
        p.values.add(value);
        p.values.add(value);
        p.values.add(cursor.jobId());
        p.cursor = true;
      }
    }
    return p;
  }

  private static String status(Dialect dialect) {
    return "CASE WHEN r.is_paused = "
        + ((dialect == Dialect.POSTGRESQL || dialect == Dialect.ORACLE) ? "TRUE" : "1")
        + " THEN 'PAUSED' ELSE 'PENDING' END";
  }

  private static String sort(JobFilter f, Dialect dialect) {
    JobQuerySortField field = f.sortField() == null ? JobQuerySortField.CREATED_AT : f.sortField();
    return switch (field) {
      case CREATED_AT, UPDATED_AT -> "r.created_at";
      case SCHEDULED_TIME -> "r.next_fire";
      case PRIORITY -> "r.priority";
      case STATUS -> status(dialect);
    };
  }

  private static String placeholders(int n) {
    return String.join(",", java.util.Collections.nCopies(n, "?"));
  }

  private static final class Parts {
    private final StringBuilder where = new StringBuilder("1 = 1");
    private final List<Object> values = new ArrayList<>();
    private boolean cursor;

    void and(String sql) {
      where.append(" AND ").append(sql);
    }

    void eq(String column, Object value) {
      if (value != null) {
        and(column + " = ?");
        values.add(value);
      }
    }

    void bound(String column, String op, Instant value) {
      if (value != null) {
        and(column + " " + op + " ?");
        values.add(value);
      }
    }

    void in(String column, Collection<?> items) {
      if (items != null && !items.isEmpty()) {
        and(column + " IN (" + placeholders(items.size()) + ")");
        values.addAll(items);
      }
    }
  }
}

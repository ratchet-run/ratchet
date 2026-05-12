package run.ratchet.store.util;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Shared SQL helpers for archive query implementations. */
public final class ArchiveQuerySupport {

  private static final String ARCHIVE_COLUMNS_PATTERN = "[A-Za-z0-9_.,\\s]+";

  private ArchiveQuerySupport() {}

  /**
   * Builds an archive lookup query.
   *
   * <p>{@code archiveColumns} must be a store-module column list, not caller input. The helper
   * accepts only simple identifier lists because SQL select-list expressions cannot be bound as
   * parameters.
   */
  public static ArchiveSearchQuery buildFindArchivedJobsQuery(
      String archiveColumns,
      String targetClass,
      String businessKey,
      Instant from,
      Instant to,
      int limit) {
    if (archiveColumns == null || !archiveColumns.matches(ARCHIVE_COLUMNS_PATTERN)) {
      throw new IllegalArgumentException("archiveColumns must be a simple column list");
    }
    StringBuilder sql =
        new StringBuilder("SELECT " + archiveColumns + " FROM scheduler_job_archive WHERE 1=1");
    List<Object> parameters = new ArrayList<>();
    if (targetClass != null) {
      sql.append(" AND target_class = ?");
      parameters.add(targetClass);
    }
    if (businessKey != null) {
      sql.append(" AND business_key = ?");
      parameters.add(businessKey);
    }
    if (from != null) {
      sql.append(" AND archived_at >= ?");
      parameters.add(Timestamp.from(from));
    }
    if (to != null) {
      sql.append(" AND archived_at <= ?");
      parameters.add(Timestamp.from(to));
    }
    sql.append(" ORDER BY archived_at DESC LIMIT ?");
    parameters.add(limit);
    return new ArchiveSearchQuery(sql.toString(), List.copyOf(parameters));
  }

  public static void bindParameters(Query query, ArchiveSearchQuery searchQuery) {
    int parameter = 1;
    for (Object value : searchQuery.parameters()) {
      query.setParameter(parameter++, value);
    }
  }

  public record ArchiveSearchQuery(String sql, List<Object> parameters) {}
}

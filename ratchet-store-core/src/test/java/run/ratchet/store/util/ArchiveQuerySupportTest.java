package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import jakarta.persistence.Query;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class ArchiveQuerySupportTest {

  @Test
  void buildFindArchivedJobsQueryPreservesSqlAndParameterOrder() {
    Instant from = Instant.parse("2026-05-01T00:00:00Z");
    Instant to = Instant.parse("2026-05-02T00:00:00Z");

    var searchQuery =
        ArchiveQuerySupport.buildFindArchivedJobsQuery(
            "archive_id, archived_at", "com.example.Job", "business-1", from, to, 25);

    assertEquals(
        "SELECT archive_id, archived_at FROM scheduler_job_archive WHERE 1=1"
            + " AND target_class = ?"
            + " AND business_key = ?"
            + " AND archived_at >= ?"
            + " AND archived_at <= ?"
            + " ORDER BY archived_at DESC LIMIT ?",
        searchQuery.sql());
    assertEquals(
        List.of("com.example.Job", "business-1", Timestamp.from(from), Timestamp.from(to), 25),
        searchQuery.parameters());
  }

  @Test
  void buildFindArchivedJobsQueryOmitsNullFilters() {
    var searchQuery =
        ArchiveQuerySupport.buildFindArchivedJobsQuery("archive_id", null, null, null, null, 10);

    assertEquals(
        "SELECT archive_id FROM scheduler_job_archive WHERE 1=1"
            + " ORDER BY archived_at DESC LIMIT ?",
        searchQuery.sql());
    assertEquals(List.of(10), searchQuery.parameters());
  }

  @Test
  void bindParametersUsesOneBasedSqlOrder() {
    var searchQuery =
        new ArchiveQuerySupport.ArchiveSearchQuery("SELECT 1", List.of("target", "business", 50));
    Query query = mock(Query.class);

    ArchiveQuerySupport.bindParameters(query, searchQuery);

    InOrder order = inOrder(query);
    order.verify(query).setParameter(1, "target");
    order.verify(query).setParameter(2, "business");
    order.verify(query).setParameter(3, 50);
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  void buildFindArchivedJobsQueryRejectsNonColumnFragments() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArchiveQuerySupport.buildFindArchivedJobsQuery(
                "archive_id FROM scheduler_job_archive; DELETE FROM scheduler_job_archive",
                null,
                null,
                null,
                null,
                10));
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

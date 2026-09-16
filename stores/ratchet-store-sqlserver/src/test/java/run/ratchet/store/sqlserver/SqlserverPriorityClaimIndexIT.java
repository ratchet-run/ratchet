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
package run.ratchet.store.sqlserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class SqlserverPriorityClaimIndexIT {
  @Test
  void migrationAddsOrderedFilteredIndexAndCanRepeat() throws Exception {
    var fixture = new SqlserverTestFixture();
    String migration;
    try (var input =
        getClass().getResourceAsStream("/ddl/migrations/V008__priority_ordered_claim_index.sql")) {
      migration = new String(Objects.requireNonNull(input).readAllBytes(), StandardCharsets.UTF_8);
    }
    try (var connection = fixture.openConnection();
        var statement = connection.createStatement()) {
      statement.execute("DROP INDEX idx_claim_pending_priority ON scheduler_job_queue");
      statement.execute(migration);
      statement.execute(migration);
      try (var rows =
          statement.executeQuery(
              """
          SELECT c.name, ic.is_descending_key, i.has_filter, i.filter_definition
          FROM sys.indexes i
          JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
          JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
          WHERE i.object_id = OBJECT_ID('scheduler_job_queue')
            AND i.name = 'idx_claim_pending_priority' AND ic.key_ordinal > 0
          ORDER BY ic.key_ordinal
          """)) {
        var keys = new ArrayList<String>();
        while (rows.next()) {
          keys.add(rows.getString(1) + ":" + rows.getInt(2));
          assertTrue(rows.getBoolean(3));
          assertTrue(rows.getString(4).contains("PENDING"));
        }
        assertEquals(List.of("job_type:0", "priority:1", "scheduled_time:0", "job_id:0"), keys);
      }
      try (var rows =
          statement.executeQuery(
              "SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('scheduler_job_queue') AND name = 'idx_claim_executable'")) {
        assertTrue(rows.next());
        assertEquals(1, rows.getInt(1));
      }
    }
  }
}

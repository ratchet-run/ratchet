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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the session-isolation probe driving {@link
 * SqlserverJobStoreImpl#checkIsolationLevel()} is valid T-SQL that actually returns the level.
 *
 * <p>Regression guard: the probe was originally copied verbatim from the PostgreSQL store ({@code
 * SHOW transaction_isolation}), which is invalid on SQL Server. Because {@code IsolationCheck}
 * swallows a throwing probe and skips the check, the broken query silently disabled the guard on
 * every startup without failing any test. Running the probe directly here proves it is live.
 */
class SqlserverIsolationProbeTest {

  private final SqlserverTestFixture fixture = new SqlserverTestFixture();

  @Test
  void isolationProbeReturnsReadCommitted() throws Exception {
    try (Connection conn = fixture.openConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(SqlserverJobStoreImpl.ISOLATION_PROBE_SQL)) {
      assertTrue(rs.next(), "isolation probe returned no row");
      String level = rs.getString(1);
      assertNotNull(level, "isolation probe returned NULL — the CASE did not match a known level");
      assertEquals(
          "read committed",
          level.toLowerCase(Locale.ROOT),
          "default SQL Server session isolation should be READ COMMITTED (level 2)");
    }
  }
}

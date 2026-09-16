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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.store.ExplainPlanTestSupport;

class SqlserverExplainPlanCaptureIT {

  private static final SqlserverTestFixture FIXTURE = new SqlserverTestFixture();

  /**
   * Captures the estimated plan for the optimized executable-claim SELECT via {@code SET
   * SHOWPLAN_XML ON} (which returns the plan XML without executing the statement). The filtered
   * index is forced with a hint so the test asserts it remains <em>usable</em> for the claim shape
   * — SQL Server would reject the hint outright if the filtered index could no longer serve the
   * query. This mirrors the PostgreSQL test's "disable seqscan to verify the index remains usable"
   * intent.
   */
  private static String showplanXml(Statement statement) throws SQLException {
    return showplanXml(statement, true);
  }

  private static String showplanXml(Statement statement, boolean forceDueIndex)
      throws SQLException {
    String sql =
        """
        SELECT job_id, status, job_type, priority, scheduled_time, version, timeout_sec,
               picked_by, picked_at, business_key, attempts, max_retries, execution_target
        FROM scheduler_job_queue WITH (INDEX(idx_claim_executable), UPDLOCK, READPAST, ROWLOCK)
        WHERE status = 'PENDING'
          AND scheduled_time <= SYSUTCDATETIME()
          AND job_type = 'SINGLE'
        ORDER BY priority DESC, scheduled_time ASC, job_id ASC
        OFFSET 0 ROWS FETCH NEXT 50 ROWS ONLY
        """;
    if (!forceDueIndex) {
      sql = sql.replace("INDEX(idx_claim_executable), ", "");
    }
    statement.execute("SET SHOWPLAN_XML ON");
    try (ResultSet rs = statement.executeQuery(sql)) {
      assertTrue(rs.next(), "SHOWPLAN_XML should return one plan row");
      return rs.getString(1);
    } finally {
      statement.execute("SET SHOWPLAN_XML OFF");
    }
  }

  @BeforeEach
  void clean() {
    FIXTURE.cleanupStore();
  }

  @Test
  void optimizedExecutableClaimPlan_usesClaimCoveringIndex() throws Exception {
    ExplainPlanTestSupport.seedPendingJobs(FIXTURE);
    try (Connection conn = ExplainPlanTestSupport.connection(FIXTURE);
        Statement statement = conn.createStatement()) {
      String plan = showplanXml(statement);
      ExplainPlanTestSupport.writePlan("target/explain-plans/sqlserver-optimized-claim.xml", plan);

      assertTrue(plan.contains("scheduler_job_queue"), plan);
      assertTrue(plan.contains("idx_claim_executable"), plan);
    }
  }

  @Test
  void unboostedBacklogUsesPriorityIndexWithoutSort() throws Exception {
    try (Connection conn = ExplainPlanTestSupport.connection(FIXTURE);
        Statement statement = conn.createStatement()) {
      statement.executeUpdate(
          """
          WITH numbers AS (
            SELECT TOP (100000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
            FROM sys.all_objects a CROSS JOIN sys.all_objects b
          )
          INSERT INTO scheduler_job (job_id, job_type, priority, payload, idempotency_key, created_at)
          SELECT CONVERT(binary(16), NEWID()), 'SINGLE', n % 5, N'{}',
                 CONVERT(varchar(36), NEWID()), SYSUTCDATETIME()
          FROM numbers
          """);
      statement.executeUpdate(
          """
          INSERT INTO scheduler_job_queue (job_id, job_type, priority, scheduled_time, updated_at)
          SELECT job_id, job_type, priority, DATEADD(hour, -1, SYSUTCDATETIME()), SYSUTCDATETIME()
          FROM scheduler_job
          """);
      statement.execute("UPDATE STATISTICS scheduler_job_queue WITH FULLSCAN");
      String plan = showplanXml(statement, false);
      ExplainPlanTestSupport.writePlan("target/explain-plans/sqlserver-priority-claim.xml", plan);
      assertTrue(plan.contains("idx_claim_pending_priority"), plan);
      assertFalse(plan.contains("PhysicalOp=\"Sort\""), plan);
    }
  }
}

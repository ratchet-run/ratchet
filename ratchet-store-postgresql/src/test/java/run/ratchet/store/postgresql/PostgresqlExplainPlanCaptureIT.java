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
package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.store.ExplainPlanTestSupport;

class PostgresqlExplainPlanCaptureIT {

  private static final PostgresqlTestFixture FIXTURE = new PostgresqlTestFixture();

  private static String explainJson(Statement statement) throws SQLException {
    String sql =
        """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        WITH picked AS (
          SELECT job_id
          FROM scheduler_job_queue
          WHERE status = 'PENDING'
            AND scheduled_time <= statement_timestamp()
            AND job_type = 'SINGLE'
          ORDER BY
            (priority + FLOOR(GREATEST(0, EXTRACT(EPOCH FROM (statement_timestamp() - scheduled_time))) / (60.0 * 15))) DESC,
            scheduled_time ASC,
            job_id ASC
          FOR UPDATE SKIP LOCKED
          LIMIT 50
        )
        UPDATE scheduler_job_queue AS q
        SET status = 'RUNNING',
            picked_by = 'explain-node',
            picked_at = statement_timestamp(),
            updated_at = statement_timestamp(),
            version = version + 1
        FROM picked
        WHERE q.job_id = picked.job_id
        RETURNING q.job_id
        """;
    try (ResultSet rs = statement.executeQuery(sql)) {
      assertTrue(rs.next(), "EXPLAIN should return one JSON plan row");
      return rs.getString(1);
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
      conn.setAutoCommit(false);
      statement.execute("ANALYZE scheduler_job_queue");
      // The fixture table is intentionally small, so PostgreSQL may prefer a sequential scan on
      // cost alone. Disable seqscan locally to verify the intended claim index remains usable.
      statement.execute("SET LOCAL enable_seqscan = off");
      String plan = explainJson(statement);
      ExplainPlanTestSupport.writePlan(
          "target/explain-plans/postgresql-optimized-claim.json", plan);
      conn.rollback();

      assertTrue(plan.contains("\"Relation Name\": \"scheduler_job_queue\""), plan);
      assertTrue(plan.contains("\"Index Name\": \"idx_claim_executable\""), plan);
    }
  }
}

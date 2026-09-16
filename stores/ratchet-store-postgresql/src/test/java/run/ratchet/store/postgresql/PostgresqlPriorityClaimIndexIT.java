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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import run.ratchet.tck.store.ExplainPlanTestSupport;

/** Real planner regression: no disabled scan types or forced index hints. */
class PostgresqlPriorityClaimIndexIT {
  private static final PostgresqlTestFixture FIXTURE = new PostgresqlTestFixture();

  private static String claimSql() {
    return "SELECT "
        + PostgresqlJobClaimOperations.claimSelectClause()
        + " FROM scheduler_job_queue WHERE status = 'PENDING'"
        + " AND job_type = 'SINGLE' AND scheduled_time <= statement_timestamp()"
        + " ORDER BY priority DESC, scheduled_time ASC, job_id ASC LIMIT 50"
        + " FOR UPDATE SKIP LOCKED";
  }

  private static void seed(Statement statement, boolean mostlyFuture) throws Exception {
    statement.execute(
        """
        INSERT INTO scheduler_job (job_id, job_type, priority, payload, idempotency_key)
        SELECT md5('claim-plan-' || n)::uuid, 'SINGLE', n % 5, '{}', md5('claim-plan-' || n)
        FROM generate_series(1, 100000) AS n
        """);
    statement.execute(
        """
        INSERT INTO scheduler_job_queue (job_id, job_type, priority, scheduled_time)
        SELECT job_id, job_type, priority,
               statement_timestamp() - interval '1 hour'
        FROM scheduler_job
        """);
    if (mostlyFuture) {
      statement.execute(
          """
          UPDATE scheduler_job_queue SET scheduled_time = statement_timestamp() + interval '1 day'
          WHERE job_id NOT IN (SELECT job_id FROM scheduler_job_queue ORDER BY job_id LIMIT 10)
          """);
    }
    statement.execute("ANALYZE scheduler_job_queue");
    statement.execute("ANALYZE scheduler_job");
  }

  private static String plan(boolean mostlyFuture) throws Exception {
    FIXTURE.cleanupStore();
    try (Connection connection = FIXTURE.openConnection();
        Statement statement = connection.createStatement()) {
      connection.setAutoCommit(false);
      try {
        seed(statement, mostlyFuture);
        try (var rows =
            statement.executeQuery("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + claimSql())) {
          assertTrue(rows.next());
          String result = rows.getString(1);
          ExplainPlanTestSupport.writePlan(
              "target/explain-plans/postgresql-priority-"
                  + (mostlyFuture ? "future" : "backlog")
                  + ".json",
              result);
          return result;
        }
      } finally {
        connection.rollback();
      }
    }
  }

  @Test
  void dueBacklogUsesPriorityIndexWithoutSorting() throws Exception {
    String plan = plan(false);
    assertTrue(plan.contains("\"Index Name\": \"idx_claim_pending_priority\""), plan);
    assertFalse(plan.contains("\"Node Type\": \"Sort\""), plan);
    assertFalse(plan.contains("\"Node Type\": \"Seq Scan\""), plan);
  }

  @Test
  void futureHeavyQueueRetainsSelectiveDueTimeIndex() throws Exception {
    String plan = plan(true);
    assertTrue(plan.contains("\"Index Name\": \"idx_claim_executable\""), plan);
    assertFalse(plan.contains("\"Node Type\": \"Seq Scan\""), plan);
  }
}

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
package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.tck.store.ExplainPlanTestSupport;

class MysqlExplainPlanCaptureIT {

  private static final MysqlTestFixture FIXTURE = new MysqlTestFixture();

  private static String explainJson(Statement statement) throws SQLException {
    String sql =
        "EXPLAIN FORMAT=JSON "
            + MysqlJobClaimOperations.buildClaimSql(
                    "job_id", "job_type = 'SINGLE'", "", "", "scheduled_time", 15)
                .replaceFirst("\\?", "15")
                .replaceFirst("\\?", "50");
    try (ResultSet rs = statement.executeQuery(sql)) {
      assertTrue(rs.next(), "EXPLAIN FORMAT=JSON should return one row");
      return rs.getString(1);
    }
  }

  private static JsonObject schedulerJobQueueTable(String plan) {
    try (JsonReader reader = Json.createReader(new StringReader(plan))) {
      JsonObject root = reader.readObject();
      return findTable(root, "scheduler_job_queue")
          .orElseThrow(() -> new AssertionError("scheduler_job_queue table not found: " + plan));
    }
  }

  private static Optional<JsonObject> findTable(JsonValue value, String tableName) {
    if (value instanceof JsonObject object) {
      JsonObject table = object.getJsonObject("table");
      if (table != null && tableName.equals(table.getString("table_name", null))) {
        return Optional.of(table);
      }
      for (JsonValue child : object.values()) {
        Optional<JsonObject> match = findTable(child, tableName);
        if (match.isPresent()) {
          return match;
        }
      }
    } else if (value instanceof JsonArray array) {
      for (JsonValue child : array) {
        Optional<JsonObject> match = findTable(child, tableName);
        if (match.isPresent()) {
          return match;
        }
      }
    }
    return Optional.empty();
  }

  private static JobEntity pendingJob(
      UUID id, JobExecutionType jobType, JobPriority priority, Instant scheduledTime) {
    JobEntity job = FIXTURE.newPendingJob();
    job.setId(id);
    job.setJobType(jobType);
    job.setPriority(priority);
    job.setScheduledTime(scheduledTime);
    return job;
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
      statement.execute("ANALYZE TABLE scheduler_job_queue");
      String plan = explainJson(statement);
      ExplainPlanTestSupport.writePlan("target/explain-plans/mysql-optimized-claim.json", plan);
      JsonObject table = schedulerJobQueueTable(plan);

      assertEquals(
          "scheduler_job_queue",
          table.getString("table_name", null),
          "claim plan should target scheduler_job_queue: " + plan);
      assertTrue(
          java.util.Set.of("idx_claim_executable", "idx_claim_pending_priority")
              .contains(table.getString("key", null)),
          "candidate lookup should use a claim index: " + plan);
      assertNotEquals(
          "ALL",
          table.getString("access_type", null),
          "claim plan should not full-scan scheduler_job_queue: " + plan);
    }
  }

  @Test
  void unboostedBacklogUsesPriorityIndexWithoutFilesort() throws Exception {
    ExplainPlanTestSupport.seedPendingJobs(FIXTURE, 6000);
    try (Connection conn = ExplainPlanTestSupport.connection(FIXTURE);
        Statement statement = conn.createStatement()) {
      statement.execute("ANALYZE TABLE scheduler_job_queue");
      String sql =
          MysqlJobClaimOperations.buildClaimSql(
                  "job_id", "job_type = 'SINGLE'", "", "", "scheduled_time", 0)
              .replace("LIMIT ?", "LIMIT 50");
      try (ResultSet rs = statement.executeQuery("EXPLAIN FORMAT=JSON " + sql)) {
        assertTrue(rs.next());
        String plan = rs.getString(1);
        ExplainPlanTestSupport.writePlan("target/explain-plans/mysql-priority-claim.json", plan);
        assertEquals(
            "idx_claim_pending_priority", schedulerJobQueueTable(plan).getString("key"), plan);
        assertFalse(plan.contains("\"using_filesort\": true"), plan);
      }
    }
  }

  @Test
  void futureHeavyQueueCanStillUseDueTimeIndex() throws Exception {
    // Start with future jobs, rather than moving an existing due backlog into the future.
    // Delete-marked due index entries can distort InnoDB range estimates until purge catches up.
    try (Connection conn = ExplainPlanTestSupport.connection(FIXTURE);
        Statement statement = conn.createStatement()) {
      statement.execute("TRUNCATE TABLE scheduler_job_queue");
    }
    Instant now = Instant.now();
    var jobs = new java.util.ArrayList<JobEntity>();
    for (int i = 0; i < 6000; i++) {
      jobs.add(
          pendingJob(
              UuidV7Factory.create(),
              JobExecutionType.SINGLE,
              JobPriority.values()[i % JobPriority.values().length],
              i < 10 ? now.minusSeconds(60) : now.plus(Duration.ofDays(1))));
    }
    FIXTURE.store().bulkInsert(jobs);
    try (Connection conn = ExplainPlanTestSupport.connection(FIXTURE);
        Statement statement = conn.createStatement()) {
      statement.execute("ANALYZE TABLE scheduler_job_queue");
      String sql =
          MysqlJobClaimOperations.buildClaimSql(
                  "job_id", "job_type = 'SINGLE'", "", "", "scheduled_time", 0)
              .replace("LIMIT ?", "LIMIT 50");
      try (ResultSet rs = statement.executeQuery("EXPLAIN FORMAT=JSON " + sql)) {
        assertTrue(rs.next());
        String plan = rs.getString(1);
        ExplainPlanTestSupport.writePlan("target/explain-plans/mysql-future-claim.json", plan);
        assertEquals("idx_claim_executable", schedulerJobQueueTable(plan).getString("key"), plan);
      }
    }
  }

  @Test
  void optimizedExecutableClaim_excludesFutureAndOtherJobTypes() {
    Instant now = Instant.now();
    UUID dueSingleId = UuidV7Factory.create();
    UUID futureSingleId = UuidV7Factory.create();
    UUID dueBatchChildId = UuidV7Factory.create();
    FIXTURE
        .store()
        .bulkInsert(
            List.of(
                pendingJob(
                    dueSingleId,
                    JobExecutionType.SINGLE,
                    JobPriority.LOW,
                    now.minus(Duration.ofMinutes(1))),
                pendingJob(
                    futureSingleId,
                    JobExecutionType.SINGLE,
                    JobPriority.CRITICAL,
                    now.plus(Duration.ofDays(1))),
                pendingJob(
                    dueBatchChildId,
                    JobExecutionType.BATCH_CHILD,
                    JobPriority.CRITICAL,
                    now.minus(Duration.ofMinutes(1)))));

    List<UUID> claimedIds =
        FIXTURE
            .store()
            .claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "mysql-explain-it")
            .stream()
            .map(JobClaimDto::id)
            .toList();

    assertEquals(List.of(dueSingleId), claimedIds);
    assertFalse(
        FIXTURE.store().claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "node-2").stream()
            .map(JobClaimDto::id)
            .toList()
            .contains(futureSingleId));
  }
}

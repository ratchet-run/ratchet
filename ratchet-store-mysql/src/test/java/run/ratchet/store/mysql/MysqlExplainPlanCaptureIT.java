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
        """
        EXPLAIN FORMAT=JSON
        SELECT job_id, status, job_type, priority, scheduled_time,
               version, timeout_sec, picked_by, picked_at, business_key,
               attempts, max_retries
        FROM scheduler_job_queue FORCE INDEX (idx_claim_executable)
        WHERE status = 'PENDING'
          AND scheduled_time <= NOW(3)
          AND job_type = 'SINGLE'
        ORDER BY
          (priority + FLOOR(GREATEST(0, TIMESTAMPDIFF(MINUTE, scheduled_time, NOW(3))) / 15)) DESC,
          scheduled_time ASC,
          job_id ASC
        LIMIT 50
        FOR UPDATE SKIP LOCKED
        """;
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
      assertEquals(
          "idx_claim_executable",
          table.getString("key", null),
          "claim plan should use idx_claim_executable: " + plan);
      assertNotEquals(
          "ALL",
          table.getString("access_type", null),
          "claim plan should not full-scan scheduler_job_queue: " + plan);
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

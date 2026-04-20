package run.ratchet.store.mysql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.id.TsidFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MysqlExplainPlanCaptureIT {

  private static final int SEED_JOBS = 600;
  private static final MysqlTestFixture FIXTURE = new MysqlTestFixture();

  @BeforeEach
  void clean() {
    FIXTURE.cleanupStore();
  }

  @Test
  void optimizedExecutableClaimPlan_usesClaimCoveringIndex() throws Exception {
    seedPendingJobs();
    try (Connection conn = connection();
        Statement statement = conn.createStatement()) {
      statement.execute("ANALYZE TABLE scheduler_job_queue");
      String plan = explainJson(statement);
      writePlan("target/explain-plans/mysql-optimized-claim.json", plan);

      assertTrue(
          plan.contains("\"table_name\": \"scheduler_job_queue\""),
          "claim plan should target scheduler_job_queue: " + plan);
      assertTrue(
          plan.contains("\"key\": \"idx_claim_executable\""),
          "claim plan should use idx_claim_executable: " + plan);
      assertFalse(
          plan.contains("\"access_type\": \"ALL\""),
          "claim plan should not full-scan scheduler_job_queue: " + plan);
    }
  }

  private static void seedPendingJobs() {
    Instant now = Instant.now();
    JobPriority[] priorities = JobPriority.values();
    List<JobEntity> jobs = new ArrayList<>(SEED_JOBS);
    for (int i = 0; i < SEED_JOBS; i++) {
      JobEntity job = FIXTURE.newPendingJob();
      job.setId(TsidFactory.next());
      job.setJobType(i % 4 == 0 ? JobExecutionType.SINGLE : JobExecutionType.BATCH_CHILD);
      job.setPriority(priorities[i % priorities.length]);
      job.setScheduledTime(now.minus(Duration.ofMinutes(i % 180)));
      jobs.add(job);
    }
    FIXTURE.store().bulkInsert(jobs);
  }

  private static Connection connection() throws SQLException {
    return DriverManager.getConnection(
        FIXTURE.container().getJdbcUrl(),
        FIXTURE.container().getUsername(),
        FIXTURE.container().getPassword());
  }

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

  private static void writePlan(String path, String plan) throws Exception {
    Path output = Path.of(path);
    Files.createDirectories(output.getParent());
    Files.writeString(output, plan + System.lineSeparator());
  }
}

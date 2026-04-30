package run.ratchet.store.postgresql;

import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.id.UuidV7Factory;
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

class PostgresqlExplainPlanCaptureIT {

  private static final int SEED_JOBS = 600;
  private static final PostgresqlTestFixture FIXTURE = new PostgresqlTestFixture();

  private static void seedPendingJobs() {
    Instant now = Instant.now();
    JobPriority[] priorities = JobPriority.values();
    List<JobEntity> jobs = new ArrayList<>(SEED_JOBS);
    for (int i = 0; i < SEED_JOBS; i++) {
      JobEntity job = FIXTURE.newPendingJob();
      job.setId(UuidV7Factory.create());
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

  private static void writePlan(String path, String plan) throws Exception {
    Path output = Path.of(path);
    Files.createDirectories(output.getParent());
    Files.writeString(output, plan + System.lineSeparator());
  }

  @BeforeEach
  void clean() {
    FIXTURE.cleanupStore();
  }

  @Test
  void optimizedExecutableClaimPlan_usesClaimCoveringIndex() throws Exception {
    seedPendingJobs();
    try (Connection conn = connection();
        Statement statement = conn.createStatement()) {
      conn.setAutoCommit(false);
      statement.execute("ANALYZE scheduler_job_queue");
      // The fixture table is intentionally small, so PostgreSQL may prefer a sequential scan on
      // cost alone. Disable seqscan locally to verify the intended claim index remains usable.
      statement.execute("SET LOCAL enable_seqscan = off");
      String plan = explainJson(statement);
      writePlan("target/explain-plans/postgresql-optimized-claim.json", plan);
      conn.rollback();

      assertTrue(plan.contains("\"Relation Name\": \"scheduler_job_queue\""), plan);
      assertTrue(plan.contains("\"Index Name\": \"idx_claim_executable\""), plan);
    }
  }
}

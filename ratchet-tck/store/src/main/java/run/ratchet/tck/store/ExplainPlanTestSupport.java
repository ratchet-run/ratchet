package run.ratchet.tck.store;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import run.ratchet.api.JobPriority;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.id.UuidV7Factory;

/**
 * Shared setup for dialect-specific explain-plan integration tests.
 *
 * @apiNote <b>Internal.</b> This is a test-fixture utility consumed only by the SQL store modules'
 *     own dialect-specific {@code *ExplainPlanCaptureIT} tests, not part of any conformance
 *     contract. It is published as public solely so cross-module test sources (e.g. {@code
 *     ratchet-store-mysql/src/test}, {@code ratchet-store-postgresql/src/test}) can use it.
 *     External consumers MUST NOT depend on it; the API and seeded-data shape may change between
 *     releases without notice.
 */
public final class ExplainPlanTestSupport {

  public static final int DEFAULT_SEED_JOBS = 600;

  private ExplainPlanTestSupport() {}

  public static void seedPendingJobs(JobStoreContractFixture fixture) {
    seedPendingJobs(fixture, DEFAULT_SEED_JOBS);
  }

  public static void seedPendingJobs(JobStoreContractFixture fixture, int seedJobs) {
    Instant now = Instant.now();
    JobPriority[] priorities = JobPriority.values();
    List<JobEntity> jobs = new ArrayList<>(seedJobs);
    for (int i = 0; i < seedJobs; i++) {
      JobEntity job = fixture.newPendingJob();
      job.setId(UuidV7Factory.create());
      job.setJobType(i % 4 == 0 ? JobExecutionType.SINGLE : JobExecutionType.BATCH_CHILD);
      job.setPriority(priorities[i % priorities.length]);
      job.setScheduledTime(now.minus(Duration.ofMinutes(i % 180)));
      jobs.add(job);
    }
    fixture.store().bulkInsert(jobs);
  }

  public static Connection connection(JpaContainerFixture fixture) throws SQLException {
    return fixture.openConnection();
  }

  public static void writePlan(String path, String plan) throws Exception {
    Path output = Path.of(path);
    Files.createDirectories(output.getParent());
    Files.writeString(output, plan + System.lineSeparator());
  }
}

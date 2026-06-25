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
package run.ratchet.store.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobPriority;
import run.ratchet.store.dto.JobClaimDto;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.tck.store.ExplainPlanTestSupport;

/**
 * Captures the Oracle optimizer plan for the executable-claim candidate select (Phase A of the
 * two-phase claim) and asserts it rides the {@code idx_claim_executable} covering index rather than
 * full-scanning the hot queue table.
 *
 * <p>MySQL's equivalent uses {@code EXPLAIN FORMAT=JSON} with {@code FORCE INDEX}; the Oracle
 * analog is {@code EXPLAIN PLAN FOR} into {@code PLAN_TABLE}, rendered with {@code
 * DBMS_XPLAN.DISPLAY} (both granted to PUBLIC, so no extra privileges are needed). An {@code INDEX}
 * hint forces the covering index the same way {@code FORCE INDEX} does, so the assertion is a
 * structural regression guard: it fails if the index is dropped, renamed, or the claim predicate
 * stops matching it (Oracle silently ignores an un-honorable hint and falls back to a full scan).
 */
class OracleExplainPlanCaptureIT {

  private static final OracleTestFixture FIXTURE = new OracleTestFixture();

  // Phase A of OracleJobClaimOperations: the unlocked top-N candidate select, with the boosted
  // priority order-by spelled out (boost interval = 15 minutes). The INDEX hint forces the covering
  // index so the captured plan deterministically demonstrates the claim path can use it.
  private static final String CLAIM_CANDIDATE_SELECT =
      """
      SELECT /*+ INDEX(sjq idx_claim_executable) */
             job_id, status, job_type, priority, scheduled_time,
             version, timeout_sec, picked_by, picked_at, business_key,
             attempts, max_retries, execution_target
      FROM scheduler_job_queue sjq
      WHERE status = 'PENDING'
        AND scheduled_time <= CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP)
        AND job_type = 'SINGLE'
      ORDER BY (priority + FLOOR(GREATEST(0,
                 (CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE) - CAST(scheduled_time AS DATE)) * 1440)
                 / 15)) DESC,
               scheduled_time ASC,
               job_id ASC
      FETCH FIRST 50 ROWS ONLY""";

  private static String explainPlan(Statement statement) throws SQLException {
    statement.execute("DELETE FROM plan_table WHERE statement_id = 'ratchet_claim'");
    statement.execute(
        "EXPLAIN PLAN SET STATEMENT_ID = 'ratchet_claim' FOR " + CLAIM_CANDIDATE_SELECT);
    StringBuilder plan = new StringBuilder();
    try (ResultSet rs =
        statement.executeQuery(
            "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE',"
                + " 'ratchet_claim', 'ALL'))")) {
      while (rs.next()) {
        plan.append(rs.getString(1)).append(System.lineSeparator());
      }
    }
    return plan.toString();
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
      statement.execute("BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, 'SCHEDULER_JOB_QUEUE'); END;");
      String plan = explainPlan(statement);
      ExplainPlanTestSupport.writePlan("target/explain-plans/oracle-optimized-claim.txt", plan);

      assertTrue(
          plan.contains("IDX_CLAIM_EXECUTABLE"),
          () -> "claim plan should use idx_claim_executable:\n" + plan);
      assertFalse(
          plan.contains("TABLE ACCESS FULL"),
          () -> "claim plan should not full-scan scheduler_job_queue:\n" + plan);
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
            .claimNextBatchOptimized(JobExecutionType.SINGLE, 10, "oracle-explain-it")
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

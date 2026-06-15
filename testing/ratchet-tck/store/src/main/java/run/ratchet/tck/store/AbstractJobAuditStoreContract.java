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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobLogEntity;
import run.ratchet.store.spi.JobAuditStore;

/** Base contract tests for {@code JobAuditStore} (execution history and per-job logs). */
public abstract class AbstractJobAuditStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupAuditFixture() {
    cleanupStore();
  }

  @Test
  void saveAndFindExecutions_roundTrips() {
    var job = persist(newPendingJob());

    var exec = JobExecutionEntity.start(job.getId(), 1, "node-1");
    auditStore().saveExecution(exec);

    var executions =
        auditStore().findExecutionsByJobId(job.getId(), JobAuditStore.DEFAULT_PAGE_LIMIT, 0);

    assertEquals(1, executions.size(), "findExecutionsByJobId should return the saved execution");
    assertEquals(job.getId(), executions.get(0).getJobId());
  }

  @Test
  void findLatestExecution_returnsNewest() {
    var job = persist(newPendingJob());

    var first = JobExecutionEntity.start(job.getId(), 1, "node-1");
    auditStore().saveExecution(first);

    var second = JobExecutionEntity.start(job.getId(), 2, "node-1");
    auditStore().saveExecution(second);

    var latest = auditStore().findLatestExecution(job.getId());

    assertTrue(latest.isPresent(), "findLatestExecution should return a result");
    assertEquals(2, latest.get().getAttempt(), "Latest execution should be the second attempt");
  }

  @Test
  void countExecutionAttempts_returnsCorrectCount() {
    var job = persist(newPendingJob());

    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "node-1"));
    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 2, "node-1"));
    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 3, "node-1"));

    int count = auditStore().countExecutionAttempts(job.getId());

    assertEquals(3, count, "countExecutionAttempts should return 3 after saving 3 executions");
  }

  @Test
  void findExecutionsByJobId_returnsRequestedPage() {
    var job = persist(newPendingJob());

    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "node-1"));
    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 2, "node-1"));
    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 3, "node-1"));

    var executions = auditStore().findExecutionsByJobId(job.getId(), 2, 1);

    assertEquals(2, executions.size(), "paged execution lookup should return the requested window");
    assertEquals(2, executions.get(0).getAttempt(), "page should preserve attempt ordering");
    assertEquals(3, executions.get(1).getAttempt(), "page should preserve attempt ordering");
  }

  @Test
  void findExecutionsByJobId_zeroLimit_returnsEmptyPage() {
    var job = persist(newPendingJob());
    auditStore().saveExecution(JobExecutionEntity.start(job.getId(), 1, "node-1"));

    var executions = auditStore().findExecutionsByJobId(job.getId(), 0, 0);

    assertTrue(executions.isEmpty(), "limit=0 should return an empty execution page");
  }

  @Test
  void findExecutionsByJobId_unknownJob_returnsEmpty() {
    var executions =
        auditStore()
            .findExecutionsByJobId(
                new UUID(0L, Long.MAX_VALUE), JobAuditStore.DEFAULT_PAGE_LIMIT, 0);

    assertTrue(executions.isEmpty(), "findExecutionsByJobId for unknown job should return empty");
  }

  @Test
  void findLatestExecution_unknownJob_returnsEmpty() {
    var latest = auditStore().findLatestExecution(new UUID(0L, Long.MAX_VALUE));

    assertTrue(latest.isEmpty(), "findLatestExecution for unknown job should return empty");
  }

  @Test
  void countExecutionAttempts_noExecutions_returnsZero() {
    var job = persist(newPendingJob());

    int count = auditStore().countExecutionAttempts(job.getId());

    assertEquals(0, count, "countExecutionAttempts with no executions should return 0");
  }

  @Test
  void saveExecution_multipleJobs_isolatedByJobId() {
    var jobA = persist(newPendingJob());
    var jobB = persist(newPendingJob());

    auditStore().saveExecution(JobExecutionEntity.start(jobA.getId(), 1, "node-1"));
    auditStore().saveExecution(JobExecutionEntity.start(jobA.getId(), 2, "node-1"));
    auditStore().saveExecution(JobExecutionEntity.start(jobB.getId(), 1, "node-1"));

    assertEquals(
        2,
        auditStore()
            .findExecutionsByJobId(jobA.getId(), JobAuditStore.DEFAULT_PAGE_LIMIT, 0)
            .size(),
        "Job A should have 2 executions");
    assertEquals(
        1,
        auditStore()
            .findExecutionsByJobId(jobB.getId(), JobAuditStore.DEFAULT_PAGE_LIMIT, 0)
            .size(),
        "Job B should have 1 execution");
  }

  @Test
  void appendLog_persistsLogEntry() {
    var saved = persist(newPendingJob());

    JobLogEntity log =
        new JobLogEntity(
            saved.getId(), Instant.now(), JobLogEntity.LogLevel.INFO, "test log message");

    assertDoesNotThrow(() -> auditStore().appendLog(log), "appendLog should not throw");
  }

  @Test
  void purgeLogsOlderThan_deletesOldLogs() {
    var saved = persist(newPendingJob());

    JobLogEntity log =
        new JobLogEntity(
            saved.getId(),
            Instant.now().minusSeconds(3600),
            JobLogEntity.LogLevel.INFO,
            "old log message");
    auditStore().appendLog(log);

    int purged = auditStore().purgeLogsOlderThan(Instant.now().minusSeconds(1800));

    assertEquals(1, purged, "purgeLogsOlderThan should delete the old log entry");
  }

  @Test
  void purgeLogsOlderThan_preservesRecentLogs() {
    var saved = persist(newPendingJob());

    JobLogEntity recentLog =
        new JobLogEntity(saved.getId(), Instant.now(), JobLogEntity.LogLevel.INFO, "recent log");
    auditStore().appendLog(recentLog);

    int purged = auditStore().purgeLogsOlderThan(Instant.now().minusSeconds(3600));

    assertEquals(0, purged, "purgeLogsOlderThan should not delete recent logs");
  }

  @Test
  void purgeLogsOlderThan_emptyStore_returnsZero() {
    int purged = auditStore().purgeLogsOlderThan(Instant.now());

    assertEquals(0, purged, "purgeLogsOlderThan on empty store should return 0");
  }

  @Test
  void appendLog_multipleEntries_allPersisted() {
    var saved = persist(newPendingJob());
    Instant oldTime = Instant.now().minusSeconds(7200);

    for (int i = 0; i < 3; i++) {
      JobLogEntity log =
          new JobLogEntity(saved.getId(), oldTime, JobLogEntity.LogLevel.INFO, "log entry " + i);
      auditStore().appendLog(log);
    }

    int purged = auditStore().purgeLogsOlderThan(Instant.now().minusSeconds(3600));

    assertEquals(3, purged, "All 3 old log entries should be purged");
  }

  @Test
  void appendLog_differentJobs_isolatedByJobId() {
    var jobA = persist(newPendingJob());
    var jobB = persist(newPendingJob());
    Instant oldTime = Instant.now().minusSeconds(7200);

    JobLogEntity logA =
        new JobLogEntity(jobA.getId(), oldTime, JobLogEntity.LogLevel.INFO, "log for job A");
    auditStore().appendLog(logA);

    JobLogEntity logB =
        new JobLogEntity(
            jobB.getId(), Instant.now(), JobLogEntity.LogLevel.INFO, "recent log for job B");
    auditStore().appendLog(logB);

    int purged = auditStore().purgeLogsOlderThan(Instant.now().minusSeconds(3600));

    assertEquals(1, purged, "Only the old log entry (job A) should be purged");
  }

  @Test
  void appendLog_withMdcMap_persistsContext() {
    var saved = persist(newPendingJob());

    JobLogEntity log =
        new JobLogEntity(
            saved.getId(),
            Instant.now(),
            JobLogEntity.LogLevel.INFO,
            "log with MDC",
            Map.of("traceId", "abc-123", "spanId", "def-456"));

    assertDoesNotThrow(
        () -> auditStore().appendLog(log), "appendLog with MDC map should not throw");
  }

  @Test
  void appendLog_allLogLevels_persists() {
    var saved = persist(newPendingJob());

    for (JobLogEntity.LogLevel level : JobLogEntity.LogLevel.values()) {
      JobLogEntity log =
          new JobLogEntity(saved.getId(), Instant.now(), level, "test " + level.name());
      assertDoesNotThrow(
          () -> auditStore().appendLog(log), "appendLog should accept " + level.name() + " level");
    }
  }
}

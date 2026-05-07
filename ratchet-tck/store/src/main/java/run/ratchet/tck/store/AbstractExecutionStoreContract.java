package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobExecutionEntity;

/** Base contract tests for {@code ExecutionStore}. */
public abstract class AbstractExecutionStoreContract implements JobStoreContractFixture {

  @BeforeEach
  @AfterEach
  void cleanupExecutionFixture() {
    cleanupStore();
  }

  @Test
  void saveAndFindExecutions_roundTrips() {
    var job = persist(newPendingJob());

    var exec = JobExecutionEntity.start(job.getId(), 1, "node-1");
    store().saveExecution(exec);

    var executions = store().findExecutionsByJobId(job.getId());

    assertEquals(1, executions.size(), "findExecutionsByJobId should return the saved execution");
    assertEquals(job.getId(), executions.get(0).getJobId());
  }

  @Test
  void findLatestExecution_returnsNewest() {
    var job = persist(newPendingJob());

    var first = JobExecutionEntity.start(job.getId(), 1, "node-1");
    store().saveExecution(first);

    var second = JobExecutionEntity.start(job.getId(), 2, "node-1");
    store().saveExecution(second);

    var latest = store().findLatestExecution(job.getId());

    assertTrue(latest.isPresent(), "findLatestExecution should return a result");
    assertEquals(2, latest.get().getAttempt(), "Latest execution should be the second attempt");
  }

  @Test
  void countExecutionAttempts_returnsCorrectCount() {
    var job = persist(newPendingJob());

    store().saveExecution(JobExecutionEntity.start(job.getId(), 1, "node-1"));
    store().saveExecution(JobExecutionEntity.start(job.getId(), 2, "node-1"));
    store().saveExecution(JobExecutionEntity.start(job.getId(), 3, "node-1"));

    int count = store().countExecutionAttempts(job.getId());

    assertEquals(3, count, "countExecutionAttempts should return 3 after saving 3 executions");
  }

  @Test
  void findExecutionsByJobId_unknownJob_returnsEmpty() {
    var executions = store().findExecutionsByJobId(new UUID(0L, Long.MAX_VALUE));

    assertTrue(executions.isEmpty(), "findExecutionsByJobId for unknown job should return empty");
  }

  @Test
  void findLatestExecution_unknownJob_returnsEmpty() {
    var latest = store().findLatestExecution(new UUID(0L, Long.MAX_VALUE));

    assertTrue(latest.isEmpty(), "findLatestExecution for unknown job should return empty");
  }

  @Test
  void countExecutionAttempts_noExecutions_returnsZero() {
    var job = persist(newPendingJob());

    int count = store().countExecutionAttempts(job.getId());

    assertEquals(0, count, "countExecutionAttempts with no executions should return 0");
  }

  @Test
  void saveExecution_multipleJobs_isolatedByJobId() {
    var jobA = persist(newPendingJob());
    var jobB = persist(newPendingJob());

    store().saveExecution(JobExecutionEntity.start(jobA.getId(), 1, "node-1"));
    store().saveExecution(JobExecutionEntity.start(jobA.getId(), 2, "node-1"));
    store().saveExecution(JobExecutionEntity.start(jobB.getId(), 1, "node-1"));

    assertEquals(
        2, store().findExecutionsByJobId(jobA.getId()).size(), "Job A should have 2 executions");
    assertEquals(
        1, store().findExecutionsByJobId(jobB.getId()).size(), "Job B should have 1 execution");
  }
}

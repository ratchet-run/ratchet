package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import run.ratchet.store.entity.JobExecutionEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Base contract tests for {@code ExecutionStore}. */
public abstract class AbstractExecutionStoreContract implements JobStoreContractFixture {

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
}

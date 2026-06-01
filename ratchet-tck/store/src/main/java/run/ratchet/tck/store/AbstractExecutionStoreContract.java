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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.spi.ExecutionStore;

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

    var executions =
        store().findExecutionsByJobId(job.getId(), ExecutionStore.DEFAULT_PAGE_LIMIT, 0);

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
  void findExecutionsByJobId_returnsRequestedPage() {
    var job = persist(newPendingJob());

    store().saveExecution(JobExecutionEntity.start(job.getId(), 1, "node-1"));
    store().saveExecution(JobExecutionEntity.start(job.getId(), 2, "node-1"));
    store().saveExecution(JobExecutionEntity.start(job.getId(), 3, "node-1"));

    var executions = store().findExecutionsByJobId(job.getId(), 2, 1);

    assertEquals(2, executions.size(), "paged execution lookup should return the requested window");
    assertEquals(2, executions.get(0).getAttempt(), "page should preserve attempt ordering");
    assertEquals(3, executions.get(1).getAttempt(), "page should preserve attempt ordering");
  }

  @Test
  void findExecutionsByJobId_zeroLimit_returnsEmptyPage() {
    var job = persist(newPendingJob());
    store().saveExecution(JobExecutionEntity.start(job.getId(), 1, "node-1"));

    var executions = store().findExecutionsByJobId(job.getId(), 0, 0);

    assertTrue(executions.isEmpty(), "limit=0 should return an empty execution page");
  }

  @Test
  void findExecutionsByJobId_unknownJob_returnsEmpty() {
    var executions =
        store()
            .findExecutionsByJobId(
                new UUID(0L, Long.MAX_VALUE), ExecutionStore.DEFAULT_PAGE_LIMIT, 0);

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
        2,
        store().findExecutionsByJobId(jobA.getId(), ExecutionStore.DEFAULT_PAGE_LIMIT, 0).size(),
        "Job A should have 2 executions");
    assertEquals(
        1,
        store().findExecutionsByJobId(jobB.getId(), ExecutionStore.DEFAULT_PAGE_LIMIT, 0).size(),
        "Job B should have 1 execution");
  }
}

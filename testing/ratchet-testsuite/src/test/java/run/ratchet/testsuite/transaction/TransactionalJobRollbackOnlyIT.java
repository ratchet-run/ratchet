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
package run.ratchet.testsuite.transaction;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TransactionalUnitProbe;
import run.ratchet.testsuite.util.BaseRatchetIT;

/**
 * A {@code @TransactionalJob} body that leaves its transaction marked for rollback fails its
 * attempt.
 */
@DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
class TransactionalJobRollbackOnlyIT extends BaseRatchetIT {
  @Inject private TestJobService scheduler;
  @Inject private JobStore store;
  @Inject private JobAuditStore audit;
  @Inject private TransactionalUnitProbe probe;
  private final List<JobHandle> handles = new ArrayList<>();
  private String key;

  @Deployment
  public static WebArchive createDeployment() {
    return TransactionalCheckpointIT.createDeployment();
  }

  @BeforeEach
  void prepare() {
    probe.clear();
    key = UUID.randomUUID().toString();
    handles.clear();
  }

  @AfterEach
  void drainAndClear() {
    try {
      for (JobHandle handle : handles) awaitExecutionClosed(handle);
    } finally {
      SwallowedRollbackJobs.ATTEMPTS.remove(key);
      probe.clear();
    }
  }

  @Test
  void swallowedNestedFailureFailsTheAttemptAndSavesNothing() {
    String value = key;
    JobHandle handle =
        track(
            scheduler.enqueue(() -> SwallowedRollbackJobs.body(value)).withMaxRetries(0).submit());
    awaitStatus(handle, JobStatus.FAILED);
    awaitExecutionClosed(handle);
    String error = store.findById(handle.id()).orElseThrow().getLastError();
    assertNotNull(error);
    assertTrue(error.contains("marked for rollback"), error);
    assertEquals(0, probe.countPage(key, 0));
    assertEquals(1, SwallowedRollbackJobs.attempts(key));
    var executions = audit.findExecutionsByJobId(handle.id(), 10, 0);
    assertEquals(1, executions.size());
    assertEquals(ExecutionStatus.FAILED, executions.get(0).getStatus());
    assertTrue(
        executions.get(0).getErrorMessage().contains("marked for rollback"),
        executions.get(0).getErrorMessage());
  }

  @Test
  void retryRunsTheBodyAgainAfterARolledBackAttempt() {
    String value = key;
    JobHandle handle =
        track(
            scheduler
                .enqueue(() -> SwallowedRollbackJobs.bodyOnce(value))
                .withMaxRetries(1)
                .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
                .submit());
    awaitStatus(handle, JobStatus.SUCCEEDED);
    awaitExecutionClosed(handle);
    assertEquals(2, SwallowedRollbackJobs.attempts(key));
    assertEquals(1, probe.countPage(key, 0));
    var executions = audit.findExecutionsByJobId(handle.id(), 10, 0);
    assertEquals(2, executions.size());
    assertEquals(ExecutionStatus.FAILED, executions.get(0).getStatus());
    assertTrue(
        executions.get(0).getErrorMessage().contains("marked for rollback"),
        executions.get(0).getErrorMessage());
    assertEquals(ExecutionStatus.SUCCEEDED, executions.get(1).getStatus());
  }

  private JobHandle track(JobHandle handle) {
    handles.add(handle);
    return handle;
  }

  private void awaitStatus(JobHandle handle, JobStatus expected) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              var job = store.findById(handle.id()).orElseThrow();
              assertEquals(
                  expected,
                  job.getStatus(),
                  "status="
                      + job.getStatus()
                      + ", lastError="
                      + job.getLastError()
                      + ", wrote="
                      + (probe.countPage(key, 0) != 0));
            });
  }

  private void awaitExecutionClosed(JobHandle handle) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertNotEquals(
                    ExecutionStatus.RUNNING,
                    audit.findLatestExecution(handle.id()).orElseThrow().getStatus()));
  }
}

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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.BusinessKeyLookup;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.store.entity.JobExecutionEntity;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TransactionalJobProbe;
import run.ratchet.testsuite.app.TransactionalJobProbe.Attempt;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Real JTA rollback and completion-fence checks on every SQL store. */
@DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
class TransactionalCompletionIT extends BaseRatchetIT {
  @Inject private TestJobService scheduler;
  @Inject private JobCrudStore jobs;
  @Inject private JobAuditStore audit;
  @Inject private TransactionalJobProbe probe;
  @Inject private BusinessKeyLookup lookup;
  private final List<JobHandle> handles = new ArrayList<>();
  private String key;
  private Attempt attempt;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(TestJobService.class, TransactionalJobProbe.class, Attempt.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void prepare() {
    probe.clear();
    TransactionalJobProbe.reset();
    handles.clear();
    key = UUID.randomUUID().toString();
    attempt = TransactionalJobProbe.prepare(key);
  }

  @AfterEach
  void releaseAndDrain() {
    attempt.release.countDown();
    try {
      for (JobHandle handle : handles) awaitExecutionClosed(handle);
    } finally {
      TransactionalJobProbe.reset();
      probe.clear();
    }
  }

  @Test
  void successCommitsApplicationRowAndSucceededTogether() {
    String value = key;
    JobHandle handle =
        track(scheduler.enqueue(() -> TransactionalJobProbe.succeed(value)).submit());
    awaitStatus(handle, JobStatus.SUCCEEDED);
    awaitExecutionClosed(handle);
    assertEquals(1, probe.count(key));
    assertEquals(1, attempt.writes.get());
    assertEquals(
        ExecutionStatus.SUCCEEDED,
        audit.findLatestExecution(handle.id()).orElseThrow().getStatus());
  }

  @Test
  void failedFirstAttemptRollsBackBeforeRetrySucceeds() {
    String value = key;
    JobHandle handle =
        track(
            scheduler
                .enqueue(() -> TransactionalJobProbe.retry(value))
                .withMaxRetries(1)
                .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
                .submit());
    awaitStatus(handle, JobStatus.SUCCEEDED);
    awaitExecutionClosed(handle);
    assertEquals(2, attempt.bodies.get());
    assertEquals(2, attempt.writes.get());
    assertEquals(1, probe.count(key), "The failed attempt must not leave an application row");
    var executions = audit.findExecutionsByJobId(handle.id(), 10, 0);
    assertEquals(2, executions.size());
    assertEquals(ExecutionStatus.FAILED, executions.get(0).getStatus());
    assertEquals(ExecutionStatus.SUCCEEDED, executions.get(1).getStatus());
  }

  @Test
  void cancellationAfterApplicationWriteRollsBackTheWrite() throws Exception {
    JobHandle handle = submitBlocked();
    try {
      assertTrue(
          attempt.written.await(30, TimeUnit.SECONDS), "Body must write before cancellation");
      assertTrue(scheduler.cancelJob(handle.id()));
    } finally {
      attempt.release.countDown();
    }
    awaitStatus(handle, JobStatus.CANCELED);
    awaitExecutionClosed(handle);
    assertEquals(0, probe.count(key));
    assertEquals(1, attempt.bodies.get());
    assertEquals(1, attempt.writes.get());
    assertEquals(
        ExecutionStatus.CANCELED, audit.findLatestExecution(handle.id()).orElseThrow().getStatus());
  }

  @Test
  void cancelStopsTheBodyAndHoldsItsBusinessKeyUntilTheBodyEnds() throws Exception {
    // The stop request reaches the body, its write rolls back, and the key waits for the body.
    String value = key;
    String businessKey = "transactional-cancel-" + key;
    JobHandle handle =
        track(
            scheduler
                .enqueue(() -> TransactionalJobProbe.stopWhenCancelled(value))
                .withBusinessKey(businessKey)
                .withMaxRetries(0)
                .submit());
    try {
      assertTrue(
          attempt.written.await(30, TimeUnit.SECONDS), "Body must write before cancellation");
      assertTrue(scheduler.cancelJob(handle.id()));
      assertTrue(
          attempt.stopRequested.await(15, TimeUnit.SECONDS), "Body must see the stop request");
      assertEquals(
          new BusinessKeyHolder(handle.id(), JobType.SINGLE, JobStatus.CANCELED),
          lookup.findBusinessKeyHolder(businessKey).orElseThrow());
    } finally {
      attempt.release.countDown();
    }
    awaitExecutionClosed(handle);
    await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> lookup.findBusinessKeyHolder(businessKey).isEmpty());
    assertEquals(JobStatus.CANCELED, jobs.findById(handle.id()).orElseThrow().getStatus());
    assertEquals(0, probe.count(key));
    assertEquals(
        ExecutionStatus.CANCELED, audit.findLatestExecution(handle.id()).orElseThrow().getStatus());
  }

  @Test
  void changedClaimRollsBackApplicationWriteAndDiscardsSupersededAttempt() throws Exception {
    JobHandle handle = submitBlocked();
    long claim;
    try {
      assertTrue(
          attempt.written.await(30, TimeUnit.SECONDS), "Body must write before moving its claim");
      claim = jobs.findById(handle.id()).orElseThrow().getClaimSeq();
      assertTrue(attempt.context.get().isCurrentAttempt());
      assertEquals(1, probe.moveClaim(handle.id(), claim));
      assertFalse(attempt.context.get().isCurrentAttempt());
    } finally {
      attempt.release.countDown();
    }
    awaitExecutionClosed(handle);
    assertEquals(0, probe.count(key));
    var job = jobs.findById(handle.id()).orElseThrow();
    assertEquals(claim + 1, job.getClaimSeq().longValue());
    assertEquals(
        JobStatus.RUNNING, job.getStatus(), "Old attempt must not finalize the moved claim");
    var execution = audit.findLatestExecution(handle.id()).orElseThrow();
    assertEquals(ExecutionStatus.FAILED, execution.getStatus());
    assertEquals(JobExecutionEntity.RESULT_DISCARDED_ERROR_MESSAGE, execution.getErrorMessage());
    assertEquals(1, attempt.bodies.get());
    assertEquals(1, attempt.writes.get());
    // Dispose of the synthetic moved claim after proving the old worker could not finalize it.
    assertTrue(scheduler.cancelJob(handle.id()));
  }

  private JobHandle submitBlocked() {
    String value = key;
    return track(
        scheduler.enqueue(() -> TransactionalJobProbe.block(value)).withMaxRetries(0).submit());
  }

  private JobHandle track(JobHandle handle) {
    handles.add(handle);
    return handle;
  }

  private void awaitStatus(JobHandle handle, JobStatus status) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> assertEquals(status, jobs.findById(handle.id()).orElseThrow().getStatus()));
  }

  private void awaitExecutionClosed(JobHandle handle) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              var execution = audit.findLatestExecution(handle.id()).orElseThrow();
              assertTrue(execution.getStatus() != ExecutionStatus.RUNNING);
            });
  }
}

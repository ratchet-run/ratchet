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
import jakarta.persistence.TransactionRequiredException;
import jakarta.transaction.TransactionalException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.api.exception.AttemptSupersededException;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobExecutionEntity.ExecutionStatus;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.TransactionalCompletionStore;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TransactionalUnitProbe;
import run.ratchet.testsuite.app.TransactionalUnitProbe.Attempt;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Atomic application writes and checkpoints through the server's real JTA carrier. */
@DisabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mongodb")
class TransactionalCheckpointIT extends BaseRatchetIT {
  @Inject private TestJobService scheduler;
  @Inject private JobStore store;
  @Inject private JobAuditStore audit;
  @Inject private PayloadSerializer serializer;
  @Inject private TransactionalUnitProbe probe;
  private final List<JobHandle> handles = new ArrayList<>();
  private String key;
  private Attempt attempt;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), dbType)
        .addClasses(TestJobService.class, TransactionalUnitProbe.class, Attempt.class)
        .addStoreInfrastructure()
        .addResource(
            RatchetArchiveBuilder.testRuntimeConfigContent(
                dbType,
                Map.of(
                    "ratchet.execution.checkpoint-flush-interval-ms", "600000",
                    "ratchet.execution.progress-flush-interval-ms", "500",
                    // Deliberate failures would open the breaker and skip later probe jobs.
                    "ratchet.circuit-breaker.enabled", "false",
                    "ratchet.timeout.cancellation-grace-seconds", "0",
                    "ratchet.timeout.default-sla-seconds", "3")),
            "ratchet-testsuite.properties")
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void prepare() {
    probe.clear();
    TransactionalUnitProbe.reset();
    handles.clear();
    key = UUID.randomUUID().toString();
    attempt = TransactionalUnitProbe.prepare(key);
  }

  @AfterEach
  void releaseAndDrain() {
    attempt.release.countDown();
    try {
      for (var handle : handles) awaitExecutionClosed(handle);
    } finally {
      TransactionalUnitProbe.reset();
      probe.clear();
    }
  }

  @Test
  void unitCommitsApplicationRowAndCheckpointTogether() throws Exception {
    var handle = submit("atomic");
    try {
      assertTrue(attempt.inside.await(30, TimeUnit.SECONDS));
      assertEquals(0, probe.countPage(key, 0));
      assertNoCheckpoint(handle);
    } finally {
      attempt.release.countDown();
    }
    awaitStatus(handle, JobStatus.SUCCEEDED);
    assertEquals(1, probe.countPage(key, 0));
    assertCheckpoint(handle, "1");
  }

  @Test
  void failedUnitRollsBackApplicationRowAndCheckpoint() {
    var handle = submit("rollback");
    awaitStatus(handle, JobStatus.SUCCEEDED);
    assertEquals("1", attempt.retryFirst);
    assertEquals(List.of(Optional.empty(), Optional.of("1")), attempt.starts);
    assertEquals(List.of(Optional.empty(), Optional.of("1"), Optional.of("1")), attempt.received);
    assertEquals(1, probe.countPage(key, 0));
    assertEquals(1, probe.countPage(key, 1));
    assertCheckpoint(handle, "done");
  }

  @Test
  void retryAfterCommittedUnitResumesAfterThatUnit() {
    var handle = submit("retry");
    awaitStatus(handle, JobStatus.SUCCEEDED);
    assertEquals("2", attempt.retryFirst);
    assertEquals(List.of(Optional.empty(), Optional.of("1"), Optional.of("2")), attempt.received);
    for (int page = 0; page < 3; page++) assertEquals(1, probe.countPage(key, page));
    assertCheckpoint(handle, "done");
  }

  @Test
  void changedClaimRollsBackUnitAndThrowsAttemptSuperseded() throws Exception {
    var handle = submit("blocked");
    try {
      assertTrue(attempt.inside.await(30, TimeUnit.SECONDS));
      long claim = store.findById(handle.id()).orElseThrow().getClaimSeq();
      assertEquals(1, probe.moveClaim(handle.id(), claim));
      attempt.release.countDown();
      awaitExecutionClosed(handle);
      assertInstanceOf(AttemptSupersededException.class, attempt.failure.get());
      assertEquals(0, probe.countPage(key, 0));
      assertNoCheckpoint(handle);
      assertEquals(claim + 1, store.findById(handle.id()).orElseThrow().getClaimSeq().longValue());
    } finally {
      attempt.release.countDown();
      // Dispose of the synthetic claim after proving the old unit rolled back.
      assertTrue(scheduler.cancelJob(handle.id()));
    }
  }

  @Test
  void cancelDuringUnitIsNotBlockedAndRollsBackUnit() throws Exception {
    var handle = submit("blocked");
    try {
      assertTrue(attempt.inside.await(30, TimeUnit.SECONDS));
      assertTimeout(Duration.ofSeconds(5), () -> assertTrue(scheduler.cancelJob(handle.id())));
      assertEquals(1, attempt.release.getCount());
      assertEquals(1, attempt.finished.getCount());
    } finally {
      attempt.release.countDown();
    }
    awaitStatus(handle, JobStatus.CANCELED);
    awaitExecutionClosed(handle);
    assertEquals(0, probe.countPage(key, 0));
    assertNoCheckpoint(handle);
  }

  @Test
  void hardTimeoutDuringUnitRetriesFromLastCommittedUnit() {
    String value = key;
    var handle =
        scheduler
            .enqueue(() -> TransactionalUnitProbe.execute(value, "timeout"))
            .withTimeout(Duration.ofSeconds(3))
            .withMaxRetries(1)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
            .submit();
    handles.add(handle);
    awaitStatus(handle, JobStatus.SUCCEEDED);
    assertEquals("1", attempt.retryFirst);
    assertTrue(attempt.timeoutInterrupted, "The hard watchdog must interrupt the open unit");
    assertEquals(1, probe.countPage(key, 0));
    assertEquals(1, probe.countPage(key, 1));
    assertCheckpoint(handle, "done");
  }

  @Test
  void progressFlushesDuringUnitAndCancel() throws Exception {
    var handle = submit("progress");
    try {
      assertTrue(attempt.inside.await(30, TimeUnit.SECONDS));
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                assertProgress(handle);
                assertEquals(1, attempt.release.getCount());
                assertEquals(1, attempt.finished.getCount());
                assertEquals(
                    JobStatus.RUNNING, store.findById(handle.id()).orElseThrow().getStatus());
              });
      assertTrue(scheduler.cancelJob(handle.id()));
      assertProgress(handle);
    } finally {
      attempt.release.countDown();
    }
    awaitStatus(handle, JobStatus.CANCELED);
    awaitExecutionClosed(handle);
    assertEquals(0, probe.countPage(key, 0));
    assertNoCheckpoint(handle);
    assertProgress(handle);
  }

  @Test
  void enclosingTransactionIsRejected() {
    var handle = submit("enclosing");
    awaitStatus(handle, JobStatus.SUCCEEDED);
    assertInstanceOf(IllegalStateException.class, attempt.failure.get());
    assertFalse(attempt.unitRan);
    assertEquals(0, probe.countPage(key, 0));
    assertNoCheckpoint(handle);
  }

  @Test
  void claimStateWriteRequiresCallerTransaction() {
    UUID id = UUID.randomUUID();
    var completion = store.capability(TransactionalCompletionStore.class).orElseThrow();
    Exception failure =
        assertThrows(
            Exception.class,
            () -> completion.writeClaimState(id, "ratchet.checkpoint", "{}", 0L, null));
    boolean mandatory = false;
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof TransactionalException || cause instanceof TransactionRequiredException)
        mandatory = true;
    }
    assertTrue(mandatory, () -> "Expected a MANDATORY transaction failure: " + failure);
    assertTrue(states().getState(id, "ratchet.checkpoint").isEmpty());
  }

  private JobHandle submit(String scenario) {
    String value = key;
    var handle =
        scheduler
            .enqueue(() -> TransactionalUnitProbe.execute(value, scenario))
            .withTimeout(Duration.ofSeconds(60))
            .withMaxRetries(scenario.equals("rollback") || scenario.equals("retry") ? 1 : 0)
            .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
            .submit();
    handles.add(handle);
    return handle;
  }

  private void assertProgress(JobHandle handle) {
    // An assertion, not orElseThrow(), so awaitility keeps polling until the flush lands.
    var row = states().getState(handle.id(), "ratchet.progress");
    assertTrue(row.isPresent(), "Progress has not been flushed yet");
    var progress = serializer.deserialize(row.get().json(), Map.class);
    assertEquals(1L, ((Number) progress.get("done")).longValue());
    assertEquals(3L, ((Number) progress.get("total")).longValue());
  }

  private JobStateStore states() {
    return store.capability(JobStateStore.class).orElseThrow();
  }

  private void assertNoCheckpoint(JobHandle handle) {
    assertTrue(states().getState(handle.id(), "ratchet.checkpoint").isEmpty());
  }

  private void assertCheckpoint(JobHandle handle, String expected) {
    var row = states().getState(handle.id(), "ratchet.checkpoint").orElseThrow();
    assertEquals(expected, serializer.deserialize(row.json(), Map.class).get("value"));
  }

  private void awaitStatus(JobHandle handle, JobStatus status) {
    await()
        .atMost(Duration.ofSeconds(40))
        .untilAsserted(
            () -> assertEquals(status, store.findById(handle.id()).orElseThrow().getStatus()));
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

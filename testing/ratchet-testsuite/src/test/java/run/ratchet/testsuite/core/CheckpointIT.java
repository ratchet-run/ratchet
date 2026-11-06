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
package run.ratchet.testsuite.core;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.spi.JobStateStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.CheckpointJob;
import run.ratchet.testsuite.app.CheckpointTransactions;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Checks commit, rollback and immediate durability on every testsuite store. */
class CheckpointIT extends BaseRatchetIT {
  @Inject private TestJobService jobs;
  @Inject private JobStore store;
  @Inject private PayloadSerializer serializer;

  @Deployment
  public static WebArchive createDeployment() {
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(
            System.getProperty("testsuite.profile", "wildfly-managed"),
            System.getProperty("ratchet.test.db.type", "mysql"))
        .addClasses(CheckpointJob.class, CheckpointTransactions.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void reset() {
    CheckpointJob.reset();
  }

  @Test
  void committedCheckpointSurvivesRetryAndCompletion() {
    JobHandle handle =
        jobs.enqueue(CheckpointJob::retryCommitted)
            .withMaxRetries(1)
            .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
            .submit();
    assertRetry(handle);
  }

  @Test
  void rollbackPreservesCommittedValueInSameAttemptAndRetry() {
    JobHandle handle =
        jobs.enqueue(CheckpointJob::retryRolledBack)
            .withMaxRetries(1)
            .withBackoff(BackoffPolicy.FIXED, Duration.ZERO)
            .submit();
    assertRetry(handle);
  }

  private void assertRetry(JobHandle handle) {
    JobAssertions.assertJobCompleted(store, handle);
    assertEquals(
        List.of(Optional.of("page-1"), Optional.of("page-1")),
        CheckpointJob.OBSERVED.get(handle.id()));
    assertStored(handle.id(), "page-1");
  }

  @Test
  void noTransactionWritesBeforeReturning() throws InterruptedException {
    JobHandle handle = jobs.enqueue(CheckpointJob::immediate).submit();
    try {
      assertTrue(CheckpointJob.written.await(20, TimeUnit.SECONDS));
      assertStored(handle.id(), "now");
    } finally {
      CheckpointJob.release.countDown();
    }
    JobAssertions.assertJobCompleted(store, handle);
  }

  @Test
  void flusherWritesWhileJobIsStillRunning() throws InterruptedException {
    JobHandle handle = jobs.enqueue(CheckpointJob::live).submit();
    try {
      assertTrue(CheckpointJob.written.await(20, TimeUnit.SECONDS));
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                var state =
                    store
                        .capability(JobStateStore.class)
                        .orElseThrow()
                        .getState(handle.id(), "ratchet.checkpoint");
                assertTrue(
                    state.isPresent(), "Background flusher has not saved the checkpoint yet");
                assertEquals(
                    "live",
                    serializer.deserialize(state.orElseThrow().json(), Map.class).get("value"));
                assertEquals(
                    JobStatus.RUNNING, store.findById(handle.id()).orElseThrow().getStatus());
                assertEquals(1L, CheckpointJob.release.getCount());
              });
    } finally {
      CheckpointJob.release.countDown();
    }
    JobAssertions.assertJobCompleted(store, handle);
  }

  private void assertStored(UUID id, String expected) {
    var state =
        store
            .capability(JobStateStore.class)
            .orElseThrow()
            .getState(id, "ratchet.checkpoint")
            .orElseThrow();
    assertEquals(expected, serializer.deserialize(state.json(), Map.class).get("value"));
  }
}

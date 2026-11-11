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
package run.ratchet.testsuite.batch;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobQueryService;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.KeyRange;
import run.ratchet.api.PartitionStatus;
import run.ratchet.api.PartitionedBatchBuilder;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.PartitionStore;
import run.ratchet.testsuite.app.PartitionJobs;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

class PartitionedBatchIT extends BaseRatchetIT {
  @Inject private JobSchedulerService scheduler;
  @Inject private JobQueryService query;
  @Inject private JobStore jobs;
  private String key;

  @Deployment
  public static WebArchive createDeployment() {
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(
            System.getProperty("testsuite.profile", "wildfly-managed"),
            System.getProperty("ratchet.test.db.type", "mysql"))
        .addClasses(PartitionJobs.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void reset() {
    assumeTrue(jobs.capability(PartitionStore.class).isPresent());
    key = UUID.randomUUID().toString();
    PartitionJobs.reset(false, false);
  }

  private PartitionedBatchBuilder batch() {
    return scheduler
        .enqueueBatch("partition-integration")
        .forEachPartition(key, KeyRange.split(0, 40, 10), PartitionJobs::process)
        .withMaxRetries(0);
  }

  private void succeeded(JobHandle handle) {
    JobAssertions.assertBatchSucceeded(jobs, handle, Duration.ofSeconds(30));
  }

  private void seal() {
    batch().submit();
  }

  @Test
  void failedPartitionResumesAloneFromItsSavedCheckpoint() {
    PartitionJobs.reset(true, false);
    JobHandle first = batch().submit();
    JobAssertions.assertBatchTerminated(jobs, first, Duration.ofSeconds(30));
    assertEquals(JobStatus.FAILED, jobs.getJobStatus(first.id()));
    PartitionJobs.succeed();
    JobHandle second = batch().submit();
    succeeded(second);
    seal();
    assertNotEquals(first.id(), second.id());
    assertEquals(2, PartitionJobs.calls.get(20L).get());
    for (long from : List.of(0L, 10L, 30L)) assertEquals(1, PartitionJobs.calls.get(from).get());
    assertEquals("25", PartitionJobs.firstCheckpoint.get(20L));
    var rows = query.getBatchPartitions(key, 10, 0);
    assertEquals(4, rows.totalCount());
    for (var row : rows.items()) {
      assertEquals(PartitionStatus.SUCCEEDED, row.status());
      assertEquals(row.index() == 2 ? 2 : 1, row.generation());
    }
    assertTrue(scheduler.deletePartitionSet(key));
  }

  @Test
  void runningGenerationReturnsSameIdAndCreatesNothing() {
    PartitionJobs.reset(false, true);
    JobHandle first = batch().submit();
    try {
      await().atMost(Duration.ofSeconds(20)).until(() -> !PartitionJobs.calls.isEmpty());
      assertEquals(first.id(), batch().submit().id());
      var summary = query.getPartitionSet(key).orElseThrow();
      assertTrue(summary.open());
      assertEquals(1, summary.latestGeneration());
      assertEquals(first.id(), summary.latestBatchId());
      assertEquals(
          4, jobs.capability(PartitionStore.class).orElseThrow().countBatchChildren(first.id()));
    } finally {
      PartitionJobs.release();
    }
    succeeded(first);
    seal();
    assertTrue(scheduler.deletePartitionSet(key));
  }

  @Test
  void reopenStartsFreshAndRequestRetryDeduplicates() {
    JobHandle first = batch().submit();
    succeeded(first);
    String request = UUID.randomUUID().toString();
    JobHandle reopened = batch().withIdempotencyKey(request).reopen(List.of(0)).submit();
    assertEquals(
        reopened.id(), batch().withIdempotencyKey(request).reopen(List.of(0)).submit().id());
    succeeded(reopened);
    seal();
    assertEquals(2, PartitionJobs.calls.get(0L).get());
    assertEquals("none", PartitionJobs.firstCheckpoint.get(0L));
    for (long from : List.of(10L, 20L, 30L)) assertEquals(1, PartitionJobs.calls.get(from).get());
    assertTrue(scheduler.deletePartitionSet(key));
  }

  @Test
  void sealedSetCanBeDeletedAndKeyReused() {
    PartitionJobs.reset(false, true);
    JobHandle first = batch().submit();
    try {
      await().atMost(Duration.ofSeconds(20)).until(() -> !PartitionJobs.calls.isEmpty());
      assertTrue(query.getPartitionSet(key).orElseThrow().open());
    } finally {
      PartitionJobs.release();
    }
    succeeded(first);
    seal();
    var summary = query.getPartitionSet(key).orElseThrow();
    assertFalse(summary.open());
    assertEquals(first.id(), summary.latestBatchId());
    assertTrue(scheduler.deletePartitionSet(key));
    assertFalse(scheduler.deletePartitionSet(key));
    assertTrue(query.getPartitionSet(key).isEmpty());
    JobHandle fresh = batch().submit();
    succeeded(fresh);
    assertNotEquals(first.id(), fresh.id());
    assertEquals(1, query.getPartitionSet(key).orElseThrow().latestGeneration());
    seal();
    assertTrue(scheduler.deletePartitionSet(key));
  }
}

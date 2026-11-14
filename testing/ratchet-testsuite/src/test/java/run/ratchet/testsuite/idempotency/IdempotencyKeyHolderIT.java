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
package run.ratchet.testsuite.idempotency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.IdempotencyKeyHolder;
import run.ratchet.api.IdempotencyKeyHolder.Retention;
import run.ratchet.api.IdempotencyKeyLookup;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.ri.core.JobArchivingService;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestDataManipulator;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** The ledger remains readable through the public API after archive and purge. */
class IdempotencyKeyHolderIT extends BaseRatchetIT {
  @Inject private JobSchedulerService scheduler;
  @Inject private IdempotencyKeyLookup lookup;
  @Inject private JobStore store;
  @Inject private JobArchivingService archivingService;
  @Inject private TestDataManipulator dataManipulator;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void unusedKeyReturnsEmpty() {
    assertTrue(lookup.findIdempotencyKeyHolder(UUID.randomUUID().toString()).isEmpty());
  }

  @Test
  void liveKeyIsReadableThroughStableInterface() {
    String key = UUID.randomUUID().toString();
    var handle =
        scheduler
            .schedule(Duration.ofHours(1), SimpleJob::execute)
            .withIdempotencyKey(key)
            .submit();
    try {
      var expected =
          new IdempotencyKeyHolder(handle.id(), Retention.LIVE, JobType.SINGLE, JobStatus.PENDING);
      assertEquals(expected, lookup.findIdempotencyKeyHolder(key).orElseThrow());
      assertEquals(expected, scheduler.findIdempotencyKeyHolder(key).orElseThrow());
    } finally {
      scheduler.cancelJob(handle.id());
    }
  }

  @Test
  void archivedAndPurgedKeyStillReturnsOriginalIdOnResubmit() throws Exception {
    var capability = store.capability(ArchiveStore.class);
    assumeTrue(capability.isPresent(), "ArchiveStore is not advertised");
    var archives = capability.orElseThrow();
    String key = UUID.randomUUID().toString();
    var handle = scheduler.enqueue(SimpleJob::execute).withIdempotencyKey(key).submit();
    try {
      JobAssertions.assertJobCompleted(store, handle);
      var terminal = store.findById(handle.id()).orElseThrow().getStatus();
      assertEquals(
          new IdempotencyKeyHolder(handle.id(), Retention.LIVE, JobType.SINGLE, terminal),
          lookup.findIdempotencyKeyHolder(key).orElseThrow());
      dataManipulator.setJobUpdatedAt(handle.id(), Instant.now().minus(3, ChronoUnit.DAYS));
      var parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));
      archivingService.init(true, 1, 100, parser.parse("0 0 2 * * ?"));
      archivingService.triggerArchiving().get(30, TimeUnit.SECONDS);
      assertTrue(store.findById(handle.id()).isEmpty());
      assertEquals(
          new IdempotencyKeyHolder(handle.id(), Retention.ARCHIVED, JobType.SINGLE, terminal),
          lookup.findIdempotencyKeyHolder(key).orElseThrow());
      var archived = archives.findArchivedJob(handle.id()).orElseThrow();
      // Backdate only this archive so the purge does not consume unrelated recent archives.
      Instant cutoff = Instant.now().minus(10, ChronoUnit.DAYS);
      dataManipulator.setArchivedAt(archived.getId(), cutoff.minusSeconds(60));
      archives.purgeArchivedJobs(cutoff);
      assertTrue(archives.findArchivedJob(handle.id()).isEmpty());
      assertEquals(
          new IdempotencyKeyHolder(handle.id(), Retention.PURGED, null, null),
          lookup.findIdempotencyKeyHolder(key).orElseThrow());
      var resubmitted = scheduler.enqueue(SimpleJob::execute).withIdempotencyKey(key).submit();
      assertEquals(handle.id(), resubmitted.id());
      assertTrue(store.findById(handle.id()).isEmpty());
      assertEquals(
          Retention.PURGED, lookup.findIdempotencyKeyHolder(key).orElseThrow().retention());
    } finally {
      scheduler.cancelJob(handle.id());
    }
  }
}

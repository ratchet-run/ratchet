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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import run.ratchet.api.BusinessKeyHolder;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.JobStatus;
import run.ratchet.api.JobType;
import run.ratchet.api.exception.DuplicateBusinessKeyException;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.CancellableJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Checks running cancellation reservations on every store, including MongoDB. */
class BusinessKeyHoldIT extends BaseRatchetIT {
  @Inject private JobSchedulerService scheduler;
  @Inject private JobCrudStore store;
  private final List<JobHandle> jobs = new ArrayList<>();
  private JobHandle running;

  @Deployment
  public static WebArchive createDeployment() {
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(
            System.getProperty("testsuite.profile", "wildfly-managed"),
            System.getProperty("ratchet.test.db.type", "mysql"))
        .addClasses(CancellableJob.class, SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void reset() {
    CancellableJob.reset();
    SimpleJob.resetCount();
  }

  @AfterEach
  void cleanup() {
    CancellableJob.release();
    for (JobHandle job : jobs) scheduler.cancelJob(job.id());
    if (running != null) {
      await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.ENDED);
    }
  }

  @Test
  void cancelledRunningJobHoldsItsKeyUntilTheBodyEnds() {
    // A running canceled body keeps its reservation until the test lets it exit.
    String key = "cancel-hold-" + UUID.randomUUID();
    JobHandle old = start(key);
    assertTrue(scheduler.cancelJob(old.id()));
    await().atMost(Duration.ofSeconds(5)).untilTrue(CancellableJob.SAW_REQUEST);
    assertHeld(key, old);
    DuplicateBusinessKeyException failure =
        assertThrows(
            DuplicateBusinessKeyException.class,
            () -> jobs.add(scheduler.enqueue(SimpleJob::execute).withBusinessKey(key).submit()));
    assertTrue(failure.getMessage().contains("held by canceled job"));
    assertTrue(failure.getMessage().contains(old.id().toString()));
    CancellableJob.release();
    awaitFree(key);
    assertNotNull(submitLater(key));
  }

  @Test
  void cancelledPendingJobReleasesItsKeyAtOnce() {
    // A pending cancellation immediately frees its business key for another submit.
    String key = "pending-cancel-" + UUID.randomUUID();
    JobHandle old = submitLater(key);
    assertTrue(scheduler.cancelJob(old.id()));
    assertTrue(scheduler.findBusinessKeyHolder(key).isEmpty());
    assertNotNull(submitLater(key));
  }

  @Test
  void replaceOfRunningKeyedJobStopsTheOldBodyAndHoldsTheKey() {
    // Replacement runs without the old key while the canceled body still holds it.
    String key = "replace-hold-" + UUID.randomUUID();
    JobHandle old = start(key);
    JobHandle replacement = scheduler.replace(old.id(), Duration.ZERO, SimpleJob::execute, null);
    jobs.add(replacement);
    assertNotNull(replacement);
    await().atMost(Duration.ofSeconds(5)).untilTrue(CancellableJob.SAW_REQUEST);
    assertHeld(key, old);
    JobAssertions.assertJobCompleted(store, replacement);
    assertHeld(key, old);
    CancellableJob.release();
    awaitFree(key);
  }

  private JobHandle start(String key) {
    CancellableJob.HOLD_AFTER_REQUEST = true;
    running =
        scheduler
            .enqueue(CancellableJob::execute)
            .withBusinessKey(key)
            .withTimeout(Duration.ofSeconds(120))
            .submit();
    jobs.add(running);
    await().atMost(Duration.ofSeconds(15)).untilTrue(CancellableJob.STARTED);
    return running;
  }

  private JobHandle submitLater(String key) {
    JobHandle job =
        scheduler.schedule(Duration.ofHours(1), SimpleJob::execute).withBusinessKey(key).submit();
    jobs.add(job);
    return job;
  }

  private void assertHeld(String key, JobHandle old) {
    assertFalse(CancellableJob.ENDED.get(), "The old body must still be waiting on the gate");
    assertEquals(
        new BusinessKeyHolder(old.id(), JobType.SINGLE, JobStatus.CANCELED),
        scheduler.findBusinessKeyHolder(key).orElseThrow());
  }

  private void awaitFree(String key) {
    await()
        .atMost(Duration.ofSeconds(15))
        .until(() -> scheduler.findBusinessKeyHolder(key).isEmpty());
  }
}

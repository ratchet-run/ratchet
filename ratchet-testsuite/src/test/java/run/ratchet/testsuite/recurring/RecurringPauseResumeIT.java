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
package run.ratchet.testsuite.recurring;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.testsuite.app.CronTestJobs;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Validates pause/resume behavior for recurring jobs.
 *
 * <p>Tests that pausing a recurring job prevents it from firing and resuming re-enables execution.
 */
class RecurringPauseResumeIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private RecurringJobStore recurringJobStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(CronTestJobs.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    CronTestJobs.reset();
  }

  @Test
  void pauseRecurringJob_shouldStopFiring() {
    JobHandle handle = scheduleTickingRecurring();

    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertTrue(
                    CronTestJobs.tickCount() >= 1,
                    "Expected at least 1 tick but got " + CronTestJobs.tickCount()));

    boolean paused = jobService.pauseJob(handle.id());
    assertTrue(paused, "Expected pauseJob to return true");

    assertTrue(
        recurringJobStore.getRecurring(handle.id()).orElseThrow().paused(),
        "Expected recurring master to be paused");

    // Record tick count at pause time and verify it remains stable while paused.
    int ticksAtPause = CronTestJobs.tickCount();
    await()
        .during(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertEquals(
                    ticksAtPause,
                    CronTestJobs.tickCount(),
                    "Expected no new ticks after pause but got "
                        + (CronTestJobs.tickCount() - ticksAtPause)
                        + " additional ticks"));
  }

  @Test
  void resumePausedRecurringJob_shouldRestartFiring() {
    JobHandle handle = scheduleTickingRecurring();

    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertTrue(
                    CronTestJobs.tickCount() >= 1,
                    "Expected at least 1 tick but got " + CronTestJobs.tickCount()));

    assertTrue(jobService.pauseJob(handle.id()));
    assertTrue(
        recurringJobStore.getRecurring(handle.id()).orElseThrow().paused(),
        "Expected recurring master to be paused");

    int ticksBeforeResume = CronTestJobs.tickCount();

    assertTrue(jobService.resumeJob(handle.id()), "Expected resumeJob to return true");

    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertTrue(
                    CronTestJobs.tickCount() > ticksBeforeResume,
                    "Expected new ticks after resume but count stayed at "
                        + CronTestJobs.tickCount()));
  }

  @Test
  void pauseAlreadyPausedRecurringJob_shouldBeIdempotent() {
    JobHandle handle = scheduleTickingRecurring();

    assertTrue(jobService.pauseJob(handle.id()), "Expected initial pauseJob to return true");
    assertTrue(jobService.pauseJob(handle.id()), "Expected repeated pauseJob to return true");

    assertTrue(
        recurringJobStore.getRecurring(handle.id()).orElseThrow().paused(),
        "Expected recurring master to be paused");
  }

  @Test
  void resumeActiveRecurringJob_shouldReturnFalse() {
    JobHandle handle = scheduleTickingRecurring();

    assertFalse(
        jobService.resumeJob(handle.id()), "Expected resumeJob on active recurring job to fail");

    assertFalse(
        recurringJobStore.getRecurring(handle.id()).orElseThrow().paused(),
        "Expected recurring master to remain active");
  }

  @Test
  void pauseResumeMissingJob_shouldReturnFalse() {
    UUID missingJobId = UUID.randomUUID();

    assertFalse(jobService.pauseJob(missingJobId), "Expected pauseJob on missing job to fail");
    assertFalse(jobService.resumeJob(missingJobId), "Expected resumeJob on missing job to fail");
  }

  private JobHandle scheduleTickingRecurring() {
    return jobService
        .scheduleRecurring("*/1 * * * * ?", ZoneOffset.UTC, CronTestJobs::tick)
        .submit();
  }
}

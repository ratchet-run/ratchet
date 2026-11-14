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
package run.ratchet.testsuite.resource;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.api.ResourceLimit;
import run.ratchet.spi.SchedulerLifecycleHook;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.ResourceTestJob;
import run.ratchet.testsuite.app.TestCleanupStrategy;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Sets a resource limit from {@link SchedulerLifecycleHook#beforeStart()}, as the docs recommend,
 * and submits a job that names it.
 *
 * <p>The runtime logs a failing hook as a warning and starts anyway, so the hook records its own
 * failure for the test to report. This class does not extend {@code BaseRatchetIT}: that class
 * truncates every table before each test, which would delete the limit set at start-up.
 */
@ExtendWith(ArquillianExtension.class)
class ResourceLimitStartupHookIT {

  static final String RESOURCE = "startup-hook-res";

  @Inject private JobSchedulerService scheduler;

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private TestCleanupStrategy cleanupStrategy;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(StartupLimitHook.class, ResourceTestJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @AfterEach
  void cleanup() {
    cleanupStrategy.truncateAll();
  }

  @Test
  void limitSetInBeforeStartIsReadyForTheFirstSubmit() {
    assertNull(
        StartupLimitHook.FAILURE.get(),
        () -> "setResourceLimit failed in beforeStart: " + StartupLimitHook.FAILURE.get());
    assertTrue(StartupLimitHook.RAN.get(), "beforeStart must run during deployment start-up");
    assertEquals(
        Optional.of(new ResourceLimit(RESOURCE, 1, Duration.ofMillis(200), "Set in beforeStart")),
        scheduler.findResourceLimit(RESOURCE));

    ResourceTestJob.reset();
    JobHandle handle = jobService.enqueue(ResourceTestJob::execute).withResource(RESOURCE).submit();

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(200))
        .until(() -> ResourceTestJob.getCompletedCount() >= 1);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);
  }

  /** Sets the limit the way the docs show. */
  @ApplicationScoped
  public static class StartupLimitHook implements SchedulerLifecycleHook {

    static final AtomicBoolean RAN = new AtomicBoolean();
    static final AtomicReference<Throwable> FAILURE = new AtomicReference<>();

    @Inject JobSchedulerService scheduler;

    @Override
    public void beforeStart() {
      RAN.set(true);
      try {
        scheduler.setResourceLimit(
            ResourceLimit.of(RESOURCE, 1)
                .withRetryDelay(Duration.ofMillis(200))
                .withDescription("Set in beforeStart"));
      } catch (RuntimeException e) {
        FAILURE.set(e);
        throw e;
      }
    }
  }
}

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
package run.ratchet.testsuite.executor;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.ExecutorProvider;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Negative-control test for the executor SPI default path.
 *
 * <p>The standard {@code addStoreInfrastructure()} archive includes no test override for {@link
 * ExecutorProvider}, so CDI must resolve the production default implementation from the {@code
 * ratchet} library jar. This guards against accidentally re-introducing a test-only executor
 * override that would mask container/JNDI portability bugs in the production class.
 *
 * <p>Verification is by behavior — submit work to both executors and confirm it runs — rather than
 * by class identity, so the test stays valid even if the default implementation is moved or renamed
 * within the framework's internal packages.
 */
class DefaultExecutorProviderIT extends BaseRatchetIT {

  @Inject private ExecutorProvider executorProvider;

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

  @BeforeEach
  void resetJobCounter() {
    SimpleJob.resetCount();
  }

  @Test
  void cdiResolvesAnExecutorProvider() {
    assertNotNull(executorProvider, "ExecutorProvider must be CDI-resolvable");
    // Behavior, not identity: the production default must wire up and produce working executors.
    assertNotNull(
        executorProvider.getJobExecutor(),
        "Default ExecutorProvider must supply a job executor (no test override registered).");
    assertNotNull(
        executorProvider.getScheduledExecutor(),
        "Default ExecutorProvider must supply a scheduled executor (no test override registered).");
  }

  @Test
  void jobExecutorIsUsableAndCachedAcrossCalls() throws InterruptedException {
    ExecutorService first = executorProvider.getJobExecutor();
    assertNotNull(first, "job executor must resolve");
    assertSame(first, executorProvider.getJobExecutor(), "executor reference must be cached");

    // Lifecycle methods (isShutdown / shutdown) are forbidden by Jakarta Concurrency on managed
    // executors, so prove liveness by actually executing a task.
    CountDownLatch latch = new CountDownLatch(1);
    first.execute(latch::countDown);
    assertTrue(
        latch.await(5, TimeUnit.SECONDS),
        "container-managed job executor must run submitted tasks");
  }

  @Test
  void scheduledExecutorIsUsableAndCachedAcrossCalls() throws InterruptedException {
    ScheduledExecutorService first = executorProvider.getScheduledExecutor();
    assertNotNull(first, "scheduled executor must resolve");
    assertSame(first, executorProvider.getScheduledExecutor(), "executor reference must be cached");

    CountDownLatch latch = new CountDownLatch(1);
    first.schedule(latch::countDown, 10, TimeUnit.MILLISECONDS);
    assertTrue(
        latch.await(5, TimeUnit.SECONDS),
        "container-managed scheduled executor must run scheduled tasks");
  }
}

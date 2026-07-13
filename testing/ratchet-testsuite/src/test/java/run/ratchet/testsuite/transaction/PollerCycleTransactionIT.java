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

import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import run.ratchet.api.JobHandle;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.internal.PollerCycleExecutor;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.app.TestTransactionRunner;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.PollerControl;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Proves that a poll cycle suspends an inherited or otherwise ambient JTA transaction.
 *
 * <p>The claim store uses {@code REQUIRED}. Without the {@code NOT_SUPPORTED} boundary on {@link
 * PollerCycleExecutor}, the claim joins the caller transaction and the worker is dispatched before
 * that transaction commits. The worker then sees the previously committed {@code PENDING} row and
 * skips execution.
 */
@EnabledIfSystemProperty(
    named = "ratchet.test.db.type",
    matches = "mysql|postgresql|oracle|sqlserver")
class PollerCycleTransactionIT extends BaseRatchetIT {

  private static final Duration EXECUTION_TIMEOUT = Duration.ofSeconds(10);

  @Inject private JobCrudStore jobCrudStore;
  @Inject private PollerCycleExecutor pollerCycleExecutor;
  @Inject private PollerScheduler pollerScheduler;
  @Inject private TestJobService jobService;
  @Inject private TestTransactionRunner txRunner;

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

  @Override
  protected void truncateAll() throws Exception {
    PollerControl.stopAndAwait(pollerScheduler);
    super.truncateAll();
  }

  @BeforeEach
  void resetJobs() {
    SimpleJob.resetCount();
  }

  @Test
  void pollCycle_commitsClaimBeforeDispatch_whileCallerTransactionRemainsActive() {
    JobHandle handle = txRunner.call(() -> jobService.enqueue(SimpleJob::execute).submit());

    txRunner.runRollbackOnly(
        () -> {
          pollerCycleExecutor.tick();
          await()
              .atMost(EXECUTION_TIMEOUT)
              .untilAsserted(() -> assertEquals(1, SimpleJob.getInvocationCount()));
        });

    JobAssertions.assertJobCompleted(jobCrudStore, handle, EXECUTION_TIMEOUT);
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobStore;
import run.ratchet.testsuite.app.CheckpointJob;
import run.ratchet.testsuite.app.CheckpointTransactions;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** The hard watchdog saves a committed checkpoint before scheduling the retry. */
class CheckpointHardTimeoutIT extends BaseRatchetIT {
  @Inject private TestJobService jobs;
  @Inject private JobStore store;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(System.getProperty("testsuite.profile", "wildfly-managed"), dbType)
        .addClasses(CheckpointJob.class, CheckpointTransactions.class, TestJobService.class)
        .addStoreInfrastructure()
        .addResource(
            RatchetArchiveBuilder.testRuntimeConfigContent(
                dbType,
                Map.of(
                    "ratchet.execution.checkpoint-flush-interval-ms", "600000",
                    "ratchet.timeout.cancellation-grace-seconds", "0",
                    "ratchet.timeout.default-sla-seconds", "3")),
            "ratchet-testsuite.properties")
        .addBeansXml()
        .build();
  }

  @Test
  void hardTimeoutPreservesCheckpointForRetryWithoutBackgroundFlush() {
    CheckpointJob.reset();
    JobHandle handle =
        jobs.enqueue(CheckpointJob::hardTimeout)
            .withTimeout(Duration.ofSeconds(3))
            .withMaxRetries(1)
            .withBackoff(BackoffPolicy.FIXED, Duration.ofMillis(100))
            .submit();
    JobAssertions.assertJobCompleted(store, handle);
    assertEquals(
        List.of(Optional.empty(), Optional.of("page-7")), CheckpointJob.OBSERVED.get(handle.id()));
  }
}

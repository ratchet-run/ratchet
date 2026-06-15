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
import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobStatus;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.BatchItemProcessor;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Validates that batch completion triggers workflow branches (thenOnBatchSuccess). */
class BatchCompletionCallbackIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(BatchItemProcessor.class, SimpleJob.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetTrackers() {
    BatchItemProcessor.reset();
    SimpleJob.resetCount();
  }

  @Test
  void batchWithSuccessCallback_shouldExecuteCallbackAfterBatchCompletes() {
    List<String> items = List.of("x", "y");

    JobHandle handle =
        jobService
            .enqueueBatch("callback-batch")
            .forEach(items, BatchItemProcessor::process)
            .thenOnBatchSuccess(SimpleJob::execute)
            .submit();

    // Wait for the batch parent to succeed
    JobAssertions.assertBatchSucceeded(jobCrudStore, handle, Duration.ofSeconds(30));

    // The success callback should eventually fire
    JobAssertions.assertJobStatus(jobCrudStore, handle, JobStatus.SUCCEEDED);

    assertEquals(2, BatchItemProcessor.processedCount());
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertEquals(
                    1, SimpleJob.getInvocationCount(), "Success callback should have fired once"));
  }
}

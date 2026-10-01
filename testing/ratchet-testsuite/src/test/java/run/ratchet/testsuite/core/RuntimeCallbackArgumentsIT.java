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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.inject.Inject;
import java.time.Duration;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.RuntimeCallbackJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

class RuntimeCallbackArgumentsIT extends BaseRatchetIT {
  @Inject private TestJobService jobService;
  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");
    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            RuntimeCallbackJob.class, RuntimeCallbackJob.Failure.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  @AfterEach
  void resetRecorder() {
    RuntimeCallbackJob.reset();
  }

  @Test
  void callbacksReceiveContextAndFinalFailureAfterPersistence() {
    JobHandle success =
        jobService
            .enqueue(RuntimeCallbackJob::succeed)
            .onSuccess(RuntimeCallbackJob::onSuccess)
            .submit();
    JobHandle failure =
        jobService
            .enqueue(RuntimeCallbackJob::fail)
            .withMaxRetries(0)
            .onFailure((ctx, error) -> RuntimeCallbackJob.onFailure(ctx, error))
            .submit();
    JobAssertions.assertJobCompleted(jobCrudStore, success);
    JobAssertions.assertJobFailed(jobCrudStore, failure);
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              assertNotNull(RuntimeCallbackJob.success(success.id()));
              assertEquals(success.id(), RuntimeCallbackJob.success(success.id()).jobId());
              RuntimeCallbackJob.Failure recorded = RuntimeCallbackJob.failure(failure.id());
              assertNotNull(recorded);
              assertEquals(failure.id(), recorded.context().jobId());
              assertInstanceOf(IllegalStateException.class, recorded.error());
              assertEquals("callback integration failure", recorded.error().getMessage());
            });
  }
}

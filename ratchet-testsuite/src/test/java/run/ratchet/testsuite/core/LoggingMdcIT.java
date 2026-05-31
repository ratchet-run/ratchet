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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.MdcCapturingJob;
import run.ratchet.testsuite.app.StubCallerPrincipalProvider;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Verifies that JobMdcContext populates the stable MDC keys during job execution. */
class LoggingMdcIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(MdcCapturingJob.class, StubCallerPrincipalProvider.class, TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetCapture() {
    MdcCapturingJob.reset();
  }

  @Test
  void mdcKeysArePopulatedDuringJobExecution() {
    JobHandle handle = jobService.enqueueNow(MdcCapturingJob::execute);

    assertNotNull(handle);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    assertCapturedMdc(handle);
  }

  @Test
  void mdcKeysArePopulatedWhenJobFails() {
    JobHandle handle =
        jobService.enqueue(MdcCapturingJob::executeAndFail).withMaxRetries(0).submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    assertCapturedMdc(handle);
  }

  @Test
  void mdcKeysArePopulatedWhenJobTimesOut() {
    MdcCapturingJob.setSleepMs(60_000);

    JobHandle handle =
        jobService
            .enqueue(MdcCapturingJob::executeSlow)
            .withTimeout(Duration.ofSeconds(1))
            .submit();

    assertNotNull(handle);
    JobAssertions.assertJobFailed(jobCrudStore, handle);

    assertCapturedMdc(handle);
  }

  @Test
  void mdcKeysArePopulatedWhenRunningJobIsCanceled() {
    MdcCapturingJob.setSleepMs(5_000);

    JobHandle handle = jobService.enqueueNow(MdcCapturingJob::executeSlow);

    assertNotNull(handle);
    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .until(MdcCapturingJob::hasStarted);

    assertTrue(jobService.cancelJob(handle.id()), "Should be able to cancel a running job");
    JobAssertions.assertJobCanceled(jobCrudStore, handle);

    assertCapturedMdc(handle);
  }

  private void assertCapturedMdc(JobHandle handle) {
    Map<String, Object> captured = MdcCapturingJob.getCapturedMdc();
    assertNotNull(captured, "MdcCapturingJob did not run — no MDC snapshot captured");

    // jobId is always populated.
    Object jobIdValue = captured.get("jobId");
    assertNotNull(jobIdValue, "jobId MDC key missing during job execution. Captured: " + captured);
    assertEquals(
        String.valueOf(handle.id()),
        String.valueOf(jobIdValue),
        "jobId MDC value should match the submitted job's ID");

    // node is populated from NodeIdentityProvider; we don't pin a specific value but it must be
    // non-null.
    assertTrue(
        captured.containsKey("node") && captured.get("node") != null,
        "node MDC key missing or null during job execution. Captured: " + captured);

    // jobType is populated from JobEntity.getPublicJobType().name(); single jobs = "SINGLE".
    Object jobTypeValue = captured.get("jobType");
    assertNotNull(
        jobTypeValue, "jobType MDC key missing during job execution. Captured: " + captured);
    assertEquals(
        "SINGLE",
        String.valueOf(jobTypeValue),
        "jobType MDC key should be SINGLE for a directly-enqueued job. Captured: " + captured);

    // jobCreator is populated from CallerPrincipalProvider (callerPrincipal) at enqueue time.
    // StubCallerPrincipalProvider is in this deployment and returns "it-caller".
    assertEquals(
        StubCallerPrincipalProvider.STUB_PRINCIPAL,
        String.valueOf(captured.get("jobCreator")),
        "jobCreator MDC key should match the CallerPrincipalProvider value. Captured: " + captured);
  }
}

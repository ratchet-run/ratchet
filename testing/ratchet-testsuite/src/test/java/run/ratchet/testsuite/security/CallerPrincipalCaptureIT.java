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
package run.ratchet.testsuite.security;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import jakarta.inject.Inject;
import java.time.Duration;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.ri.security.CallerPrincipalProvider;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.CallerPrincipalInheritanceJob;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.StubCallerPrincipalProvider;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Verifies that the framework stamps the captured caller principal onto every persisted {@link
 * JobEntity} at creation.
 *
 * <p>This deployment does NOT configure a real security realm — a realm-configured Arquillian test
 * is deferred follow-up work. Instead, a {@link StubCallerPrincipalProvider}
 * {@code @Alternative @Priority(1)} replaces the default {@link CallerPrincipalProvider} in the CDI
 * graph and returns a fixed principal, proving the framework invokes the provider during job
 * creation and persists the returned value.
 */
class CallerPrincipalCaptureIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;

  @Inject private JobCrudStore jobCrudStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(
            StubCallerPrincipalProvider.class,
            SimpleJob.class,
            CallerPrincipalInheritanceJob.class,
            TestJobService.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @BeforeEach
  void resetFixtures() {
    CallerPrincipalInheritanceJob.reset();
  }

  @Test
  void scheduledJob_stampsCallerPrincipalFromProviderBeforeExecution() {
    JobHandle handle = jobService.schedule(Duration.ofSeconds(2), SimpleJob::execute).submit();

    assertNotNull(handle);
    JobEntity beforeExecution =
        jobCrudStore
            .findById(handle.id())
            .orElseThrow(() -> new AssertionError("Job not found after submit"));

    assertEquals(
        StubCallerPrincipalProvider.STUB_PRINCIPAL,
        beforeExecution.getCallerPrincipal(),
        "Framework MUST persist the principal returned by CallerPrincipalProvider at job creation");

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    JobEntity afterExecution =
        jobCrudStore
            .findById(handle.id())
            .orElseThrow(() -> new AssertionError("Job not found after completion"));

    assertEquals(
        beforeExecution.getCallerPrincipal(),
        afterExecution.getCallerPrincipal(),
        "Execution must not overwrite the caller principal stamped at job creation");
  }

  @Test
  void runtimeChildSubmittedOnWorkerThread_inheritsParentCallerPrincipal() {
    JobHandle parent =
        jobService.enqueueNow(CallerPrincipalInheritanceJob::submitChildOnWorkerThread);

    JobAssertions.assertJobCompleted(jobCrudStore, parent);
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> CallerPrincipalInheritanceJob.childId() != null);

    UUID childId = CallerPrincipalInheritanceJob.childId();
    JobEntity child =
        jobCrudStore
            .findById(childId)
            .orElseThrow(() -> new AssertionError("Child job not found after runtime submit"));

    assertEquals(
        StubCallerPrincipalProvider.STUB_PRINCIPAL,
        child.getCallerPrincipal(),
        "Child submissions made on the worker thread must inherit the parent's captured principal");
    JobAssertions.assertJobCompleted(jobCrudStore, () -> childId);
  }

  @Test
  void onSuccessCallbackRunsInsideJobContextBindWindow() {
    JobHandle handle =
        jobService
            .enqueue(CallerPrincipalInheritanceJob::succeed)
            .onSuccess(ctx -> CallerPrincipalInheritanceJob.captureSuccessCallbackContext())
            .submit();

    JobAssertions.assertJobCompleted(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertEquals(
                    StubCallerPrincipalProvider.STUB_PRINCIPAL,
                    CallerPrincipalInheritanceJob.successCallbackPrincipal()));
  }

  @Test
  void onFailureCallbackRunsInsideJobContextBindWindow() {
    JobHandle handle =
        jobService
            .enqueue(CallerPrincipalInheritanceJob::fail)
            .withMaxRetries(0)
            .onFailure(
                (ctx, failure) -> CallerPrincipalInheritanceJob.captureFailureCallbackContext())
            .submit();

    JobAssertions.assertJobFailed(jobCrudStore, handle);

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertEquals(
                    StubCallerPrincipalProvider.STUB_PRINCIPAL,
                    CallerPrincipalInheritanceJob.failureCallbackPrincipal()));
  }
}

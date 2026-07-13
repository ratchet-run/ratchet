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
package run.ratchet.testsuite.capability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.inject.Inject;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobHandle;
import run.ratchet.api.JobSchedulerService;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.WorkflowConditionStore;
import run.ratchet.testsuite.app.SimpleJob;
import run.ratchet.testsuite.app.TestJobService;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.CoreOnlyStoreExtension;
import run.ratchet.testsuite.util.JobAssertions;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Deploys the reference engine into a real Jakarta EE container against a store that advertises
 * only the mandatory core contract, and proves the deployment boots and runs.
 *
 * <p>{@link CoreOnlyStoreExtension} strips every optional capability interface from the store
 * bean's type set, so each {@code @Inject Instance<Cap>} the engine holds is unsatisfied. The
 * container accepts the deployment only because no capability leaked into a hard {@code @Inject} —
 * a leak would surface here as an unsatisfied-dependency deployment failure rather than as a quiet
 * runtime guard. A core job then runs end to end with every capability disabled.
 */
class CoreOnlyStoreDeploymentIT extends BaseRatchetIT {

  @Inject private TestJobService jobService;
  @Inject private JobCrudStore jobCrudStore;
  @Inject private JobSchedulerService scheduler;

  @Inject private Instance<RecurringJobStore> recurringStore;
  @Inject private Instance<BatchStore> batchStore;
  @Inject private Instance<WorkflowConditionStore> workflowConditionStore;
  @Inject private Instance<SignalStore> signalStore;
  @Inject private Instance<ResourcePermitStore> resourcePermitStore;
  @Inject private Instance<LockStore> lockStore;
  @Inject private Instance<ArchiveStore> archiveStore;
  @Inject private Instance<JobQueryStore> queryStore;
  @Inject private Instance<JobAnalyticsStore> analyticsStore;
  @Inject private Instance<JobAuditStore> auditStore;
  @Inject private Instance<JobExtensionStore> jobExtensionStore;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addClasses(SimpleJob.class, TestJobService.class, CoreOnlyStoreExtension.class)
        .addStoreInfrastructure()
        .addBeansXml()
        .build()
        .addAsServiceProvider(Extension.class, CoreOnlyStoreExtension.class);
  }

  @Test
  void deploymentBootsWithEveryOptionalCapabilityUnsatisfied() {
    // The container booted (this test runs at all), and the core scheduler resolves.
    assertNotNull(scheduler, "the core scheduler service must resolve on a core-only store");

    // Every optional capability is genuinely absent from the bean graph.
    assertTrue(recurringStore.isUnsatisfied(), "RecurringJobStore must be unsatisfied");
    assertTrue(batchStore.isUnsatisfied(), "BatchStore must be unsatisfied");
    assertTrue(
        workflowConditionStore.isUnsatisfied(), "WorkflowConditionStore must be unsatisfied");
    assertTrue(signalStore.isUnsatisfied(), "SignalStore must be unsatisfied");
    assertTrue(resourcePermitStore.isUnsatisfied(), "ResourcePermitStore must be unsatisfied");
    assertTrue(lockStore.isUnsatisfied(), "LockStore must be unsatisfied");
    assertTrue(archiveStore.isUnsatisfied(), "ArchiveStore must be unsatisfied");
    assertTrue(queryStore.isUnsatisfied(), "JobQueryStore must be unsatisfied");
    assertTrue(analyticsStore.isUnsatisfied(), "JobAnalyticsStore must be unsatisfied");
    assertTrue(auditStore.isUnsatisfied(), "JobAuditStore must be unsatisfied");
    assertTrue(jobExtensionStore.isUnsatisfied(), "JobExtensionStore must be unsatisfied");
  }

  @Test
  void coreJobRunsEndToEndWithoutCapabilities() {
    JobHandle handle = jobService.enqueueNow(SimpleJob::execute);

    assertNotNull(handle);
    JobAssertions.assertJobCompleted(jobCrudStore, handle);
  }

  @Test
  void capabilityDependentSubmissionsFailFastAndCancellationsNoOp() {
    // A recurring-cancel by business key has no RecurringJobStore to consult on a core-only store,
    // so it reports zero cancellations rather than dereferencing the absent capability.
    assertEquals(
        0,
        scheduler.cancelRecurringJobByBusinessKey("no-such-key"),
        "cancelRecurringJobByBusinessKey must no-op to 0 when RecurringJobStore is absent");

    // A job that asks for resource-permit gating cannot be honored without the ResourcePermitStore
    // capability, so the submission is rejected rather than silently running with unbounded
    // concurrency.
    assertThrows(
        UnsupportedOperationException.class,
        () -> jobService.enqueue(SimpleJob::execute).withResource("db-pool").immediate().submit(),
        "a resource-gated submission must fail fast when ResourcePermitStore is absent");
  }
}

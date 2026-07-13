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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;
import run.ratchet.api.JobStatus;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ArchiveStore;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.store.spi.JobExtensionStore;
import run.ratchet.store.spi.JobQueryStore;
import run.ratchet.store.spi.JobStore;
import run.ratchet.store.spi.LockStore;
import run.ratchet.store.spi.NodeStore;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.SignalStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.store.spi.WorkflowConditionStore;

/**
 * Shared conformance contract for a store exposing only Ratchet's mandatory core capabilities.
 *
 * <p>Concrete store suites supply a real store fixture wrapped by {@link CoreOnlyStoreView}. The
 * contract proves that crash recovery remains available while optional capability probes and their
 * contract accessors report the capability as not applicable.
 */
public abstract class AbstractCoreOnlyStoreContract {

  private static final List<Class<?>> OPTIONAL_CAPABILITIES =
      List.of(
          RecurringJobStore.class,
          BatchStore.class,
          WorkflowConditionStore.class,
          SignalStore.class,
          ResourcePermitStore.class,
          LockStore.class,
          ArchiveStore.class,
          JobQueryStore.class,
          JobAnalyticsStore.class,
          JobAuditStore.class,
          JobExtensionStore.class);

  protected abstract JobStoreContractFixture fixture();

  @BeforeEach
  @AfterEach
  protected void cleanStore() {
    fixture().cleanupStore();
  }

  @Test
  protected void capabilityProbeAdvertisesCoreAndHidesOptionalCapabilities() {
    JobStore store = fixture().store();

    assertTrue(
        store.capability(JobCrudStore.class).isPresent(), "core CRUD must always be advertised");
    assertTrue(
        store.capability(NodeStore.class).isPresent(),
        "node heartbeat / crash recovery must always be advertised");
    assertTrue(
        store.capability(TagStore.class).isPresent(), "tag writes must always be advertised");

    for (Class<?> capability : OPTIONAL_CAPABILITIES) {
      assertTrue(
          store.capability(capability).isEmpty(),
          () -> capability.getSimpleName() + " must report absent on a core-only store");
    }
  }

  @Test
  protected void orphanRecoveryResetsRunningJobsWithoutAnyOptionalCapability() {
    JobStoreContractFixture fixture = fixture();
    JobEntity job = fixture.persist(fixture.newPendingJob());
    job.setStatus(JobStatus.RUNNING);
    job.setPickedBy("phantom-node-" + job.getId());
    job.setPickedAt(Instant.now().minusSeconds(45));
    fixture.store().save(job);

    int reset = fixture.store().resetOrphanJobs(Duration.ofSeconds(15));

    assertTrue(reset >= 1, "a job picked 45s ago with a 15s grace should be reclaimed");
    assertEquals(
        JobStatus.PENDING,
        fixture.store().findById(job.getId()).orElseThrow().getStatus(),
        "crash recovery must run on a store that advertises only core");
  }

  @Test
  protected void capabilityContractsSkipWhenCapabilityAbsent() {
    JobStoreContractFixture fixture = fixture();

    assertThrows(TestAbortedException.class, fixture::batchStore);
    assertThrows(TestAbortedException.class, fixture::signalStore);
    assertThrows(TestAbortedException.class, fixture::archiveStore);
    assertThrows(TestAbortedException.class, fixture::recurringStore);
  }
}

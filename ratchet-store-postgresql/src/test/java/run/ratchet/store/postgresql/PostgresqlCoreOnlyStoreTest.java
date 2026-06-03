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
package run.ratchet.store.postgresql;

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
import run.ratchet.store.spi.DlqAlertStore;
import run.ratchet.store.spi.JobAnalyticsStore;
import run.ratchet.store.spi.JobAuditStore;
import run.ratchet.store.spi.JobCrudStore;
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
 * Proves a store that advertises only the mandatory core contract still satisfies the engine's
 * correctness floor. The backing store is a real PostgreSQL implementation viewed through {@link
 * run.ratchet.tck.store.CoreOnlyStoreView}, so core lifecycle calls run for real while every
 * optional capability reports absent.
 */
class PostgresqlCoreOnlyStoreTest {

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
          DlqAlertStore.class);

  private final PostgresqlCoreOnlyTestFixture fixture = new PostgresqlCoreOnlyTestFixture();

  @BeforeEach
  @AfterEach
  void clean() {
    fixture.cleanupStore();
  }

  @Test
  void capabilityProbe_advertisesCoreAndHidesOptionalCapabilities() {
    JobStore store = fixture.store();

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
  void orphanRecovery_resetsRunningJobsWithoutAnyCapability() {
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
  void capabilityContracts_skipWhenCapabilityAbsent() {
    // The fixture's capability accessors abort (JUnit skip / conformance N/A) rather than fail when
    // the store does not advertise the capability.
    assertThrows(TestAbortedException.class, fixture::batchStore);
    assertThrows(TestAbortedException.class, fixture::signalStore);
    assertThrows(TestAbortedException.class, fixture::archiveStore);
    assertThrows(TestAbortedException.class, fixture::recurringStore);
  }
}

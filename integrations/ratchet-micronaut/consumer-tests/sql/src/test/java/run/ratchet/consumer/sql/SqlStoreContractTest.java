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
package run.ratchet.consumer.sql;

import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import run.ratchet.consumer.MicronautStoreContracts;
import run.ratchet.ri.core.RatchetRuntime;
import run.ratchet.store.converter.RuntimeContextInstallation;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractActiveBusinessKeyContract;
import run.ratchet.tck.store.AbstractArchiveStoreContract;
import run.ratchet.tck.store.AbstractBatchStoreContract;
import run.ratchet.tck.store.AbstractDualWriteInvariantContract;
import run.ratchet.tck.store.AbstractJobAnalyticsStoreContract;
import run.ratchet.tck.store.AbstractJobAuditStoreContract;
import run.ratchet.tck.store.AbstractJobBatchStatusStoreContract;
import run.ratchet.tck.store.AbstractJobBulkStoreContract;
import run.ratchet.tck.store.AbstractJobClaimStoreContract;
import run.ratchet.tck.store.AbstractJobCrudStoreContract;
import run.ratchet.tck.store.AbstractJobPauseStoreContract;
import run.ratchet.tck.store.AbstractJobPropertyStoreContract;
import run.ratchet.tck.store.AbstractJobQueryStoreContract;
import run.ratchet.tck.store.AbstractJobRetryStoreContract;
import run.ratchet.tck.store.AbstractJobStateStoreContract;
import run.ratchet.tck.store.AbstractJobTerminalStoreContract;
import run.ratchet.tck.store.AbstractLockStoreContract;
import run.ratchet.tck.store.AbstractNodeStoreContract;
import run.ratchet.tck.store.AbstractResourcePermitStoreContract;
import run.ratchet.tck.store.AbstractSignalStoreContract;
import run.ratchet.tck.store.AbstractTagStoreContract;
import run.ratchet.tck.store.AbstractWorkflowConditionStoreContract;
import run.ratchet.tck.store.JobStoreContractFixture;

/** Existing store TCK contracts exercised through Ratchet's Micronaut transactional store proxy. */
class SqlStoreContractTest {
  private static SqlDatabase database;
  private static ApplicationContext application;
  private static final MicronautStoreContracts FIXTURE =
      new MicronautStoreContracts() {
        @Override
        protected ApplicationContext context() {
          return application;
        }

        @Override
        protected void resetStore() {
          SqlAccess jdbc = new SqlAccess(application);
          jdbc.transaction(
              status -> {
                for (String table : SqlApplication.TABLES) {
                  jdbc.update("DELETE FROM " + table);
                }
              });
        }
      };

  @BeforeAll
  static void start() {
    database = SqlDatabase.start();
    application = SqlApplication.start(database, "consumer.store-contract=true");
    // Store contracts drive the store directly and install their own converter configuration,
    // so stop the engine and release the runtime's claim, as the Spring lane does.
    application.getBean(RatchetRuntime.class).close();
    application.getBean(RuntimeContextInstallation.class).close();
  }

  @AfterAll
  static void stop() {
    if (application != null) {
      application.close();
    }
    if (database != null) {
      database.close();
    }
  }

  private interface StoreFixture extends JobStoreContractFixture {
    default MicronautStoreContracts fixture() {
      return FIXTURE;
    }

    @Override
    default JobStore store() {
      return fixture().store();
    }

    @Override
    default JobEntity newPendingJob() {
      return fixture().newPendingJob();
    }

    @Override
    default JobEntity newBatchParentJob() {
      return fixture().newBatchParentJob();
    }

    @Override
    default void cleanupStore() {
      fixture().cleanupStore();
    }

    @Override
    default boolean isStaleWriteException(Throwable failure) {
      return fixture().isStaleWriteException(failure);
    }
  }

  @Nested
  class Crud extends AbstractJobCrudStoreContract implements StoreFixture {}

  @Nested
  class Claim extends AbstractJobClaimStoreContract implements StoreFixture {}

  @Nested
  class Retry extends AbstractJobRetryStoreContract implements StoreFixture {}

  @Nested
  class Terminal extends AbstractJobTerminalStoreContract implements StoreFixture {}

  @Nested
  class Bulk extends AbstractJobBulkStoreContract implements StoreFixture {}

  @Nested
  class Pause extends AbstractJobPauseStoreContract implements StoreFixture {}

  @Nested
  class Batch extends AbstractBatchStoreContract implements StoreFixture {}

  @Nested
  class BatchStatus extends AbstractJobBatchStatusStoreContract implements StoreFixture {}

  @Nested
  class Workflow extends AbstractWorkflowConditionStoreContract implements StoreFixture {}

  @Nested
  class Signal extends AbstractSignalStoreContract implements StoreFixture {}

  @Nested
  class Idempotency extends AbstractActiveBusinessKeyContract implements StoreFixture {}

  @Nested
  class Properties extends AbstractJobPropertyStoreContract implements StoreFixture {}

  @Nested
  class State extends AbstractJobStateStoreContract implements StoreFixture {}

  @Nested
  class Query extends AbstractJobQueryStoreContract implements StoreFixture {}

  @Nested
  class Tags extends AbstractTagStoreContract implements StoreFixture {}

  @Nested
  class Permits extends AbstractResourcePermitStoreContract implements StoreFixture {}

  @Nested
  class Locks extends AbstractLockStoreContract implements StoreFixture {}

  @Nested
  class Nodes extends AbstractNodeStoreContract implements StoreFixture {}

  @Nested
  class Archive extends AbstractArchiveStoreContract implements StoreFixture {}

  @Nested
  class Analytics extends AbstractJobAnalyticsStoreContract implements StoreFixture {}

  @Nested
  class Audit extends AbstractJobAuditStoreContract implements StoreFixture {}

  @Nested
  class DualWrite extends AbstractDualWriteInvariantContract implements StoreFixture {}
}

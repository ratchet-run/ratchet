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

import example.ratchet.ConsumerApplication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import run.ratchet.consumer.SpringStoreContracts;
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
import run.ratchet.tck.store.AbstractJobExtensionStoreContract;
import run.ratchet.tck.store.AbstractJobPauseStoreContract;
import run.ratchet.tck.store.AbstractJobQueryStoreContract;
import run.ratchet.tck.store.AbstractJobRetryStoreContract;
import run.ratchet.tck.store.AbstractJobTerminalStoreContract;
import run.ratchet.tck.store.AbstractLockStoreContract;
import run.ratchet.tck.store.AbstractNodeStoreContract;
import run.ratchet.tck.store.AbstractResourcePermitStoreContract;
import run.ratchet.tck.store.AbstractSignalStoreContract;
import run.ratchet.tck.store.AbstractTagStoreContract;
import run.ratchet.tck.store.AbstractWorkflowConditionStoreContract;
import run.ratchet.tck.store.JobStoreContractFixture;

/** Existing store TCK contracts exercised through Ratchet's Spring transactional store proxy. */
class SqlStoreContractTest {
  private static final String[] TABLES = {
    "scheduler_idempotency_key",
    "scheduler_business_key_reservation",
    "scheduler_job_queue",
    "scheduler_job_tag",
    "scheduler_job_log",
    "scheduler_job_execution",
    "scheduler_resource_permit",
    "scheduler_workflow_condition",
    "scheduler_batch_metrics",
    "scheduler_job_archive",
    "scheduler_job_properties",
    "scheduler_job_extension_state",
    "scheduler_job",
    "scheduler_recurring_job_archive",
    "scheduler_recurring_job",
    "scheduler_batch",
    "scheduler_resource_limit",
    "scheduler_lock",
    "scheduler_node"
  };

  private static SqlDatabase database;
  private static ConfigurableApplicationContext application;
  private static final SpringStoreContracts FIXTURE =
      new SpringStoreContracts() {
        @Override
        protected ConfigurableApplicationContext context() {
          return application;
        }

        @Override
        protected void resetStore() {
          JdbcTemplate jdbc = application.getBean(JdbcTemplate.class);
          new TransactionTemplate(application.getBean(PlatformTransactionManager.class))
              .executeWithoutResult(
                  status -> {
                    for (String table : TABLES) {
                      jdbc.update("DELETE FROM " + table);
                    }
                  });
        }
      };

  @BeforeAll
  static void start() {
    database = SqlDatabase.start();
    application =
        new SpringApplicationBuilder(ConsumerApplication.class)
            .properties(database.properties())
            .run();
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
    default SpringStoreContracts fixture() {
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
  class Extension extends AbstractJobExtensionStoreContract implements StoreFixture {}

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

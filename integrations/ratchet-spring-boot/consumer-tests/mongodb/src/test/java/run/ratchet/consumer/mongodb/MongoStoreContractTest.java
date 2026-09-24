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
package run.ratchet.consumer.mongodb;

import example.ratchet.mongo.MongoConsumerApplication;
import example.ratchet.mongo.MongoConsumerProperties;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.testcontainers.mongodb.MongoDBContainer;
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

/** Existing store TCK contracts exercised through the Spring MongoDB store bean. */
class MongoStoreContractTest {
  private static MongoDBContainer database;
  private static ConfigurableApplicationContext application;
  private static final SpringStoreContracts FIXTURE =
      new SpringStoreContracts() {
        @Override
        protected ConfigurableApplicationContext context() {
          return application;
        }

        @Override
        protected void resetStore() {
          MongoTemplate mongo = application.getBean(MongoTemplate.class);
          mongo
              .getDb()
              .listCollectionNames()
              .forEach(name -> mongo.getDb().getCollection(name).deleteMany(new Document()));
        }

        @Override
        public boolean supportsTransactionalRollback() {
          return false;
        }
      };

  @BeforeAll
  static void start() {
    database = MongoDatabase.create();
    database.start();
    application =
        new SpringApplicationBuilder(MongoConsumerApplication.class).properties(properties()).run();
    application.getBean(RatchetRuntime.class).close();
    application.getBean(RuntimeContextInstallation.class).close();
  }

  @AfterAll
  static void stop() {
    if (application != null) {
      application.close();
    }
    if (database != null) {
      database.stop();
    }
  }

  private static Map<String, Object> properties() {
    Map<String, Object> properties =
        new LinkedHashMap<>(
            MongoConsumerProperties.connection(
                database.getReplicaSetUrl("ratchet_store_contract")));
    properties.put("ratchet.allowed-packages", "example.ratchet.mongo,run.ratchet.tck.store");
    return properties;
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
    default boolean supportsTransactionalRollback() {
      return fixture().supportsTransactionalRollback();
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

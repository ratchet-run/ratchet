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
package run.ratchet.testsuite.transaction;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.inject.Inject;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.BatchRecoveryService;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestTransactionRunner;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/** Proves the CDI batch-recovery collaborator is invoked through its REQUIRES_NEW interceptor. */
@EnabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mysql|postgresql")
class BatchRecoveryRequiresNewIT extends BaseRatchetIT {

  @Inject private BatchRecoveryService batchRecoveryService;

  @Inject private BatchStore batchStore;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private UserTransaction utx;

  @Inject private TransactionSynchronizationRegistry transactionRegistry;

  @Inject private TestTransactionRunner txRunner;

  @Deployment
  public static WebArchive createDeployment() {
    String dbType = System.getProperty("ratchet.test.db.type", "mysql");
    String profile = System.getProperty("testsuite.profile", "wildfly-managed");

    return RatchetArchiveBuilder.create()
        .addRatchetDependencies(profile, dbType)
        .addStoreInfrastructure()
        .addBeansXml()
        .build();
  }

  @Test
  void recoveryCollaboratorSuspendsCallerAndRunsInNewTransaction() throws Exception {
    UUID batchId = txRunner.call(this::seedCompletedBatch);
    AtomicReference<Object> recoveryTransaction = new AtomicReference<>();

    utx.begin();
    try {
      Object callerTransaction = transactionRegistry.getTransactionKey();
      assertNotNull(callerTransaction);

      assertTrue(
          batchRecoveryService.recoverCompletedBatch(
              batchId,
              () -> {
                recoveryTransaction.set(transactionRegistry.getTransactionKey());
                return true;
              }));

      assertNotNull(recoveryTransaction.get());
      assertNotEquals(callerTransaction, recoveryTransaction.get());
    } finally {
      utx.rollback();
    }
  }

  private UUID seedCompletedBatch() {
    JobEntity parent = jobCrudStore.save(batchParentJob());
    BatchEntity batch = new BatchEntity();
    batch.setId(parent.getId());
    batch.setTotalItems(1);
    batch.setCompletedItems(1);
    batch.setFailedItems(0);
    batch.setCompletionProcessed(false);
    batchStore.saveBatch(batch);
    return parent.getId();
  }

  private static JobEntity batchParentJob() {
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.BATCH_PARENT);
    job.setStatus(JobStatus.PENDING);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    job.setScheduledTime(Instant.now().plus(Duration.ofHours(1)));
    job.setPayload(JobPayloadFactory.noop());
    job.setIdempotencyKey(UUID.randomUUID().toString());
    return job;
  }
}

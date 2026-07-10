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

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.inject.Inject;
import jakarta.transaction.UserTransaction;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.JobPriority;
import run.ratchet.api.JobStatus;
import run.ratchet.ri.core.internal.PostExecutionHandler;
import run.ratchet.ri.payload.JobPayloadFactory;
import run.ratchet.store.entity.BatchEntity;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.BatchStore;
import run.ratchet.store.spi.JobCrudStore;
import run.ratchet.testsuite.app.TestTransactionRunner;
import run.ratchet.testsuite.util.BaseRatchetIT;
import run.ratchet.testsuite.util.RatchetArchiveBuilder;

/**
 * Proves the runtime effect of {@code @Transactional(REQUIRES_NEW)} on {@link
 * PostExecutionHandler}: the batch-child completion ack commits in its own transaction and survives
 * a rollback of the caller's JTA context. The annotation is pinned by reflection in the store TCK,
 * but only a live container proves that the container actually suspends the caller transaction
 * rather than silently treating the attribute as REQUIRED and re-executing completed work.
 *
 * <p>JPA-only: exercises JTA suspension semantics not applicable to document stores.
 */
@EnabledIfSystemProperty(named = "ratchet.test.db.type", matches = "mysql|postgresql")
class PostExecutionRequiresNewIT extends BaseRatchetIT {

  @Inject private PostExecutionHandler postExecutionHandler;

  @Inject private BatchStore batchStore;

  @Inject private JobCrudStore jobCrudStore;

  @Inject private UserTransaction utx;

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
  void batchChildAck_commitsInOwnTransaction_andSurvivesCallerRollback() throws Exception {
    // A three-item batch so neither the ack nor the probe below completes it: the only observable
    // effect is the completed-child counter moving, with no completion cascade.
    // Seeding goes through TestTransactionRunner so the servlet frame cannot be left poisoned by
    // the store-interceptor TOM race before the deliberate utx.begin() below; the
    // handleJobSuccess window stays exposed because caller-managed rollback IS the behavior
    // under test.
    JobEntity parent =
        txRunner.call(
            () -> {
              JobEntity saved = jobCrudStore.save(batchParentJob());
              BatchEntity batch = new BatchEntity();
              batch.setId(saved.getId());
              batch.setTotalItems(3);
              batch.setCompletedItems(0);
              batch.setFailedItems(0);
              batch.setCompletionProcessed(false);
              batchStore.saveBatch(batch);
              return saved;
            });

    // update() reads only getDependsOn() off the child, so an in-memory entity is enough and the
    // poller has no claimable row to touch.
    JobEntity child = new JobEntity();
    child.setJobType(JobExecutionType.BATCH_CHILD);
    child.setDependsOn(parent.getId());

    utx.begin();
    postExecutionHandler.handleJobSuccess(child); // REQUIRES_NEW commits completed 0 -> 1
    utx.rollback(); // rolls back the empty caller transaction

    // Read the committed counter without going through the JPA first/second-level cache: a fresh
    // atomic increment locks and reads the live row, then returns the post-update value. If the
    // REQUIRES_NEW ack committed, the row is already 1 and this returns 2; if the ack had instead
    // joined and rolled back with the caller, the row would be 0 and this would return 1.
    int afterProbeIncrement = batchStore.incrementCompletedAtomic(parent.getId()).completedItems();
    assertEquals(
        2,
        afterProbeIncrement,
        "REQUIRES_NEW ack must commit independently of the rolled-back caller transaction");
  }

  private static JobEntity batchParentJob() {
    JobEntity job = new JobEntity();
    job.setJobType(JobExecutionType.BATCH_PARENT);
    job.setStatus(JobStatus.PENDING);
    job.setPriority(JobPriority.NORMAL);
    job.setBackoffPolicy(BackoffPolicy.NONE);
    // Future-dated so the running poller never claims the parent anchor mid-test.
    job.setScheduledTime(Instant.now().plus(Duration.ofHours(1)));
    job.setPayload(JobPayloadFactory.noop());
    job.setIdempotencyKey(UUID.randomUUID().toString());
    return job;
  }
}

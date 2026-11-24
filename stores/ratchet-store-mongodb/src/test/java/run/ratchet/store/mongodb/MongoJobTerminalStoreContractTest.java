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
package run.ratchet.store.mongodb;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.mongodb.MongoFieldNames.COMPLETION_TOKEN;
import static run.ratchet.store.mongodb.MongoFieldNames.ID;
import static run.ratchet.store.mongodb.MongoFieldNames.STATUS;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.store.dto.JobCompletionPlan.DependencyTransition;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.JobStore;
import run.ratchet.tck.store.AbstractJobTerminalStoreContract;

/** MongoDB contract test for {@code JobTerminalStore} operations. */
class MongoJobTerminalStoreContractTest extends AbstractJobTerminalStoreContract {

  private static final MongoTestFixture fixture = new MongoTestFixture();

  @Test
  void completionIsDurableOnlyWhenItsFencedWriteCommits() {
    JobEntity job = runningClaim();
    var plan =
        completionPlan(job, JobStatus.RUNNING, JobStatus.SUCCEEDED, null, List.of(), List.of());
    var committed = store().commitCompletion(plan);
    assertTrue(committed.committed());
    assertTrue(committed.durable());

    var missed = store().commitCompletion(plan);
    assertFalse(missed.committed());
    assertFalse(missed.durable());
  }

  @Test
  void completionConfirmationWaitsForAnInFlightWrite() throws Exception {
    JobEntity job = runningClaim();
    UUID token = UUID.randomUUID();
    CountDownLatch commandStarted = new CountDownLatch(1);
    CompletableFuture<Boolean> confirmation;
    try (var session = fixture.startSession()) {
      session.startTransaction();
      assertEquals(
          1,
          fixture
              .database()
              .getCollection("scheduler_job")
              .updateOne(
                  session,
                  eq(ID, job.getId()),
                  combine(set(STATUS, JobStatus.FAILED.name()), set(COMPLETION_TOKEN, token)))
              .getModifiedCount());
      confirmation =
          CompletableFuture.supplyAsync(
              () ->
                  fixture.observeCommands(
                      command -> {
                        if (command.equals("findAndModify")) commandStarted.countDown();
                      },
                      () ->
                          store()
                              .wasCompletionCommitted(
                                  job.getId(), JobStatus.FAILED, job.getClaimSeq(), token)));
      assertTrue(commandStarted.await(5, TimeUnit.SECONDS));
      assertFalse(confirmation.isDone());
      session.commitTransaction();
    }
    assertTrue(confirmation.get(5, TimeUnit.SECONDS));
  }

  @Test
  void completionCancelOfRunningDependencyRecordsItsClaim() {
    JobEntity parent = runningClaim();
    JobEntity first = runningClaim();
    // A second claim gives the dependent a claim sequence the parent does not share.
    assertTrue(store().resetRunningJob(first.getId(), "mongo-attribution", first.getClaimSeq()));
    assertTrue(store().tryPickUpJob(first.getId(), "mongo-attribution"));
    JobEntity dependent = store().findById(first.getId()).orElseThrow();
    assertNotEquals(parent.getClaimSeq(), dependent.getClaimSeq());
    var change =
        new DependencyTransition(
            dependent.getId(),
            JobStatus.RUNNING,
            dependent.getVersion(),
            dependent.getScheduledTime(),
            JobStatus.CANCELED,
            dependent.getScheduledTime(),
            dependent.getJobType());
    assertTrue(
        store()
            .commitCompletion(
                completionPlan(
                    parent,
                    JobStatus.RUNNING,
                    JobStatus.SUCCEEDED,
                    null,
                    List.of(change),
                    List.of()))
            .committed());
    assertTrue(store().wasCanceledUnderClaim(dependent.getId(), dependent.getClaimSeq()));
    assertFalse(store().wasCanceledUnderClaim(dependent.getId(), parent.getClaimSeq()));
  }

  private JobEntity runningClaim() {
    JobEntity job = persist(newPendingJob());
    assertTrue(store().tryPickUpJob(job.getId(), "mongo-attribution"));
    return store().findById(job.getId()).orElseThrow();
  }

  @AfterAll
  static void closeFixture() {
    fixture.close();
  }

  @Override
  public JobStore store() {
    return fixture.store();
  }

  @Override
  public JobEntity newPendingJob() {
    return fixture.newPendingJob();
  }

  @Override
  public JobEntity newBatchParentJob() {
    return fixture.newBatchParentJob();
  }

  @Override
  public void cleanupStore() {
    fixture.cleanupStore();
  }
}

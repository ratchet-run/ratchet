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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
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

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
package run.ratchet.tck.store.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.ACQUIRED;
import static run.ratchet.store.spi.ResourcePermitStore.AcquireResult.AT_CAPACITY;

import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.spi.ResourcePermitStore;
import run.ratchet.store.spi.ResourcePermitStore.AcquireResult;
import run.ratchet.tck.store.JpaContainerFixture;

/**
 * Exercises a claim change after the permit claim check and before its write in the bundled SQL
 * stores. The attempt that passed the check can still acquire and holds capacity until its permit
 * is released, even though a newer claim is running by the time the write completes.
 *
 * <p>This is not a TCK contract. It pins the bundled stores' claim read, {@code SELECT status,
 * claim_seq FROM scheduler_job_queue}, followed by a separate permit write. A store that checks the
 * claim another way, or atomically with the write, conforms without this window.
 */
public abstract class AbstractSqlResourcePermitClaimRaceTest {

  protected abstract JpaContainerFixture fixture();

  @BeforeEach
  @AfterEach
  void clean() {
    fixture().cleanupStore();
  }

  @Test
  void staleClaimThatPassedTheCheckHoldsPermitUntilItReleases() {
    ResourcePermitStore permits = fixture().resourcePermitStore();
    permits.configureResource("res-race", 1, 1000, "claim race");
    JobEntity first = claimNewJob("node-a");
    Thread testThread = Thread.currentThread();
    AtomicBoolean fired = new AtomicBoolean();
    AtomicReference<JobEntity> retry = new AtomicReference<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    AcquireResult stale;
    try {
      fixture()
          .observeNativeQueries(
              (sql, event) -> {
                if (Thread.currentThread() != testThread
                    || !event.completed()
                    || !event.operation().equals("getResultList")
                    || !sql.replaceAll("\\s+", " ")
                        .toUpperCase(Locale.ROOT)
                        .contains("SELECT STATUS, CLAIM_SEQ FROM SCHEDULER_JOB_QUEUE")
                    || !fired.compareAndSet(false, true)) {
                  return;
                }
                try {
                  executor
                      .submit(
                          () -> {
                            assertTrue(
                                fixture()
                                    .store()
                                    .scheduleJobRetry(
                                        first.getId(),
                                        "timed out",
                                        Instant.now().minusSeconds(5),
                                        first.getAttempts() + 1,
                                        first.getClaimSeq()));
                            var claimed = fixture().store().claimNextBatch(1, "node-b");
                            assertEquals(1, claimed.size());
                            assertEquals(first.getId(), claimed.get(0).getId());
                            assertTrue(claimed.get(0).getClaimSeq() > first.getClaimSeq());
                            retry.set(claimed.get(0));
                          })
                      .get(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new IllegalStateException("claim change interrupted", e);
                } catch (ExecutionException e) {
                  throw new IllegalStateException("claim change failed", e.getCause());
                } catch (TimeoutException e) {
                  throw new IllegalStateException("claim change timed out", e);
                }
              });
      stale = permits.tryAcquirePermit("res-race", first.getId(), first.getClaimSeq(), "node-a");
    } finally {
      fixture().observeNativeQueries((sql, event) -> {});
      executor.shutdownNow();
    }
    assertTrue(fired.get(), "the claim changed between the check and the write");
    assertEquals(ACQUIRED, stale);
    assertEquals(AT_CAPACITY, acquire(permits, retry.get(), "node-b"));
    JobEntity other = claimNewJob("node-c");
    assertEquals(AT_CAPACITY, acquire(permits, other, "node-c"));
    permits.releasePermit("res-race", first.getId(), first.getClaimSeq());
    assertEquals(ACQUIRED, acquire(permits, retry.get(), "node-b"));
    assertEquals(AT_CAPACITY, acquire(permits, other, "node-c"));
  }

  private JobEntity claimNewJob(String nodeId) {
    JobEntity pending = fixture().store().create(fixture().newPendingJob());
    var claimed = fixture().store().claimNextBatch(1, nodeId);
    assertEquals(1, claimed.size());
    assertEquals(pending.getId(), claimed.get(0).getId());
    return claimed.get(0);
  }

  private AcquireResult acquire(ResourcePermitStore permits, JobEntity job, String nodeId) {
    return permits.tryAcquirePermit("res-race", job.getId(), job.getClaimSeq(), nodeId);
  }
}

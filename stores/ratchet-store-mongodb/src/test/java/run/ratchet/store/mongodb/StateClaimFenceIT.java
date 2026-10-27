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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import run.ratchet.store.spi.JobStateStore.StateWriteResult;
import run.ratchet.tck.util.ConcurrentTestRunner;

/** Uses the same replica-set fixture as ConcurrentClaimIT. */
class StateClaimFenceIT extends BaseDocumentStoreIT {
  @Test
  void reclaimSerializesWithStateWrites() {
    for (int round = 0; round < 6; round++) {
      var id = store().save(newPendingJob()).getId();
      long claim = store().claimNextBatch(1, "old-owner").get(0).getClaimSeq();
      assertEquals(StateWriteResult.UPDATED, store().initState(id, "race", "initial", claim));
      AtomicBoolean reclaimed = new AtomicBoolean();
      AtomicInteger updated = new AtomicInteger();
      var failures =
          ConcurrentTestRunner.runAll(
              Duration.ofSeconds(15),
              () -> {
                for (int attempt = 0; attempt < 30; attempt++) {
                  boolean afterReclaim = reclaimed.get();
                  StateWriteResult result =
                      store().updateState(id, "race", "write", updated.get(), claim);
                  if (afterReclaim) assertEquals(StateWriteResult.SUPERSEDED, result);
                  if (result == StateWriteResult.UPDATED) updated.incrementAndGet();
                }
              },
              () -> {
                assertEquals(
                    1,
                    store().resetOrphanJobsBefore(Instant.now().plusSeconds(60), 3, 100).reset());
                reclaim(id, claim);
                reclaimed.set(true);
              });
      assertTrue(failures.stream().allMatch(failure -> failure == null), failures.toString());
      assertTrue(reclaimed.get());
      assertEquals(updated.get(), store().getState(id, "race").orElseThrow().version());
      assertEquals(
          StateWriteResult.SUPERSEDED,
          store().updateState(id, "race", "stale", updated.get(), claim));
      long nextClaim = store().findById(id).orElseThrow().getClaimSeq();
      assertEquals(
          StateWriteResult.UPDATED,
          store().updateState(id, "race", "new owner", updated.get(), nextClaim));
      assertEquals(updated.get() + 1, store().getState(id, "race").orElseThrow().version());
      store().cancelJob(id);
    }
  }

  private void reclaim(UUID id, long oldClaim) {
    for (int attempt = 0; attempt < 100; attempt++) {
      var claimed = store().claimNextBatch(1, "next-owner");
      if (!claimed.isEmpty()) {
        assertEquals(id, claimed.get(0).getId());
        assertTrue(claimed.get(0).getClaimSeq() > oldClaim);
        return;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("Interrupted while waiting to reclaim job " + id, e);
      }
    }
    fail("Failed to reclaim job " + id + " after 100 claim attempts");
  }
}

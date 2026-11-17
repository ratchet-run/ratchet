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
package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

/** Unicode storage and same-value lookup, independent of each store's comparison collation. */
public abstract class AbstractUnicodeStringKeyStoreContract implements JobStoreContractFixture {
  @BeforeEach
  @AfterEach
  void cleanupUnicodeKeys() {
    cleanupStore();
  }

  @Test
  void unicodeSignalKeyRoundTripsAndWakesJob() {
    var signals = signalStore();
    var job = newPendingJob();
    job.setStatus(JobStatus.WAITING);
    job.setSignalKey("sig-ā中");
    job.setSignalTimeout(Instant.now().plusSeconds(600));
    var saved = persist(job);
    assertEquals("sig-ā中", store().findById(saved.getId()).orElseThrow().getSignalKey());
    assertEquals(
        1,
        signals.deliverSignalByKey(
            "sig-ā中",
            null,
            null,
            "APPROVED",
            null,
            "caller",
            Instant.now(),
            UUID.randomUUID().toString()));
    assertEquals(JobStatus.PENDING, store().findById(saved.getId()).orElseThrow().getStatus());
  }

  @Test
  void unicodeTagIsFoundAndCancelled() {
    var queries = queryStore();
    var saved = persist(newPendingJob());
    store().insertTags(saved.getId(), List.of("tag-ā中"));
    assertEquals(List.of(saved.getId()), queries.findJobIdsByTag("tag-ā中", 10, 0));
    assertEquals(1, store().cancelJobsByTag("tag-ā中"));
    assertEquals(JobStatus.CANCELED, store().findById(saved.getId()).orElseThrow().getStatus());
  }

  @Test
  void unicodeNodeHeartbeatRenewsOneRow() {
    Instant first = Instant.parse("2026-05-12T12:00:00Z");
    store().upsertHeartbeat("node-ā中", first);
    store().upsertHeartbeat("node-ā中", first.plusSeconds(10));
    var node = store().findNodeById("node-ā中").orElseThrow();
    assertEquals("node-ā中", node.getId());
    assertEquals(first.plusSeconds(10), node.getLastHeartbeat());
    assertEquals(1, store().findAllNodes(10).size());
  }

  @Test
  void unicodeLockIsAcquiredRenewedAndReleased() {
    var locks = lockStore();
    assertTrue(locks.tryLock("lock-ā中", Duration.ofMinutes(5), "owner"));
    assertTrue(locks.renewLock("lock-ā中", Duration.ofMinutes(10), "owner"));
    assertFalse(locks.tryLock("lock-ā中", Duration.ofMinutes(5), "other"));
    locks.unlock("lock-ā中", "owner");
    assertTrue(locks.tryLock("lock-ā中", Duration.ofMinutes(5), "other"));
  }
}

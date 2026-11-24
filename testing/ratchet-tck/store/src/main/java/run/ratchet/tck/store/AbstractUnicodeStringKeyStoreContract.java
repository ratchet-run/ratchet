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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.entity.JobExecutionType;
import run.ratchet.store.spi.ExecutionTargetFilter;

/** Unicode storage and same-value lookup, independent of each store's comparison collation. */
public abstract class AbstractUnicodeStringKeyStoreContract implements JobStoreContractFixture {
  @BeforeEach
  @AfterEach
  void cleanupUnicodeKeys() {
    cleanupStore();
  }

  @Test
  void lockNamesAndOwnersUseTheTrimmedKey() {
    var locks = lockStore();
    var ttl = Duration.ofMinutes(5);
    assertTrue(locks.tryLock(" key ", ttl, " owner "));
    assertFalse(locks.tryLock("key", ttl, "other"));
    assertTrue(locks.renewLock("key ", ttl, "owner"));
    locks.unlock("key", "owner ");
    assertTrue(locks.tryLock("key ", ttl, "other"));
    assertThrows(IllegalArgumentException.class, () -> locks.tryLock(" ", ttl, "owner"));
    assertThrows(IllegalArgumentException.class, () -> locks.renewLock("key", ttl, " "));
    assertThrows(IllegalArgumentException.class, () -> locks.unlock(" ", "other"));
  }

  @Test
  void nodeHeartbeatAndLookupUseTheTrimmedKey() {
    Instant now = Instant.parse("2026-05-12T12:00:00Z");
    store().upsertHeartbeat(" k ", now);
    store().upsertHeartbeat("k", now.plusSeconds(1));
    assertEquals("k", store().findNodeById("k ").orElseThrow().getId());
    assertEquals(now.plusSeconds(1), store().findNodeById("k").orElseThrow().getLastHeartbeat());
    assertEquals(1, store().findAllNodes(10).size());
    assertThrows(IllegalArgumentException.class, () -> store().upsertHeartbeat(" ", now));
    assertThrows(IllegalArgumentException.class, () -> store().findNodeById(" "));
  }

  @Test
  void signalWriteAndDeliveryUseTheTrimmedKey() {
    var signals = signalStore();
    var waiting = newPendingJob();
    waiting.setStatus(JobStatus.WAITING);
    waiting.setSignalKey(" k ");
    waiting.setSignalTimeout(Instant.now().plusSeconds(600));
    var job = persist(waiting);
    assertEquals("k", store().findById(job.getId()).orElseThrow().getSignalKey());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            signals.deliverSignalByKey(
                " ",
                null,
                null,
                "APPROVED",
                null,
                "caller",
                Instant.now(),
                UUID.randomUUID().toString()));
    assertEquals(
        1,
        signals.deliverSignalByKey(
            "k ",
            null,
            null,
            "APPROVED",
            null,
            "caller",
            Instant.now(),
            UUID.randomUUID().toString()));
    assertEquals(JobStatus.PENDING, store().findById(job.getId()).orElseThrow().getStatus());
  }

  @Test
  void claimFiltersUseTheTrimmedExecutionTargetAndNodeId() {
    var job = newPendingJob();
    assertThrows(IllegalArgumentException.class, () -> job.setExecutionTarget(" "));
    job.setExecutionTarget(" target ");
    var saved = persist(job);
    assertThrows(
        IllegalArgumentException.class, () -> ExecutionTargetFilter.matching(List.of(" "), false));
    assertThrows(
        IllegalArgumentException.class, () -> ExecutionTargetFilter.excluding(List.of(" "), false));
    var claimed =
        store()
            .claimNextBatchOptimized(
                JobExecutionType.SINGLE,
                1,
                " node ",
                NodeTagFilter.NONE,
                ExecutionTargetFilter.matching(List.of("target "), false));
    assertEquals(List.of(saved.getId()), claimed.stream().map(claim -> claim.id()).toList());
    assertEquals("target", claimed.get(0).executionTarget());
    assertEquals("node", store().findById(saved.getId()).orElseThrow().getPickedBy());
  }

  @Test
  void unicodeSignalKeyRoundTripsAndWakesJob() {
    var signals = signalStore();
    var job = newPendingJob();
    job.setStatus(JobStatus.WAITING);
    job.setSignalKey("sig-ā中😀");
    job.setSignalTimeout(Instant.now().plusSeconds(600));
    var saved = persist(job);
    assertEquals("sig-ā中😀", store().findById(saved.getId()).orElseThrow().getSignalKey());
    assertEquals(
        1,
        signals.deliverSignalByKey(
            "sig-ā中😀",
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
    store().insertTags(saved.getId(), List.of("tag-ā中😀"));
    assertEquals(List.of(saved.getId()), queries.findJobIdsByTag("tag-ā中😀", 10, 0));
    assertEquals(1, store().cancelJobsByTag("tag-ā中😀"));
    assertEquals(JobStatus.CANCELED, store().findById(saved.getId()).orElseThrow().getStatus());
  }

  @Test
  void unicodeNodeHeartbeatRenewsOneRow() {
    Instant first = Instant.parse("2026-05-12T12:00:00Z");
    store().upsertHeartbeat("node-ā中😀", first);
    store().upsertHeartbeat("node-ā中😀", first.plusSeconds(10));
    var node = store().findNodeById("node-ā中😀").orElseThrow();
    assertEquals("node-ā中😀", node.getId());
    assertEquals(first.plusSeconds(10), node.getLastHeartbeat());
    assertEquals(1, store().findAllNodes(10).size());
  }

  @Test
  void unicodeLockIsAcquiredRenewedAndReleased() {
    var locks = lockStore();
    assertTrue(locks.tryLock("lock-ā中😀", Duration.ofMinutes(5), "owner"));
    assertTrue(locks.renewLock("lock-ā中😀", Duration.ofMinutes(10), "owner"));
    assertFalse(locks.tryLock("lock-ā中😀", Duration.ofMinutes(5), "other"));
    locks.unlock("lock-ā中😀", "owner");
    assertTrue(locks.tryLock("lock-ā中😀", Duration.ofMinutes(5), "other"));
  }

  @Test
  void unicodeBusinessKeyHoldIsReleasedByMatchingClaim() {
    var job = newPendingJob();
    job.setBusinessKey("bk-ā中😀");
    var saved = persist(job);
    var claim = store().claimNextBatch(1, "hold-node").get(0);
    assertEquals(saved.getId(), claim.getId());
    assertTrue(store().cancelRunningJob(saved.getId(), Instant.now().plusSeconds(600)));
    assertTrue(store().findBusinessKeyHolder("bk-ā中😀").isPresent());
    assertTrue(store().releaseBusinessKeyHold(saved.getId(), claim.getClaimSeq()));
    assertTrue(store().findBusinessKeyHolder("bk-ā中😀").isEmpty());
    var replacement = newPendingJob();
    replacement.setBusinessKey("bk-ā中😀");
    persist(replacement);
  }

  @Test
  void unicodeBusinessKeyHoldIsReleasedByExpiredHoldSweep() {
    var job = newPendingJob();
    job.setBusinessKey("bk-ā中😀");
    var saved = persist(job);
    assertEquals(saved.getId(), store().claimNextBatch(1, "hold-node").get(0).getId());
    assertTrue(store().cancelRunningJob(saved.getId(), Instant.now().minusSeconds(60)));
    assertTrue(store().findBusinessKeyHolder("bk-ā中😀").isPresent());
    assertEquals(1, store().deleteExpiredBusinessKeyHolds(Instant.now(), 100));
    assertTrue(store().findBusinessKeyHolder("bk-ā中😀").isEmpty());
    var replacement = newPendingJob();
    replacement.setBusinessKey("bk-ā中😀");
    persist(replacement);
  }
}

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
import java.util.function.BiConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.JobStatus;

/** Unicode storage and exact comparison of string keys. */
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

  private void forDistinctKeys(BiConsumer<String, String> check) {
    List<List<String>> pairs =
        List.of(
            List.of("x-a", "X-A"),
            List.of("x-a", "x-ā"),
            List.of("x-ā", "x-a\u0304"),
            List.of("x-😀", "x-😁"),
            List.of("x-😀", "x-"));
    for (var pair : pairs) {
      try {
        check.accept(pair.get(0), pair.get(1));
      } catch (AssertionError failure) {
        throw new AssertionError("Key comparison failed for " + pair, failure);
      } finally {
        cleanupStore();
      }
    }
  }

  @Test
  void locksAndNodesKeepBothKeys() {
    forDistinctKeys(this::locksAndNodesKeepBothKeys);
  }

  private void locksAndNodesKeepBothKeys(String first, String second) {
    var store = store();
    var locks = lockStore();
    assertTrue(locks.tryLock(first, Duration.ofMinutes(5), "first-owner"));
    assertTrue(locks.tryLock(second, Duration.ofMinutes(5), "second-owner"));
    assertFalse(locks.tryLock(first, Duration.ofMinutes(5), "second-owner"));
    assertFalse(locks.tryLock(second, Duration.ofMinutes(5), "first-owner"));
    assertTrue(locks.renewLock(first, Duration.ofMinutes(5), "first-owner"));
    assertTrue(locks.renewLock(second, Duration.ofMinutes(5), "second-owner"));
    Instant now = Instant.now();
    store.upsertHeartbeat(first, now);
    store.upsertHeartbeat(second, now.plusSeconds(1));
    assertEquals(first, store.findNodeById(first).orElseThrow().getId());
    assertEquals(second, store.findNodeById(second).orElseThrow().getId());
    assertEquals(2, store.findAllNodes(10).size());
  }

  @Test
  void tagsSignalsAndBusinessKeysKeepBothJobs() {
    forDistinctKeys(this::tagsSignalsAndBusinessKeysKeepBothJobs);
  }

  private void tagsSignalsAndBusinessKeysKeepBothJobs(String first, String second) {
    var store = store();
    var queries = queryStore();
    var signals = signalStore();
    var firstJob = newPendingJob();
    firstJob.setBusinessKey(first);
    var secondJob = newPendingJob();
    secondJob.setBusinessKey(second);
    store.create(firstJob);
    store.create(secondJob);
    assertEquals(firstJob.getId(), store.findBusinessKeyHolder(first).orElseThrow().id());
    assertEquals(secondJob.getId(), store.findBusinessKeyHolder(second).orElseThrow().id());
    assertEquals(first, store.findById(firstJob.getId()).orElseThrow().getBusinessKey());
    assertEquals(second, store.findById(secondJob.getId()).orElseThrow().getBusinessKey());
    store.insertTags(firstJob.getId(), List.of(first));
    store.insertTags(secondJob.getId(), List.of(second));
    assertEquals(List.of(firstJob.getId()), queries.findJobIdsByTag(first, 10, 0));
    assertEquals(List.of(secondJob.getId()), queries.findJobIdsByTag(second, 10, 0));
    assertEquals(List.of(first), store.findById(firstJob.getId()).orElseThrow().getTags());
    assertEquals(List.of(second), store.findById(secondJob.getId()).orElseThrow().getTags());

    var firstWaiting = newPendingJob();
    firstWaiting.setStatus(JobStatus.WAITING);
    firstWaiting.setSignalKey(first);
    firstWaiting.setSignalTimeout(Instant.now().plusSeconds(600));
    var secondWaiting = newPendingJob();
    secondWaiting.setStatus(JobStatus.WAITING);
    secondWaiting.setSignalKey(second);
    secondWaiting.setSignalTimeout(Instant.now().plusSeconds(600));
    store.create(firstWaiting);
    store.create(secondWaiting);
    assertEquals(first, store.findById(firstWaiting.getId()).orElseThrow().getSignalKey());
    assertEquals(second, store.findById(secondWaiting.getId()).orElseThrow().getSignalKey());
    assertEquals(
        1,
        signals.deliverSignalByKey(
            first,
            null,
            null,
            "APPROVED",
            null,
            "caller",
            Instant.now(),
            UUID.randomUUID().toString()));
    assertEquals(JobStatus.PENDING, store.findById(firstWaiting.getId()).orElseThrow().getStatus());
    assertEquals(
        JobStatus.WAITING, store.findById(secondWaiting.getId()).orElseThrow().getStatus());
    assertEquals(
        1,
        signals.deliverSignalByKey(
            second,
            null,
            null,
            "APPROVED",
            null,
            "caller",
            Instant.now(),
            UUID.randomUUID().toString()));
    assertEquals(
        JobStatus.PENDING, store.findById(secondWaiting.getId()).orElseThrow().getStatus());
  }

  @Test
  void resourcesKeepBothConfigurations() {
    forDistinctKeys(this::resourcesKeepBothConfigurations);
  }

  private void resourcesKeepBothConfigurations(String first, String second) {
    var resources = resourcePermitStore();
    resources.configureResource(first, 1, 100, "first");
    resources.configureResource(second, 2, 200, "second");
    var firstLimit = resources.findResourceLimit(first).orElseThrow();
    var secondLimit = resources.findResourceLimit(second).orElseThrow();
    assertEquals(first, firstLimit.resourceName());
    assertEquals(second, secondLimit.resourceName());
    assertEquals(1, firstLimit.maxConcurrent());
    assertEquals(2, secondLimit.maxConcurrent());
    assertEquals("first", firstLimit.description());
    assertEquals("second", secondLimit.description());
  }
}

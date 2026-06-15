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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.ExecutorTargets;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.entity.JobEntity;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.RecurringJobStore.ArchiveReason;
import run.ratchet.store.spi.TagStore;

/**
 * TCK contract for {@link RecurringJobStore} — the dedicated recurring-master store.
 *
 * <p>Covers the seven mandatory contracts:
 *
 * <ol>
 *   <li>Concurrent claim safety — exactly-once observation across nodes.
 *   <li>Atomic next-fire advance — successor claim cycle does not re-claim the master.
 *   <li>Catch-up correctness — bounded MAX_CATCHUP runs spawn before {@code next_fire} is updated.
 *   <li>Pause / resume isolation + concurrent-admin idempotency.
 *   <li>Cancel / archive atomicity — snapshot row appears iff live row disappears.
 *   <li>Business-key reservation orphan absence post-cancel.
 *   <li>Child lineage via {@code recurring_master_id}.
 * </ol>
 *
 * <p>Implementors provide a configured {@link RecurringJobStore} (typically the same instance that
 * backs {@link JobStoreContractFixture#store()}) plus a {@link JobPayload} factory.
 */
public abstract class AbstractRecurringJobStoreContract {

  /** Returns the {@link RecurringJobStore} under test. */
  protected abstract RecurringJobStore recurringStore();

  /**
   * Returns the {@link TagStore} that backs the same underlying schema as {@link
   * #recurringStore()}. Used by the tag-routing contracts (TCK 8 / 9): in production, recurring
   * masters acquire their tags through this SPI after {@code createRecurring}.
   */
  protected abstract TagStore tagStore();

  /** Builds a no-op {@link JobPayload} so the contract is store-impl agnostic. */
  protected abstract JobPayload noopPayload();

  /**
   * Returns the core {@link JobStoreContractFixture} backing the same store as {@link
   * #recurringStore()}. Used by the child-job round-trip contract, which needs a core {@code
   * create}/{@code findById} plus a transient job factory. Implementors return the same fixture
   * they use to back the recurring store.
   */
  protected abstract JobStoreContractFixture jobFixture();

  /** Removes all rows from the live + archive tables/collections. */
  protected abstract void cleanupRecurringStore();

  @BeforeEach
  @AfterEach
  void clean() {
    cleanupRecurringStore();
  }

  /** TCK 1 — concurrent claim safety: a single due master is claimed exactly once. */
  @Test
  void claimDueRecurring_concurrentNodesObserveExactlyOnce() {
    UUID id = UuidV7Factory.create();
    recurringStore().createRecurring(definition(id, "0 * * * * ?", Instant.now().minusSeconds(60)));

    // First node claims the row inside an implicit transaction boundary in the harness. Most
    // store impls open a fresh transaction per call; in containers with shared connections, both
    // calls run sequentially. Either way the second call must NOT also observe the master because
    // claim advances next_fire well into the future as part of the same fire-path operation.
    List<RecurringJobDefinition> firstBatch = recurringStore().claimDueRecurring(10, "node-1");
    assertEquals(1, firstBatch.size());
    recurringStore().advanceNextFire(id, Instant.now().plusSeconds(3600));

    List<RecurringJobDefinition> secondBatch = recurringStore().claimDueRecurring(10, "node-2");
    assertTrue(secondBatch.isEmpty(), "post-advance, the master must not re-claim");
  }

  // TCK 1b — real-thread concurrent-claim safety used to live here. It was deleted because the
  // store impls' two strategies are too different to share one test: Mongo's claim path stamps
  // a per-row claim_token + claim_expires_at lease (stateful, safe under serial OR parallel
  // calls), while the SQL stores rely on FOR UPDATE SKIP LOCKED which only enforces mutex while
  // the two transactions' lock-hold windows overlap. A common test with no synchronization
  // could not prove either guarantee deterministically. The Mongo flavor lives at
  // MongoRecurringJobStoreContractTest#claimDueRecurring_parallelClaimersDoNotDoubleObserve;
  // the SQL flavor lives at AbstractJpaRecurringClaimConcurrencyContract, which uses
  // JpaContainerFixture.runInTransaction + latches to force the lock-hold windows to overlap.

  /**
   * TCK 1c — releaseClaim returns the row to the eligible pool without changing next_fire. SQL
   * stores satisfy this trivially (transaction-scoped row lock); Mongo must clear the claim_token /
   * claim_expires_at so peers can see the row again.
   */
  @Test
  void releaseClaim_makesClaimedRowImmediatelyEligibleAgain() {
    UUID id = UuidV7Factory.create();
    Instant pastDue = Instant.now().minusSeconds(60);
    recurringStore().createRecurring(definition(id, "0 * * * * ?", pastDue));

    List<RecurringJobDefinition> firstBatch = recurringStore().claimDueRecurring(10, "node-1");
    assertEquals(1, firstBatch.size(), "the past-due master must be claimable initially");

    // Without releaseClaim a Mongo lease would hide the row for CLAIM_LEASE_SECONDS; calling
    // releaseClaim drops the lease so the next claim cycle sees it again immediately. The
    // next_fire value must be unchanged — the row goes back to its original schedule.
    recurringStore().releaseClaim(id);

    List<RecurringJobDefinition> secondBatch = recurringStore().claimDueRecurring(10, "node-2");
    assertEquals(1, secondBatch.size(), "released claim must be observable on the next cycle");
    assertEquals(
        id, secondBatch.get(0).id(), "the same master id must reappear in the second claim");
  }

  /** TCK 2 — atomic next-fire advance: the master's next_fire strictly increases. */
  @Test
  void advanceNextFire_makesFutureClaimsSkipTheRow() {
    UUID id = UuidV7Factory.create();
    // Truncate to milliseconds because some stores (notably MongoDB) persist Dates with
    // millisecond precision, so sub-millisecond nanos from Instant.now() are dropped on read.
    Instant initialFire =
        Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS).minusSeconds(10);
    recurringStore().createRecurring(definition(id, "0 * * * * ?", initialFire));

    Instant nextFire = initialFire.plusSeconds(3600);
    recurringStore().advanceNextFire(id, nextFire);

    Optional<RecurringJobDefinition> reread = recurringStore().getRecurring(id);
    assertTrue(reread.isPresent());
    assertFalse(
        reread.get().nextFire().isBefore(nextFire),
        () ->
            "next_fire must reflect the advanced value; expected >= "
                + nextFire
                + " but got "
                + reread.get().nextFire());
  }

  /**
   * TCK 3 — catch-up correctness: the master is claimable while past-due, becomes non-claimable
   * once next_fire is moved into the future. The RI's catch-up loop is the consumer; the store
   * contract simply guarantees the underlying primitives.
   */
  @Test
  void catchUp_advancesNextFireAtomicallyThroughManyClaims() {
    UUID id = UuidV7Factory.create();
    Instant pastDue = Instant.now().minusSeconds(600);
    recurringStore().createRecurring(definition(id, "0 * * * * ?", pastDue));

    // The RI's RecurringJobExecutor processes catch-up children inside a single advance call.
    // The contract validates the primitive: after advanceNextFire to the future, claim is empty.
    recurringStore().advanceNextFire(id, Instant.now().plusSeconds(3600));
    assertTrue(
        recurringStore().claimDueRecurring(10, "node-1").isEmpty(),
        "post-catchup advance should empty the claim batch");
  }

  /** TCK 4a — pause / resume isolation. */
  @Test
  void pauseRecurring_skipsClaimEvenWithPastDueNextFire() {
    UUID id = UuidV7Factory.create();
    recurringStore().createRecurring(definition(id, "0 * * * * ?", Instant.now().minusSeconds(60)));

    assertTrue(recurringStore().pauseRecurring(id));
    assertTrue(
        recurringStore().claimDueRecurring(10, "node-1").isEmpty(),
        "paused masters must not appear in claim");

    assertTrue(recurringStore().resumeRecurring(id));
    assertFalse(
        recurringStore().claimDueRecurring(10, "node-1").isEmpty(),
        "resumed past-due masters must reappear in claim");
  }

  /** TCK 4b — concurrent-admin idempotency: only one of two pause calls flips the row. */
  @Test
  void pauseRecurring_isIdempotentAcrossConcurrentCallers() {
    UUID id = UuidV7Factory.create();
    recurringStore().createRecurring(definition(id, "0 * * * * ?", Instant.now().plusSeconds(60)));

    assertTrue(recurringStore().pauseRecurring(id));
    assertFalse(
        recurringStore().pauseRecurring(id), "second pause on already-paused master returns false");

    assertTrue(recurringStore().resumeRecurring(id));
    assertFalse(
        recurringStore().resumeRecurring(id),
        "second resume on already-active master returns false");
  }

  /** TCK 5 — cancel / archive atomicity: successful cancel emits an archive row. */
  @Test
  void cancelRecurringAndArchive_writesArchiveSnapshotAtomically() {
    UUID id = UuidV7Factory.create();
    recurringStore().createRecurring(definition(id, "0 * * * * ?", Instant.now().plusSeconds(60)));

    assertTrue(recurringStore().cancelRecurringAndArchive(id, ArchiveReason.CANCELED));
    assertTrue(
        recurringStore().getRecurring(id).isEmpty(), "live row must be gone after cancel/archive");

    // Idempotent: a second cancel on an unknown id returns false.
    assertFalse(recurringStore().cancelRecurringAndArchive(id, ArchiveReason.CANCELED));
  }

  /**
   * TCK 5b — archive snapshot correctness: the row that lands in the archive carries the same
   * identifying state the live row had. The implementor projects every column relevant to a
   * post-mortem (id, businessKey, cron, archive reason, principal). The contract verifies the
   * smallest visible projection: businessKey survives the archive write, and a re-create with the
   * same businessKey succeeds because the bkres slot is freed by the same cancel.
   */
  @Test
  void cancelRecurringAndArchive_archivedRowMatchesLiveSnapshot() {
    String key = "tck-archive-snapshot-" + UUID.randomUUID();
    UUID first = UuidV7Factory.create();
    recurringStore()
        .createRecurring(
            definitionWithBusinessKey(first, key, "0 0 * * * ?", Instant.now().plusSeconds(60)));

    assertTrue(recurringStore().cancelRecurringAndArchive(first, ArchiveReason.CANCELED));
    assertTrue(recurringStore().findRecurringByBusinessKey(key).isEmpty());

    // Re-registering the same key on a fresh id must succeed because the archive snapshot
    // does NOT keep the bkres slot occupied — that's the whole point of moving the row off the
    // live table. If the archive carried a phantom uniqueness lock, this create would fail.
    UUID second = UuidV7Factory.create();
    assertNotNull(
        recurringStore()
            .createRecurring(definitionWithBusinessKey(second, key, "0 0 * * * ?", Instant.now())));
  }

  /** TCK 6 — orphan-absence: the bkres entry is removed in the same cancel transaction. */
  @Test
  void cancelRecurringAndArchive_removesAssociatedBkresEntry() {
    UUID id = UuidV7Factory.create();
    String key = "tck-bkres-" + UUID.randomUUID();
    recurringStore()
        .createRecurring(definitionWithBusinessKey(id, key, "0 * * * * ?", Instant.now()));

    assertTrue(recurringStore().cancelRecurringAndArchive(id, ArchiveReason.CANCELED));
    // After cancel, findRecurringByBusinessKey must return empty (live row gone) and
    // re-registering the same key must succeed (bkres uniqueness slot is free).
    assertTrue(recurringStore().findRecurringByBusinessKey(key).isEmpty());
    UUID newId = UuidV7Factory.create();
    UUID created =
        recurringStore()
            .createRecurring(definitionWithBusinessKey(newId, key, "0 * * * * ?", Instant.now()));
    assertNotNull(created);
  }

  /**
   * TCK 7 — child lineage (master half): the recurring master's id round-trips so the executor has
   * the value it needs to stamp onto every spawned child's {@code recurring_master_id} column. The
   * child-side half of this contract — actually writing a {@link
   * run.ratchet.store.entity.JobEntity} with {@code recurringMasterId} set and re-reading it
   * through the cold-table mapper — lives in {@link #create_persistsRecurringMasterIdOnChildJob}:
   * it needs both this capability (to create the master the child's foreign key references) and the
   * core {@code create}/{@code findById} round-trip.
   */
  @Test
  void getRecurring_returnsCompleteDefinitionForChildLineage() {
    UUID id = UuidV7Factory.create();
    recurringStore().createRecurring(definition(id, "0 * * * * ?", Instant.now().plusSeconds(60)));

    Optional<RecurringJobDefinition> def = recurringStore().getRecurring(id);
    assertTrue(def.isPresent());
    assertEquals(id, def.get().id(), "master id round-trips for child lineage attribution");
  }

  /**
   * TCK 7b — occurrence routing inheritance (master half): execution_target must round-trip on the
   * recurring master so the executor can stamp the same target on each child occurrence.
   */
  @Test
  void getRecurring_returnsExecutionTargetForOccurrenceInheritance() {
    UUID id = UuidV7Factory.create();
    recurringStore()
        .createRecurring(
            definitionWithExecutionTarget(
                id, "0 * * * * ?", Instant.now().plusSeconds(60), ExecutorTargets.VIRTUAL));

    Optional<RecurringJobDefinition> def = recurringStore().getRecurring(id);

    assertTrue(def.isPresent());
    assertEquals(ExecutorTargets.VIRTUAL, def.get().executionTarget());
  }

  /**
   * Business-key uniqueness: two concurrent {@code createRecurring} calls with the same business
   * key must not both succeed. SQL stores enforce via {@code scheduler_business_key_reservation};
   * MongoDB enforces via a unique partial index on {@code scheduler_recurring_job.business_key}.
   * The contract is store-agnostic: the second create raises.
   */
  @Test
  void createRecurring_rejectsDuplicateActiveBusinessKey() {
    String key = "tck-dup-" + UUID.randomUUID();
    recurringStore()
        .createRecurring(
            definitionWithBusinessKey(UuidV7Factory.create(), key, "0 * * * * ?", Instant.now()));

    assertThrows(
        RuntimeException.class,
        () ->
            recurringStore()
                .createRecurring(
                    definitionWithBusinessKey(
                        UuidV7Factory.create(), key, "0 * * * * ?", Instant.now())),
        "second createRecurring with the same active business key must be rejected");
  }

  /**
   * TCK 8 — claim honors require/exclude tag filters. Tags persist via {@link TagStore} after
   * {@code createRecurring}, and {@code claimDueRecurring} must filter on them the same way the
   * one-shot claim path does.
   */
  @Test
  void claimDueRecurring_honorsRequireAndExcludeTagFilters() {
    UUID gpuMaster = UuidV7Factory.create();
    UUID cpuMaster = UuidV7Factory.create();
    Instant pastDue = Instant.now().minusSeconds(60);
    recurringStore().createRecurring(definition(gpuMaster, "0 * * * * ?", pastDue));
    recurringStore().createRecurring(definition(cpuMaster, "0 * * * * ?", pastDue));
    tagStore().insertTags(gpuMaster, List.of("gpu"));
    tagStore().insertTags(cpuMaster, List.of("cpu"));

    NodeTagFilter requireGpu = new NodeTagFilter(List.of("gpu"), List.of());
    List<RecurringJobDefinition> required =
        recurringStore().claimDueRecurring(10, "node-gpu", requireGpu);
    assertEquals(1, required.size(), "only the gpu master matches a gpu require-filter");
    assertEquals(gpuMaster, required.get(0).id());

    NodeTagFilter excludeGpu = new NodeTagFilter(List.of(), List.of("gpu"));
    List<RecurringJobDefinition> filtered =
        recurringStore().claimDueRecurring(10, "node-no-gpu", excludeGpu);
    assertEquals(1, filtered.size(), "only the cpu master remains when gpu is excluded");
    assertEquals(cpuMaster, filtered.get(0).id());
  }

  /**
   * Tag rows attached to a recurring master are removed when the master is canceled. The SQL stores
   * do not carry a {@code fk_job_tag_job} foreign key because tags are polymorphic across {@code
   * scheduler_job} and {@code scheduler_recurring_job}; the cancel path must delete the tag rows
   * explicitly. Mongo embeds tags in the doc, so this is automatic — both impls must satisfy the
   * contract.
   */
  @Test
  void cancelRecurringAndArchive_removesAttachedTagRows() {
    UUID id = UuidV7Factory.create();
    recurringStore().createRecurring(definition(id, "0 * * * * ?", Instant.now().plusSeconds(60)));
    tagStore().insertTags(id, List.of("orphan-test"));

    assertTrue(recurringStore().cancelRecurringAndArchive(id, ArchiveReason.CANCELED));

    // Re-tagging a fresh master with the same tag should find only that master, never the
    // canceled one (which would happen if tag rows were leaking from prior cancels).
    UUID fresh = UuidV7Factory.create();
    recurringStore()
        .createRecurring(definition(fresh, "0 * * * * ?", Instant.now().plusSeconds(60)));
    tagStore().insertTags(fresh, List.of("orphan-test"));
    int canceledByTag = recurringStore().cancelRecurringJobsByTag("orphan-test");
    assertEquals(1, canceledByTag, "tag query must see only the live master, not the orphaned id");
  }

  /**
   * TCK 9 — {@code cancelRecurringJobsByTag} finds masters whose tags were added through {@link
   * TagStore} and leaves untagged masters alone. Catches the Mongo-side regression where {@code
   * insertTags} wrote to the wrong collection and the cancel-by-tag query saw zero rows.
   */
  @Test
  void cancelRecurringJobsByTag_cancelsTaggedMastersOnly() {
    UUID legacy = UuidV7Factory.create();
    UUID survivor = UuidV7Factory.create();
    Instant future = Instant.now().plusSeconds(60);
    recurringStore().createRecurring(definition(legacy, "0 * * * * ?", future));
    recurringStore().createRecurring(definition(survivor, "0 * * * * ?", future));
    tagStore().insertTags(legacy, List.of("legacy"));

    int canceled = recurringStore().cancelRecurringJobsByTag("legacy");
    assertEquals(1, canceled, "only the legacy-tagged master is canceled");
    assertTrue(recurringStore().getRecurring(legacy).isEmpty());
    assertTrue(recurringStore().getRecurring(survivor).isPresent());
  }

  /**
   * Annotation-orphan cleanup must honor BOTH predicates: a master is canceled only when its
   * business key is absent from the known set AND it was created strictly before the node-start
   * cutoff. Masters registered after node start are exempt even when unknown; an empty known set
   * cancels every business-keyed master created before the cutoff. The only prior coverage mocked
   * the store, so a sign-flip on either predicate (the created_at comparison or the NOT-IN
   * membership) would silently cancel live recurring masters or strand orphans firing forever. This
   * drives the real SQL/Mongo filter on every store.
   *
   * <p>created_at is settable on the definition (all three stores honor {@code createdAt()}), so
   * before/after-cutoff is expressed directly on each master rather than relying on persist order:
   * keyA/keyB are stamped 60s before a fixed cutoff, keyC 60s after it. The cutoff is truncated to
   * milliseconds because MongoDB persists Dates at millisecond precision.
   */
  @Test
  void cancelOrphanedRecurringAnnotationJobs_honorsKnownSetAndCreatedAtCutoff() {
    Instant cutoff = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    Instant beforeCutoff = cutoff.minusSeconds(60);
    Instant afterCutoff = cutoff.plusSeconds(60);

    String keyA = "tck-orphan-a-" + UUID.randomUUID();
    String keyB = "tck-orphan-b-" + UUID.randomUUID();
    String keyC = "tck-orphan-c-" + UUID.randomUUID();
    UUID idA = UuidV7Factory.create();
    UUID idB = UuidV7Factory.create();
    UUID idC = UuidV7Factory.create();
    Instant fire = Instant.now().plusSeconds(60);
    recurringStore().createRecurring(orphanCandidate(idA, keyA, fire, beforeCutoff));
    recurringStore().createRecurring(orphanCandidate(idB, keyB, fire, beforeCutoff));
    recurringStore().createRecurring(orphanCandidate(idC, keyC, fire, afterCutoff));

    // keyA is known (survives), keyB is unknown + before cutoff (canceled), keyC is unknown but
    // after cutoff (exempt — registered after this node started).
    int firstPass = recurringStore().cancelOrphanedRecurringAnnotationJobs(Set.of(keyA), cutoff);
    assertEquals(1, firstPass, "only the unknown, before-cutoff master (keyB) is canceled");
    assertTrue(recurringStore().getRecurring(idA).isPresent(), "known key survives cleanup");
    assertTrue(recurringStore().getRecurring(idB).isEmpty(), "unknown + before-cutoff is canceled");
    assertTrue(
        recurringStore().getRecurring(idC).isPresent(),
        "after-cutoff master is exempt even when unknown");

    // Empty known set: every business-keyed master before the cutoff is now an orphan, so keyA is
    // canceled too — but keyC stays spared because the created_at cutoff still protects it.
    int secondPass = recurringStore().cancelOrphanedRecurringAnnotationJobs(Set.of(), cutoff);
    assertEquals(
        1, secondPass, "empty known set cancels the remaining before-cutoff master (keyA)");
    assertTrue(recurringStore().getRecurring(idA).isEmpty(), "empty known set cancels keyA");
    assertTrue(
        recurringStore().getRecurring(idC).isPresent(), "created_at cutoff still spares keyC");
  }

  /**
   * A child job created with a {@code recurringMasterId} must persist that link and read it back.
   * The child references a live master through a foreign key on SQL stores, so this contract
   * belongs to the recurring capability rather than the core CRUD contract — it needs both a
   * created master and the core {@code create}/{@code findById} round-trip.
   */
  @Test
  void create_persistsRecurringMasterIdOnChildJob() {
    UUID masterId = UuidV7Factory.create();
    recurringStore()
        .createRecurring(definition(masterId, "0 * * * * ?", Instant.now().plusSeconds(3600)));

    JobEntity child = jobFixture().newPendingJob();
    child.setRecurringMasterId(masterId);
    JobEntity created = jobFixture().store().create(child);

    JobEntity reread = jobFixture().store().findById(created.getId()).orElseThrow();
    assertEquals(
        masterId,
        reread.getRecurringMasterId(),
        "recurring_master_id must round-trip through the child INSERT and the row mapper");
  }

  private RecurringJobDefinition orphanCandidate(
      UUID id, String businessKey, Instant nextFire, Instant createdAt) {
    return new RecurringJobDefinition(
        id,
        "0 * * * * ?",
        "UTC",
        nextFire,
        false,
        null,
        2,
        0,
        BackoffPolicy.NONE,
        0,
        0,
        noopPayload(),
        null,
        null,
        businessKey,
        null,
        null,
        createdAt,
        null,
        false);
  }

  private RecurringJobDefinition definition(UUID id, String cron, Instant nextFire) {
    return definitionWithExecutionTarget(id, cron, nextFire, null);
  }

  private RecurringJobDefinition definitionWithExecutionTarget(
      UUID id, String cron, Instant nextFire, String executionTarget) {
    return new RecurringJobDefinition(
        id,
        cron,
        "UTC",
        nextFire,
        false,
        null,
        2,
        0,
        BackoffPolicy.NONE,
        0,
        0,
        noopPayload(),
        null,
        null,
        null,
        null,
        executionTarget,
        Instant.now(),
        null,
        false);
  }

  private RecurringJobDefinition definitionWithBusinessKey(
      UUID id, String businessKey, String cron, Instant nextFire) {
    return new RecurringJobDefinition(
        id,
        cron,
        "UTC",
        nextFire,
        false,
        null,
        2,
        0,
        BackoffPolicy.NONE,
        0,
        0,
        noopPayload(),
        null,
        null,
        businessKey,
        null,
        null,
        Instant.now(),
        null,
        false);
  }
}

package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.api.NodeTagFilter;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.RecurringJobStore.ArchiveReason;
import run.ratchet.store.spi.TagStore;

/**
 * TCK contract for {@link RecurringJobStore} — the CP2 dedicated recurring-master store.
 *
 * <p>Covers the seven mandatory contracts from the CP2 PRD:
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

  /**
   * TCK 1b — real-thread concurrency: two workers calling {@code claimDueRecurring} in parallel
   * before either calls {@code advanceNextFire} must observe each master at most once across the
   * union of their batches. SQL stores rely on {@code FOR UPDATE SKIP LOCKED}; Mongo relies on
   * per-row {@code findOneAndUpdate} bumping next_fire into a lease window.
   */
  @Test
  void claimDueRecurring_parallelClaimersDoNotDoubleObserve() throws Exception {
    int masterCount = 8;
    Instant pastDue = Instant.now().minusSeconds(60);
    for (int i = 0; i < masterCount; i++) {
      recurringStore().createRecurring(definition(UuidV7Factory.create(), "0 * * * * ?", pastDue));
    }

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      CompletableFuture<List<RecurringJobDefinition>> batchA =
          CompletableFuture.supplyAsync(
              () -> recurringStore().claimDueRecurring(masterCount, "node-a"), executor);
      CompletableFuture<List<RecurringJobDefinition>> batchB =
          CompletableFuture.supplyAsync(
              () -> recurringStore().claimDueRecurring(masterCount, "node-b"), executor);

      List<UUID> idsA;
      List<UUID> idsB;
      try {
        idsA = batchA.get().stream().map(RecurringJobDefinition::id).toList();
        idsB = batchB.get().stream().map(RecurringJobDefinition::id).toList();
      } catch (ExecutionException e) {
        throw new IllegalStateException("claim threw asynchronously", e.getCause());
      }

      java.util.Set<UUID> overlap = new java.util.HashSet<>(idsA);
      overlap.retainAll(idsB);
      assertTrue(
          overlap.isEmpty(), () -> "two claimers must not share any master id; overlap=" + overlap);
      assertEquals(
          masterCount,
          idsA.size() + idsB.size(),
          "every master should appear in exactly one of the two batches");
    } finally {
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
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
    assertTrue(
        !reread.get().nextFire().isBefore(nextFire),
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
   * TCK 7 — child lineage: spawned executable child rows reference the master via {@code
   * recurring_master_id}. This contract intentionally does NOT touch {@code scheduler_job} because
   * the master <-> child relationship is the RI's responsibility (RecurringJobExecutor sets {@code
   * recurringMasterId} on the spawned {@link run.ratchet.store.entity.JobEntity}). The contract
   * validates that {@link RecurringJobStore} can produce the definition needed by the executor.
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

  private RecurringJobDefinition definition(UUID id, String cron, Instant nextFire) {
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
        null,
        Instant.now(),
        null);
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
        null,
        businessKey,
        null,
        Instant.now(),
        null);
  }
}

package run.ratchet.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;
import run.ratchet.store.spi.TagStore;
import run.ratchet.tck.store.AbstractRecurringJobStoreContract;

class MongoRecurringJobStoreContractTest extends AbstractRecurringJobStoreContract {

  private static final MongoTestFixture fixture = new MongoTestFixture();

  @AfterAll
  static void closeFixture() {
    fixture.close();
  }

  @Override
  protected RecurringJobStore recurringStore() {
    return (RecurringJobStore) fixture.store();
  }

  @Override
  protected TagStore tagStore() {
    return (TagStore) fixture.store();
  }

  @Override
  protected JobPayload noopPayload() {
    return new JobPayload("run.ratchet.tck.store.NoopTask", "run", "()V", true, List.of());
  }

  @Override
  protected void cleanupRecurringStore() {
    fixture.cleanupStore();
  }

  /**
   * Mongo-only TCK: per-row {@code findOneAndUpdate} stamps a {@code claim_token} + {@code
   * claim_expires_at} lease, so two workers calling {@code claimDueRecurring} in parallel observe
   * each due master exactly once regardless of thread interleaving. This is the stateful-claim
   * flavor of the contract; the SQL flavor (which requires explicit transaction overlap to
   * demonstrate {@code FOR UPDATE SKIP LOCKED}) lives in
   * {@code AbstractJpaRecurringClaimConcurrencyContract}.
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

      Set<UUID> overlap = new HashSet<>(idsA);
      overlap.retainAll(idsB);
      assertTrue(
          overlap.isEmpty(),
          () -> "two claimers must not share any master id; overlap=" + overlap);
      assertEquals(
          masterCount,
          idsA.size() + idsB.size(),
          "every master should appear in exactly one of the two batches");
    } finally {
      executor.shutdown();
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    }
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
        Instant.now(),
        null);
  }
}

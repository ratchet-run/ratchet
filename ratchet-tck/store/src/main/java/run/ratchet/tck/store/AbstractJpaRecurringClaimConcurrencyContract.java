package run.ratchet.tck.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import run.ratchet.api.BackoffPolicy;
import run.ratchet.store.entity.JobPayload;
import run.ratchet.store.id.UuidV7Factory;
import run.ratchet.store.spi.RecurringJobDefinition;
import run.ratchet.store.spi.RecurringJobStore;

/**
 * SQL-only concurrency contract for {@link RecurringJobStore#claimDueRecurring}.
 *
 * <p>The SQL claim path is a pure {@code SELECT ... FOR UPDATE SKIP LOCKED}: it returns rows but
 * does not modify state. Mutex only holds for the lifetime of the transaction's row locks. The
 * common {@link AbstractRecurringJobStoreContract} cannot exercise this guarantee on a JPA store —
 * its proxy commits each store call, so a serial pair of calls would re-observe the same rows.
 *
 * <p>This contract uses {@link JpaContainerFixture#runInTransaction} to keep two transactions open
 * simultaneously: each worker thread opens a tx, runs the claim SELECT (acquiring row locks), waits
 * at a barrier, then commits. With both locks held in parallel, {@code SKIP LOCKED} forces each row
 * into exactly one claimant.
 *
 * <p>Note: if a store impl ever drops {@code SKIP LOCKED} from its claim SQL, the second worker's
 * SELECT will block on the first worker's row locks, the {@code bothInTx} latch will never reach
 * zero, and this test will fail on latch timeout rather than on the overlap assertion. The failure
 * is still a failure — just diagnosed via timeout rather than via the assertion message.
 */
public abstract class AbstractJpaRecurringClaimConcurrencyContract {

  private static final long LATCH_TIMEOUT_SECONDS = 15;

  /** Returns the JPA fixture so the test can open its own transactions across a barrier. */
  protected abstract JpaContainerFixture fixture();

  /** Returns the {@link RecurringJobStore} under test (typically {@code fixture().store()}). */
  protected abstract RecurringJobStore recurringStore();

  /** Builds a no-op {@link JobPayload} so the contract is store-impl agnostic. */
  protected abstract JobPayload noopPayload();

  /** Removes all rows from the recurring tables. */
  protected abstract void cleanupRecurringStore();

  @BeforeEach
  @AfterEach
  void clean() {
    cleanupRecurringStore();
  }

  /**
   * Two workers calling {@code claimDueRecurring} with their transactions held open across a
   * barrier must observe each due master exactly once. The barrier guarantees both lock-hold
   * windows overlap — without that, a serial BEGIN+SELECT+COMMIT pair on either thread would
   * re-observe every row.
   */
  @Test
  void forUpdateSkipLocked_isolatesConcurrentClaimers() throws Exception {
    int masterCount = 8;
    Instant pastDue = Instant.now().minusSeconds(60);
    for (int i = 0; i < masterCount; i++) {
      recurringStore().createRecurring(definition(UuidV7Factory.create(), "0 * * * * ?", pastDue));
    }

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch bothInTx = new CountDownLatch(2);
    CountDownLatch okToCommit = new CountDownLatch(1);
    try {
      Future<List<UUID>> futureA =
          executor.submit(() -> claimInsideTx("node-a", masterCount, bothInTx, okToCommit));
      Future<List<UUID>> futureB =
          executor.submit(() -> claimInsideTx("node-b", masterCount, bothInTx, okToCommit));

      assertTrue(
          bothInTx.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS),
          "both workers should be inside their tx with row locks held; if this times out, the"
              + " store impl probably lost FOR UPDATE SKIP LOCKED and one worker is blocked on"
              + " the other's row locks");
      okToCommit.countDown();

      List<UUID> idsA = futureA.get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      List<UUID> idsB = futureB.get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      Set<UUID> overlap = new HashSet<>(idsA);
      overlap.retainAll(idsB);
      assertTrue(
          overlap.isEmpty(), () -> "two claimers must not share any master id; overlap=" + overlap);
      assertEquals(
          masterCount,
          idsA.size() + idsB.size(),
          "every master should appear in exactly one of the two batches");
    } catch (ExecutionException e) {
      throw new IllegalStateException("worker threw asynchronously", e.getCause());
    } catch (TimeoutException e) {
      throw new IllegalStateException("worker did not finish before the test timeout", e);
    } finally {
      executor.shutdown();
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    }
  }

  private List<UUID> claimInsideTx(
      String nodeId, int limit, CountDownLatch bothInTx, CountDownLatch okToCommit) {
    List<UUID> ids = new ArrayList<>();
    fixture()
        .runInTransaction(
            () -> {
              List<RecurringJobDefinition> claimed =
                  recurringStore().claimDueRecurring(limit, nodeId);
              for (RecurringJobDefinition def : claimed) {
                ids.add(def.id());
              }
              bothInTx.countDown();
              try {
                if (!okToCommit.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                  throw new IllegalStateException("okToCommit latch timed out inside worker tx");
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("worker interrupted while holding tx", e);
              }
            });
    return ids;
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
}

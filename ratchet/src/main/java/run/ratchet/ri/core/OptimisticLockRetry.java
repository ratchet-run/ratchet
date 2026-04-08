package run.ratchet.ri.core;

import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/**
 * Retry-with-reload helper for idempotent {@code JobCrudStore.save()} call sites that race against
 * concurrent mutations.
 *
 * <p>A save that loses an optimistic-lock race throws {@link RatchetOptimisticLockException}. For
 * call sites whose mutation is idempotent with respect to the row's current state — cascade
 * progression, batch counters, recurring master-state advance — the correct response is to reload
 * the row, reapply the mutation to the fresh copy, and save again. This helper encapsulates that
 * loop with a bounded attempt count and a small backoff.
 *
 * <p><b>Terminal-state guard.</b> If the reloaded row has a terminal status ({@link
 * run.ratchet.store.entity.JobStatus#isTerminal()} returns true — i.e. {@code SUCCEEDED},
 * {@code FAILED}, or {@code CANCELED}), the helper throws instead of reapplying the mutation. A
 * cascade that races with a {@code CANCEL} must NOT silently overwrite the cancellation — the
 * caller's mutation is no longer safe.
 *
 * <p><b>FAILED→PENDING retry paths are NOT safe to wrap.</b> The failure-retry flow that moves a
 * {@code FAILED} job back to {@code PENDING} is a *legitimate* mutation of a terminal row. The
 * helper will throw on the first reload because {@code FAILED.isTerminal()} is true. Callers
 * implementing that flow must call {@code store.save()} directly (with their own retry logic if
 * needed) rather than routing through this helper. There is no opt-out parameter by design —
 * relaxing the terminal guard would defeat its purpose for the cascade/batch use cases.
 *
 * <p><b>Interrupt handling.</b> If the retry backoff sleep is interrupted, the helper restores the
 * interrupt flag and propagates the failure as {@code RatchetOptimisticLockException}. Callers
 * using structured concurrency (e.g. an executor's {@code afterExecute} hook) can observe the
 * interrupt by re-checking {@code Thread.interrupted()}. Callers MUST NOT catch and discard the
 * wrapper exception and continue blocking — doing so would deadlock against shutdown signals.
 *
 * <p><b>Not for insert paths.</b> Initial inserts never race (no prior version exists), so wrapping
 * them in a retry loop has no benefit and will obscure genuine bugs. Use only for
 * reload-mutate-save patterns.
 *
 * <p><b>JPA / JTA compatibility warning.</b> For the MongoDB store this helper works as intended
 * because {@code MongoJobStore.save()} does not participate in a JTA transaction. For the MySQL and
 * PostgreSQL stores, {@code save()} runs inside a JTA-managed {@code EntityManager}, and the first
 * {@code em.flush()} that detects a version conflict causes Hibernate to mark the enclosing
 * transaction {@code rollbackOnly} BEFORE the exception is translated. The helper's reload-and-
 * retry loop is then a no-op: every subsequent {@code em.flush()} on the same {@code EntityManager}
 * immediately throws because the transaction is already doomed. <b>Do not call this helper from
 * within an ambient {@code @Transactional(REQUIRED)} boundary when the store is JPA-backed.</b>
 * Usable patterns for JPA stores: (a) call each save through a dedicated bean method annotated
 * {@code @Transactional(REQUIRES_NEW)} so the retry gets a fresh transaction on each attempt; (b)
 * call from outside any JTA transaction and let the JPA store's internal transaction boundary scope
 * each attempt. Neither is automatic — each call site must be audited. A helper variant that
 * enforces this is tracked for a later 0.2.x slice.
 */
public final class OptimisticLockRetry {

  private static final Logger log = Logger.getLogger(OptimisticLockRetry.class);
  private static final int DEFAULT_MAX_ATTEMPTS = 3;

  private OptimisticLockRetry() {}

  /**
   * Retries an idempotent save-with-mutate operation up to {@value #DEFAULT_MAX_ATTEMPTS} times.
   * See {@link #retryWithReload(int, long, Supplier, Consumer, Function)} for parameter semantics.
   */
  public static JobEntity retryWithReload(
      long jobId,
      Supplier<JobEntity> reload,
      Consumer<JobEntity> mutate,
      Function<JobEntity, JobEntity> save) {
    return retryWithReload(DEFAULT_MAX_ATTEMPTS, jobId, reload, mutate, save);
  }

  /**
   * Retries an idempotent save-with-mutate operation up to {@code maxAttempts} times. On each
   * attempt the helper reloads the entity, invokes {@code mutate} on the fresh copy, and calls
   * {@code save}. If {@code save} throws {@link RatchetOptimisticLockException} the loop reloads
   * and reapplies; if it throws any other exception the exception propagates directly (no retry).
   *
   * @param maxAttempts positive upper bound on save attempts
   * @param jobId id of the job being updated — used only for diagnostics
   * @param reload reloads the row from the store; returning {@code null} is treated as deletion and
   *     results in {@link RatchetOptimisticLockException}
   * @param mutate applies the caller's change to the reloaded row in place
   * @param save persists the mutated row and returns the post-save entity
   * @return the value returned by the successful {@code save} invocation
   * @throws RatchetOptimisticLockException if (a) the reload returns null, (b) the reloaded row's
   *     status is {@link run.ratchet.store.entity.JobStatus#SUCCEEDED SUCCEEDED}, {@link
   *     run.ratchet.store.entity.JobStatus#FAILED FAILED}, or {@link
   *     run.ratchet.store.entity.JobStatus#CANCELED CANCELED} — callers mutating {@code
   *     FAILED→PENDING} in the retry path must call {@code store.save()} directly, (c) {@code
   *     maxAttempts} consecutive save attempts all lose the race, or (d) the retry backoff is
   *     interrupted
   */
  public static JobEntity retryWithReload(
      int maxAttempts,
      long jobId,
      Supplier<JobEntity> reload,
      Consumer<JobEntity> mutate,
      Function<JobEntity, JobEntity> save) {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
    }
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      JobEntity reloaded = reload.get();
      if (reloaded == null) {
        throw new RatchetOptimisticLockException(
            "Job " + jobId + " no longer exists — cannot retry mutation");
      }
      if (reloaded.getStatus() != null && reloaded.getStatus().isTerminal()) {
        // Don't retry against a terminal-state job. A cascade or batch-progression update that
        // races with a CANCEL must not silently overwrite the cancellation.
        // NOTE: This guard also rejects legitimate FAILED→PENDING retry paths. Callers
        // implementing that transition must call store.save() directly rather than routing
        // through this helper — see OptimisticLockRetry class Javadoc for details.
        throw new RatchetOptimisticLockException(
            "Job "
                + jobId
                + " is in terminal state "
                + reloaded.getStatus()
                + " — cannot retry mutation (NOTE: FAILED→PENDING retry paths must call"
                + " store.save() directly, see OptimisticLockRetry Javadoc)");
      }
      try {
        mutate.accept(reloaded);
        return save.apply(reloaded);
      } catch (RatchetOptimisticLockException e) {
        if (attempt == maxAttempts) {
          log.debugf(
              "OptimisticLockRetry exhausted %s attempts on job %s — propagating",
              maxAttempts, jobId);
          throw e;
        }
        // Brief linear backoff. 10 ms is well below typical network round-trip and keeps the
        // retry path invisible to user-facing latencies. The backoff is bounded by
        // maxAttempts, so total wait is bounded above by (10 + 20 + ... + 10*(N-1)) ms.
        try {
          Thread.sleep(10L * attempt);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RatchetOptimisticLockException("Retry interrupted for job " + jobId, ie);
        }
      }
    }
    // Unreachable: loop body always either returns or throws.
    throw new IllegalStateException("OptimisticLockRetry fell out of retry loop unexpectedly");
  }
}

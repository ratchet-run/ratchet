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
 * <p><b>Terminal-state guard.</b> If the reloaded row has transitioned to a terminal status ({@link
 * run.ratchet.store.entity.JobStatus#isTerminal()}), the helper throws instead of
 * reapplying the mutation. A cascade that races with a {@code CANCEL} must NOT silently overwrite
 * the cancellation — the caller's mutation is no longer safe. Callers that DO want to mutate
 * terminal rows (for example, a test cleanup path) must not use this helper.
 *
 * <p><b>Interrupt handling.</b> If the retry backoff sleep is interrupted, the helper restores the
 * interrupt flag and propagates the failure as {@code RatchetOptimisticLockException}. Callers
 * using structured concurrency (e.g. an executor's {@code afterExecute} hook) can observe the
 * interrupt by re-checking {@code Thread.interrupted()}. Callers MUST NOT catch and discard the
 * wrapper exception and continue blocking — doing so would deadlock against shutdown signals.
 *
 * <p><b>Not for insert paths.</b> Initial inserts never race (no prior version exists), so wrapping
 * them in a retry loop has no benefit and will obscure genuine bugs. Use only for reload-mutate-
 * save patterns.
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
   * @throws RatchetOptimisticLockException if (a) the reload returns null, (b) the reloaded row is
   *     in a terminal state, (c) {@code maxAttempts} consecutive save attempts all lose the race,
   *     or (d) the retry backoff is interrupted
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
        throw new RatchetOptimisticLockException(
            "Job "
                + jobId
                + " is in terminal state "
                + reloaded.getStatus()
                + " — cannot retry mutation");
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

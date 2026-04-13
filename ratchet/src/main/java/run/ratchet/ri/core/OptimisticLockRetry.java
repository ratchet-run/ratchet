package run.ratchet.ri.core;

import run.ratchet.api.exception.RatchetOptimisticLockException;
import run.ratchet.store.entity.JobEntity;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

/**
 * Reload-mutate-save retry loop for idempotent optimistic-lock races ({@value
 * #DEFAULT_MAX_ATTEMPTS} attempts, linear backoff). Rejects terminal-state rows to prevent
 * overwriting cancellations. FAILED-to-PENDING retry paths must call {@code store.save()} directly.
 * Restores the interrupt flag if backoff sleep is interrupted.
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
   * @throws RatchetOptimisticLockException if reload returns null, row is terminal, or retries
   *     exhausted
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

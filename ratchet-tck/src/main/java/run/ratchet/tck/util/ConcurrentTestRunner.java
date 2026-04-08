package run.ratchet.tck.util;

import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Runs a fixed set of {@link Runnable} tasks in parallel, synchronising their start on a shared
 * latch, and returns one slot per task containing any {@link Throwable} the task threw (or {@code
 * null} on success).
 *
 * <p>Used by TCK contracts that need to observe the outcome of concurrent operations — notably the
 * stale-write contract in {@code AbstractJobCrudStoreContract}, where exactly one of two racing
 * {@code save()} calls must fail with the store's stale-write type.
 *
 * <p>Pattern mirrors the existing {@code ConcurrentClaimIT} in the MongoDB store: start latch +
 * done latch + fixed-size executor. Deliberately does NOT use {@link java.util.concurrent.Future},
 * because {@code ExecutorService.submit(Runnable)} wraps thrown exceptions in the Future and
 * swallows them unless the caller explicitly queries {@code Future.get()} — which is error-prone
 * for multi-task assertions. Instead, each task writes its own slot in a pre-sized {@code
 * Throwable[]}, which is race-free because indices do not overlap, and visible to the main thread
 * via the happens-before edge from the done latch.
 */
public final class ConcurrentTestRunner {

  private ConcurrentTestRunner() {}

  /**
   * Runs the supplied tasks in parallel. Each task is executed on its own thread, released
   * simultaneously when the shared start latch opens. Returns one slot per task, in the same order
   * the tasks were supplied: {@code null} if the task completed without throwing, otherwise the
   * {@link Throwable} it threw.
   *
   * <p>If any task does not complete within {@code timeout}, this method calls JUnit {@link
   * org.junit.jupiter.api.Assertions#fail(String)} — a timeout is treated as test infrastructure
   * failure, not as a regular task outcome, so callers asserting on the returned list never need to
   * distinguish a timeout slot from a stale-write slot.
   *
   * @param timeout maximum time to wait for all tasks to complete
   * @param tasks tasks to execute concurrently; must be non-null and non-empty
   * @return list of per-task results, one element per task, aligned to input order
   */
  public static List<Throwable> runAll(Duration timeout, Runnable... tasks) {
    if (tasks == null || tasks.length == 0) {
      throw new IllegalArgumentException("tasks must be non-null and non-empty");
    }
    Throwable[] results = new Throwable[tasks.length];
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(tasks.length);
    ExecutorService executor = Executors.newFixedThreadPool(tasks.length);
    try {
      for (int i = 0; i < tasks.length; i++) {
        final int slot = i;
        final Runnable task = tasks[i];
        executor.submit(
            () -> {
              try {
                startLatch.await();
                task.run();
              } catch (Throwable t) {
                results[slot] = t;
              } finally {
                doneLatch.countDown();
              }
            });
      }
      startLatch.countDown();
      boolean completed;
      try {
        completed = doneLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("ConcurrentTestRunner interrupted while awaiting task completion");
        return List.of();
      }
      if (!completed) {
        fail(
            "ConcurrentTestRunner timed out after "
                + timeout
                + " waiting for "
                + tasks.length
                + " task(s) to complete");
      }
      return Arrays.asList(results);
    } finally {
      executor.shutdownNow();
    }
  }
}

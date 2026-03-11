package run.ratchet.api;

/**
 * Internal thread-local scope management for batch job creation.
 *
 * <p>BatchScope provides thread-local storage for maintaining parent-child relationships during
 * batch job creation. When a batch is being constructed, this scope tracks the parent batch ID so
 * that any jobs created within the batch context are automatically associated with the correct
 * parent batch.
 *
 * <h2>Purpose:</h2>
 *
 * <p>This class enables implicit parent-child job relationships without requiring explicit
 * parameter passing. When code executes within a batch context (e.g., inside {@code
 * JobSchedulerService.startBatch()}), any jobs created will automatically be linked to the current
 * batch as child jobs.
 *
 * <h2>Thread Safety:</h2>
 *
 * <p>Uses {@link ThreadLocal} storage to ensure thread-safe operation. Each thread maintains its
 * own batch scope, preventing cross-thread contamination in multi-threaded environments.
 *
 * <h2>Internal Use Only:</h2>
 *
 * <p>This class is intended for internal framework use and is not part of the public API. Client
 * code should use {@link BatchBuilder} and {@link BatchContext} for batch operations.
 *
 * <h2>Lifecycle:</h2>
 *
 * <ol>
 *   <li>{@link #bind(long)} - Called when entering a batch creation context
 *   <li>{@link #active()} - Check if currently in a batch context
 *   <li>{@link #currentParentId()} - Retrieve the current batch parent ID
 *   <li>{@link #clear()} - Called when exiting the batch creation context
 * </ol>
 *
 * @see BatchBuilder
 * @see JobSchedulerService
 */
public final class BatchScope {

  /**
   * Thread-local storage for the current batch parent ID. Each thread can have at most one active
   * batch scope at a time.
   */
  private static final ThreadLocal<Long> PARENT_ID = new ThreadLocal<>();

  /** Private constructor to prevent instantiation of this utility class. */
  private BatchScope() {}

  /**
   * Checks if the current thread is executing within a batch creation context.
   *
   * <p>This method is typically used by job creation logic to determine whether a newly created job
   * should be automatically associated with a parent batch.
   *
   * @return true if the current thread has an active batch scope, false otherwise
   */
  public static boolean active() {
    return PARENT_ID.get() != null;
  }

  /**
   * Binds the current thread to a batch parent ID, establishing a batch creation context.
   *
   * <p>After calling this method, any jobs created on the current thread will be automatically
   * associated with the specified parent batch ID until {@link #clear()} is called.
   *
   * <p>Note: It is critical to call {@link #clear()} in a finally block to prevent thread-local
   * leaks in thread pool environments.
   *
   * @param parentId the ID of the parent batch to bind to the current thread
   */
  public static void bind(long parentId) {
    PARENT_ID.set(parentId);
  }

  /**
   * Clears the batch scope for the current thread.
   *
   * <p>This method must be called when exiting a batch creation context to prevent thread-local
   * memory leaks, especially in thread pool environments where threads are reused.
   *
   * <p>Typical usage pattern:
   *
   * <pre>{@code
   * BatchScope.bind(batchId);
   * try {
   *     // Create child jobs here
   * } finally {
   *     BatchScope.clear();
   * }
   * }</pre>
   */
  public static void clear() {
    PARENT_ID.remove();
  }

  /**
   * Retrieves the current batch parent ID for the calling thread.
   *
   * <p>This method is used by job creation logic to determine which batch a newly created job
   * should be associated with.
   *
   * @return the current batch parent ID, or null if not in a batch context
   */
  public static Long currentParentId() {
    return PARENT_ID.get();
  }
}

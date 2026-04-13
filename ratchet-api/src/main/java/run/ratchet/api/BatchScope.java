package run.ratchet.api;

/**
 * Thread-local scope tracking the current batch parent ID during batch job creation. Internal use
 * only; client code should use {@link BatchBuilder} and {@link BatchContext}.
 *
 * @see BatchBuilder
 * @see JobSchedulerService
 */
public final class BatchScope {

  private static final ThreadLocal<Long> PARENT_ID = new ThreadLocal<>();

  private BatchScope() {}

  public static boolean active() {
    return PARENT_ID.get() != null;
  }

  /**
   * Binds a batch parent ID to the current thread. Always pair with {@link #clear()} in a finally
   * block.
   */
  public static void bind(long parentId) {
    PARENT_ID.set(parentId);
  }

  /** Removes the batch scope from the current thread. */
  public static void clear() {
    PARENT_ID.remove();
  }

  /**
   * @return the current batch parent ID, or null if not in a batch context
   */
  public static Long currentParentId() {
    return PARENT_ID.get();
  }
}

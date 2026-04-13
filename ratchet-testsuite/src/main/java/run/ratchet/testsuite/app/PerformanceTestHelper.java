package run.ratchet.testsuite.app;

/**
 * Strategy for store-specific performance test operations.
 *
 * <p>Performance tests need backend-specific bulk operations (e.g., inserting 100K rows via
 * server-side SQL) and scan diagnostics that can't go through the store SPI. Each store backend
 * provides an implementation with optimized bulk operations.
 *
 * <p>The active implementation is determined by which class is packaged in the test WAR.
 */
public interface PerformanceTestHelper {

  /**
   * Inserts background SUCCEEDED job rows using backend-specific bulk operations.
   *
   * @param count the number of background rows to insert
   * @param keyPrefix prefix for business_key values (e.g., "bg-growth", "bg-claim")
   */
  void insertBackgroundRows(int count, String keyPrefix);

  /**
   * Queries the queue wait time percentile for completed jobs of a given target class.
   *
   * @param targetClass the fully-qualified class name of the job target
   * @param percentile the percentile fraction (e.g., 0.99)
   * @return the queue wait time in milliseconds at the given percentile, or -1 if unavailable
   */
  long queryQueueWaitPercentileForClass(String targetClass, double percentile);

  /**
   * Executes a store operation and asserts that no full collection/table scan occurred.
   *
   * @param label descriptive label for log output and assertion messages
   * @param storeOperation the actual store method call to verify
   */
  void assertNoFullScan(String label, Runnable storeOperation);
}

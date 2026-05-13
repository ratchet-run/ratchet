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

  void insertBackgroundRows(int count, String keyPrefix);

  /**
   * Returns the queue wait time in milliseconds at the given percentile.
   *
   * @return the percentile value, or {@code 0} when no matching completed jobs exist
   */
  long queryQueueWaitPercentileForClass(String targetClass, double percentile);

  void assertNoFullScan(String label, Runnable storeOperation);
}

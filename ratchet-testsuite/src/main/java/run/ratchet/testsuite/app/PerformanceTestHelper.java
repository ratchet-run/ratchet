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
   * Inserts background SUCCEEDED job rows using backend-specific bulk operations. Uses optimized
   * server-side generation (e.g., {@code generate_series()} on PostgreSQL, recursive CTEs on MySQL,
   * bulk insert APIs on MongoDB) for maximum throughput.
   *
   * <p>After insertion, refreshes backend statistics so the query planner has accurate estimates.
   *
   * @param count the number of background rows to insert
   * @param keyPrefix prefix for business_key values (e.g., "bg-growth", "bg-claim")
   */
  void insertBackgroundRows(int count, String keyPrefix);

  /**
   * Queries the queue wait time percentile for jobs of a given target class that have completed
   * successfully. Uses backend-specific query APIs.
   *
   * @param targetClass the fully-qualified class name of the job target
   * @param percentile the percentile fraction (e.g., 0.99)
   * @return the queue wait time in milliseconds at the given percentile, or -1 if unavailable
   */
  long queryQueueWaitPercentileForClass(String targetClass, double percentile);

  /**
   * Executes a store operation and asserts that no full collection/table scan occurred. Uses
   * backend-specific scan diagnostics to verify index usage.
   *
   * <p>Implementations may log scan statistics or assert on per-table metrics where available. On
   * backends where per-table scan metrics are not available (e.g., MySQL session-wide counters),
   * the operation is still executed but the assertion may be informational only.
   *
   * @param label descriptive label for log output and assertion messages
   * @param storeOperation the actual store method call to verify
   */
  void assertNoFullScan(String label, Runnable storeOperation);
}

/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
   * Inserts completed (terminal) background rows that grow the cold archive without becoming
   * claimable. Used by the table-growth IT. {@code baseOffset} is the number of background rows
   * already inserted across prior calls, so row numbering (and ids) stay unique across the IT's
   * incremental growth steps.
   */
  void insertTerminalBackgroundRows(int count, int baseOffset, String keyPrefix);

  /**
   * Inserts live PENDING background rows that grow the hot claim queue. Rows are far-future so a
   * running poller never drains them. Used by the claim-degradation IT. {@code baseOffset} is the
   * number of background rows already inserted across prior calls; the deterministic job ids derive
   * from {@code baseOffset + row}, so it must advance between calls or ids collide.
   */
  void insertPendingBackgroundRows(int count, int baseOffset, String keyPrefix);

  /**
   * Returns the queue wait time in milliseconds at the given percentile.
   *
   * @return the percentile value, or {@code 0} when no matching completed jobs exist
   */
  long queryQueueWaitPercentileForClass(String targetClass, double percentile);

  /** Runs {@code storeOperation} and asserts it triggered no sequential scan on {@code table}. */
  void assertNoFullScan(String table, String label, Runnable storeOperation);
}

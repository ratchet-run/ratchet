package run.ratchet.testsuite.app;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test deployment helper used by batch integration tests to record processed items across the
 * Arquillian deployment boundary.
 *
 * <p>State is intentionally static because job execution happens inside the test deployment while
 * assertions run from the test runner. Batch tests must call {@link #reset()} from
 * {@code @BeforeEach}; these helpers are not safe for concurrently executed test methods that share
 * the same JVM/deployment.
 */
public class BatchItemProcessor {

  private static final Set<String> PROCESSED_ITEMS = ConcurrentHashMap.newKeySet();
  private static final String FAILING_ITEM = "s3";

  public static void process(String item) {
    PROCESSED_ITEMS.add(item);
  }

  public static void failOnS3(String item) {
    PROCESSED_ITEMS.add(item);
    if (FAILING_ITEM.equals(item)) {
      throw new IllegalStateException("boom: " + item);
    }
  }

  public static void failOnBatchFailureItem(String item) {
    if ("fail".equals(item)) {
      FailingJob.execute();
    }
    process(item);
  }

  public static Set<String> processedItems() {
    return Set.copyOf(PROCESSED_ITEMS);
  }

  public static int processedCount() {
    return PROCESSED_ITEMS.size();
  }

  /** Clears static test state before each batch test method. */
  public static void reset() {
    PROCESSED_ITEMS.clear();
  }
}

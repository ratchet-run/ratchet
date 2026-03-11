package run.ratchet.testsuite.app;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks batch item processing for integration tests.
 *
 * <p>Each call to {@link #process(String)} records the item in a thread-safe set that tests can
 * query after batch completion.
 */
public class BatchItemProcessor {

  private static final Set<String> PROCESSED_ITEMS = ConcurrentHashMap.newKeySet();

  public static void process(String item) {
    PROCESSED_ITEMS.add(item);
  }

  public static Set<String> processedItems() {
    return Set.copyOf(PROCESSED_ITEMS);
  }

  public static int processedCount() {
    return PROCESSED_ITEMS.size();
  }

  public static void reset() {
    PROCESSED_ITEMS.clear();
  }
}

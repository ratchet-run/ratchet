package run.ratchet.testsuite.app;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

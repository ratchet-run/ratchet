package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link JobTask#newBoundedCache(int)} produces an LRU map that evicts the eldest
 * (least-recently-accessed) entry once the size cap is exceeded, keeping reflection caches bounded
 * across the lifetime of a long-running deployment.
 */
class JobTaskBoundedCacheTest {

  @Test
  void doesNotGrowBeyondCap() {
    Map<Integer, String> cache = JobTask.newBoundedCache(3);

    for (int i = 0; i < 10; i++) {
      cache.put(i, "v" + i);
    }

    assertEquals(3, cache.size(), "Cache must never exceed its declared maximum");
  }

  @Test
  void evictsLeastRecentlyAccessedEntry() {
    Map<Integer, String> cache = JobTask.newBoundedCache(3);

    cache.put(1, "a");
    cache.put(2, "b");
    cache.put(3, "c");

    // Touching 1 promotes it; 2 is now the LRU candidate.
    assertNotNull(cache.get(1));

    // Inserting a 4th entry must evict the LRU (2), not the freshly-touched 1.
    cache.put(4, "d");

    assertEquals(3, cache.size());
    assertTrue(cache.containsKey(1), "Recently accessed entry must survive eviction");
    assertFalse(cache.containsKey(2), "Least-recently accessed entry must be evicted");
    assertTrue(cache.containsKey(3));
    assertTrue(cache.containsKey(4));
  }

  @Test
  void supportsBasicGetPutSemantics() {
    Map<String, Integer> cache = JobTask.newBoundedCache(8);
    assertNull(cache.get("missing"));
    cache.put("k", 42);
    assertEquals(42, cache.get("k"));
    assertEquals(1, cache.size());
  }

  @Test
  void supportsStoredNullValues() {
    Map<String, String> cache = JobTask.newBoundedCache(2);

    cache.put("present-null", null);

    assertTrue(cache.containsKey("present-null"));
    assertNull(cache.get("present-null"));
    assertFalse(cache.containsKey("missing"));
    assertNull(cache.get("missing"));
  }

  @Test
  void maxEntriesOne_evictsPreviousEntryOnEveryNewKey() {
    Map<Integer, String> cache = JobTask.newBoundedCache(1);

    cache.put(1, "a");
    cache.put(2, "b");

    assertEquals(1, cache.size());
    assertFalse(cache.containsKey(1));
    assertEquals("b", cache.get(2));

    cache.put(3, "c");

    assertEquals(1, cache.size());
    assertFalse(cache.containsKey(2));
    assertEquals("c", cache.get(3));
  }
}

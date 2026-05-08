package run.ratchet.ri.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link JobTask#newBoundedCache(int)} produces a thread-safe LRU map that evicts the
 * eldest (least-recently-accessed) entry once the size cap is exceeded — protecting reflection
 * caches from unbounded growth across the lifetime of a long-running deployment.
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
  void remainsBoundedUnderConcurrentAccess() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          int maxEntries = 32;
          int threadCount = 8;
          int iterationsPerThread = 2_000;
          Map<Integer, String> cache = JobTask.newBoundedCache(maxEntries);
          CountDownLatch start = new CountDownLatch(1);
          ExecutorService executor = Executors.newFixedThreadPool(threadCount);

          try {
            List<Future<?>> futures = new ArrayList<>();
            for (int thread = 0; thread < threadCount; thread++) {
              int threadOffset = thread;
              futures.add(
                  executor.submit(
                      () -> {
                        start.await();
                        for (int i = 0; i < iterationsPerThread; i++) {
                          int key = (i + threadOffset) % (maxEntries * 4);
                          cache.put(key, "v" + key);
                          cache.get((key + maxEntries) % (maxEntries * 4));
                          cache.containsKey((key + 1) % (maxEntries * 4));
                          assertTrue(cache.size() <= maxEntries);
                        }
                        return null;
                      }));
            }

            start.countDown();
            for (Future<?> future : futures) {
              future.get();
            }

            assertTrue(cache.size() <= maxEntries, "Cache must remain bounded after contention");
            synchronized (cache) {
              cache.forEach((key, value) -> assertEquals("v" + key, value));
            }
          } finally {
            executor.shutdownNow();
          }
        });
  }
}

package run.ratchet.store.id;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class UuidV7FactoryTest {

  @Test
  void monotonicWithinMillisecond() {
    UUID prev = UuidV7Factory.create();
    for (int i = 0; i < 100; i++) {
      UUID next = UuidV7Factory.create();
      UUID p = prev;
      assertTrue(next.compareTo(p) > 0, () -> "UUIDv7 not monotonic: prev=" + p + " next=" + next);
      prev = next;
    }
  }

  @Test
  void timestampRoundtrip() {
    UUID id = UuidV7Factory.create();
    long observedNow = System.currentTimeMillis();
    long extracted = (id.getMostSignificantBits() >>> 16) & 0xFFFF_FFFF_FFFFL;
    long delta = observedNow - extracted;
    assertTrue(
        delta >= 0 && delta < 5, () -> "timestamp delta out of range (expected 0..5 ms): " + delta);
  }

  @Test
  void versionAndVariantBits() {
    UUID id = UuidV7Factory.create();
    long version = (id.getMostSignificantBits() >>> 12) & 0xF;
    assertEquals(7L, version, "version nibble must be 7");
    long variant = (id.getLeastSignificantBits() >>> 62) & 0x3;
    assertEquals(0b10L, variant, "variant bits must be 10");
  }

  @Test
  void noCollisionsAcrossThreads() throws InterruptedException, ExecutionException {
    int threads = 32;
    int idsPerThread = 31_250; // 32 * 31_250 = 1_000_000
    int total = threads * idsPerThread;
    Set<UUID> seen = ConcurrentHashMap.newKeySet(total);
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> futures = new ArrayList<>(threads);
      for (int t = 0; t < threads; t++) {
        futures.add(
            pool.submit(
                () -> {
                  for (int i = 0; i < idsPerThread; i++) {
                    seen.add(UuidV7Factory.create());
                  }
                }));
      }
      for (Future<?> f : futures) {
        f.get();
      }
    } finally {
      pool.shutdown();
    }
    assertEquals(total, seen.size(), "collision detected across 32 threads × 31_250 IDs each");
  }

  @Test
  void counterOverflowWaitsForNextTick() {
    // n must be large enough that on fast hardware (sub-microsecond per call) the loop spans
    // multiple wall-clock milliseconds AND forces counter overflow (>4096 IDs in some ms).
    // 5000 was insufficient — at 200 ns/call on modern hardware the loop finishes in <1 ms
    // before the counter reaches 4096, so wait-for-tick never triggers and elapsed=0 ms.
    // 50_000 guarantees multiple ms boundaries are crossed regardless of call-rate.
    int n = 50_000;
    UUID[] ids = new UUID[n];
    long startNs = System.nanoTime();
    for (int i = 0; i < n; i++) {
      ids[i] = UuidV7Factory.create();
    }
    long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;
    assertTrue(
        elapsedMs >= 1,
        () ->
            "expected at least one ms wall-clock advance for "
                + n
                + " IDs; got "
                + elapsedMs
                + "ms");
    for (int i = 1; i < n; i++) {
      UUID a = ids[i - 1];
      UUID b = ids[i];
      assertTrue(b.compareTo(a) > 0, () -> "IDs not strictly increasing across overflow");
    }
  }

  @Test
  void clockBackwardStepPreservesMonotonicTimestampAndCounter() throws Exception {
    Field lockField = UuidV7Factory.class.getDeclaredField("LOCK");
    lockField.setAccessible(true);
    Object lock = lockField.get(null);

    Field lastTimestampField = UuidV7Factory.class.getDeclaredField("lastTimestampMs");
    lastTimestampField.setAccessible(true);
    Field counterField = UuidV7Factory.class.getDeclaredField("counter");
    counterField.setAccessible(true);

    long originalTimestamp;
    int originalCounter;
    long futureTimestamp = System.currentTimeMillis() + 10_000L;
    int previousCounter = 7;
    synchronized (lock) {
      originalTimestamp = lastTimestampField.getLong(null);
      originalCounter = counterField.getInt(null);
      lastTimestampField.setLong(null, futureTimestamp);
      counterField.setInt(null, previousCounter);
    }

    try {
      UUID id = UuidV7Factory.create();

      assertEquals(futureTimestamp, timestampFrom(id));
      assertEquals(previousCounter + 1, counterFrom(id));
    } finally {
      synchronized (lock) {
        lastTimestampField.setLong(null, originalTimestamp);
        counterField.setInt(null, originalCounter);
      }
    }
  }

  private static long timestampFrom(UUID id) {
    return (id.getMostSignificantBits() >>> 16) & 0xFFFF_FFFF_FFFFL;
  }

  private static int counterFrom(UUID id) {
    return (int) (id.getMostSignificantBits() & 0x0FFFL);
  }
}

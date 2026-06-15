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
  void counterOverflowWaitsForNextTick() throws Exception {
    // The previous version timed 50_000 create() calls and only asserted elapsed >= 1ms, which the
    // normal path satisfies by crossing ms boundaries — the overflow branch (counter > COUNTER_MAX
    // -> wait-for-tick + reset) never ran. Saturate the per-ms counter on the current ms via
    // reflection so the very next create() must take that branch, mirroring the clock-backward
    // sibling's setup.
    Field lockField = UuidV7Factory.class.getDeclaredField("LOCK");
    lockField.setAccessible(true);
    Object lock = lockField.get(null);

    Field lastTimestampField = UuidV7Factory.class.getDeclaredField("lastTimestampMs");
    lastTimestampField.setAccessible(true);
    Field counterField = UuidV7Factory.class.getDeclaredField("counter");
    counterField.setAccessible(true);
    Field counterMaxField = UuidV7Factory.class.getDeclaredField("COUNTER_MAX");
    counterMaxField.setAccessible(true);
    int counterMax = counterMaxField.getInt(null);

    long originalTimestamp;
    int originalCounter;
    long pinnedTimestamp = System.currentTimeMillis();
    synchronized (lock) {
      originalTimestamp = lastTimestampField.getLong(null);
      originalCounter = counterField.getInt(null);
      lastTimestampField.setLong(null, pinnedTimestamp);
      counterField.setInt(null, counterMax);
    }

    try {
      UUID id = UuidV7Factory.create();

      assertTrue(
          timestampFrom(id) > pinnedTimestamp,
          () -> "overflow must advance the embedded timestamp past the saturated ms");
      assertEquals(0, counterFrom(id), "counter must reset to 0 after wait-for-tick overflow");
    } finally {
      synchronized (lock) {
        lastTimestampField.setLong(null, originalTimestamp);
        counterField.setInt(null, originalCounter);
      }
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

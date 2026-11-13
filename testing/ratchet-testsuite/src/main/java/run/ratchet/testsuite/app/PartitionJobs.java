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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import run.ratchet.api.JobContext;
import run.ratchet.api.KeyRange;

/** Deterministic partition attempts and checkpoints for integration tests. */
public final class PartitionJobs {
  public static final Map<Long, AtomicInteger> calls = new ConcurrentHashMap<>();
  public static final Map<Long, String> firstCheckpoint = new ConcurrentHashMap<>();
  private static volatile boolean fail;
  private static volatile boolean block;
  private static volatile CountDownLatch gate = new CountDownLatch(0);

  private PartitionJobs() {}

  public static void reset(boolean shouldFail, boolean shouldBlock) {
    calls.clear();
    firstCheckpoint.clear();
    fail = shouldFail;
    block = shouldBlock;
    gate = new CountDownLatch(shouldBlock ? 1 : 0);
  }

  public static void release() {
    block = false;
    gate.countDown();
  }

  public static void succeed() {
    fail = false;
  }

  public static void process(KeyRange range) {
    calls.computeIfAbsent(range.from(), ignored -> new AtomicInteger()).incrementAndGet();
    firstCheckpoint.put(range.from(), JobContext.current().lastCheckpoint().orElse("none"));
    if (block) {
      try {
        if (!gate.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Gate timed out");
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(failure);
      }
    }
    if (range.from() == 20 && fail) {
      JobContext.current().checkpoint("25");
      throw new IllegalStateException("Partition failure for resume test");
    }
  }
}

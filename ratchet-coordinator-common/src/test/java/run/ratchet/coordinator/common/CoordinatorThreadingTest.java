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
package run.ratchet.coordinator.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CoordinatorThreadingTest {

  @Test
  void managedPathDelegatesToTheSuppliedFactory() {
    AtomicInteger calls = new AtomicInteger();
    Thread stubThread = new Thread(() -> {});
    ThreadFactory stub =
        runnable -> {
          calls.incrementAndGet();
          return stubThread;
        };

    CoordinatorThreading threading = CoordinatorThreading.managed("test", stub);

    assertTrue(threading.isManaged());
    Thread produced = threading.newLoopThread("loop", () -> {});
    assertSame(stubThread, produced, "managed loop thread must come from the injected factory");
    assertEquals(1, calls.get());
    // newLoopThread renames and daemonizes the factory's thread.
    assertTrue(produced.getName().startsWith("test-loop-"));
    assertTrue(produced.isDaemon());
  }

  @Test
  void managedDispatchPoolUsesTheSuppliedFactory() throws InterruptedException {
    AtomicInteger calls = new AtomicInteger();
    ThreadFactory counting =
        runnable -> {
          calls.incrementAndGet();
          return new Thread(runnable);
        };
    CoordinatorThreading threading = CoordinatorThreading.managed("test", counting);

    ExecutorService pool = threading.newDispatchPool("dispatch", 1, 8);
    try {
      pool.submit(() -> {}).get(2, java.util.concurrent.TimeUnit.SECONDS);
    } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
      throw new AssertionError(e);
    }
    pool.shutdownNow();
    assertTrue(calls.get() >= 1, "dispatch pool must source its worker from the injected factory");
  }

  @Test
  void managedPathThrowsOnJndiMiss() {
    // No InitialContext is available in plain SE, so the well-known managed factory name cannot be
    // resolved and managed mode must fail loudly rather than fall back to raw threads.
    CoordinatorThreading threading = CoordinatorThreading.managed("test");
    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> threading.newLoopThread("loop", () -> {}));
    assertTrue(
        ex.getMessage().contains("DefaultManagedThreadFactory"),
        "JNDI-miss message should name the well-known factory: " + ex.getMessage());
  }

  @Test
  void standalonePathReturnsDaemonThreads() {
    CoordinatorThreading threading = CoordinatorThreading.standalone("test");

    assertFalse(threading.isManaged());
    Thread thread = threading.newLoopThread("loop", () -> {});
    assertNotNull(thread);
    assertTrue(thread.isDaemon(), "standalone loop threads must be daemon");
    assertTrue(thread.getName().startsWith("test-loop-"));
  }

  @Test
  void standaloneDispatchPoolRuns() throws Exception {
    CoordinatorThreading threading = CoordinatorThreading.standalone("test");
    ExecutorService pool = threading.newDispatchPool("dispatch", 2, 16);
    try {
      AtomicInteger ran = new AtomicInteger();
      pool.submit(ran::incrementAndGet).get(2, java.util.concurrent.TimeUnit.SECONDS);
      assertEquals(1, ran.get());
    } finally {
      pool.shutdownNow();
    }
  }
}

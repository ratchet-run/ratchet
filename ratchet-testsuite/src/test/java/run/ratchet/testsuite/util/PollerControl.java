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
package run.ratchet.testsuite.util;

import static org.awaitility.Awaitility.await;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.Future;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.ri.core.internal.DefaultPollerScheduler;

/**
 * Quiesces the engine's background poller for integration tests that drive the store directly.
 *
 * <p>The full reference implementation deploys into the Arquillian container, and {@code
 * RatchetLifecycle} auto-starts the poller. A test that calls {@code claimNextBatchOptimized}
 * itself is then racing that poller for the same PENDING rows: when a poll tick lands between
 * inserting a job and the test's own claim, the poller consumes the job and the direct claim sees
 * nothing. Stopping the poller (and waiting for any in-flight cycle to finish) removes the race so
 * the store sees only the test's own claims. It also avoids a {@code TRUNCATE} deadlocking against
 * a live poll cycle's row locks.
 */
public final class PollerControl {

  private static final Duration STOP_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration STOP_POLL_INTERVAL = Duration.ofMillis(10);

  private PollerControl() {}

  /** Stops the poller and blocks until no cycle is running and no future cycle is scheduled. */
  public static void stopAndAwait(PollerScheduler scheduler) {
    scheduler.stop();
    await().atMost(STOP_TIMEOUT).pollInterval(STOP_POLL_INTERVAL).until(() -> isStopped(scheduler));
  }

  /**
   * Returns {@code true} when the poller has no cycle currently running and no scheduled handle
   * pending. Reads {@link DefaultPollerScheduler} internals reflectively because the SPI exposes no
   * idle-state query.
   */
  public static boolean isStopped(PollerScheduler scheduler) {
    Object lock = readField(scheduler, "scheduleLock", Object.class);
    synchronized (lock) {
      Future<?> handle = readField(scheduler, "handle", Future.class);
      return !readField(scheduler, "cycleRunning", Boolean.class)
          && (handle == null || handle.isDone() || handle.isCancelled());
    }
  }

  private static <T> T readField(Object target, String name, Class<T> type) {
    try {
      Field field = DefaultPollerScheduler.class.getDeclaredField(name);
      field.setAccessible(true);
      return type.cast(field.get(target));
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to read " + name + " from " + target.getClass(), e);
    }
  }
}

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
package run.ratchet.ri.core.internal;

import static org.jboss.logging.Logger.Level.WARN;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Supplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.internal.ExecutorProviderClosedException;
import run.ratchet.store.spi.JobRetryStore;

class ReleaseSchedulingShutdownTest {
  @Test
  void shutDownExecutorLogsBothHandOffsAtInfo() {
    ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
    executor.shutdownNow();
    checkStopping(() -> executor);
  }

  @Test
  void closedProviderLogsBothHandOffsAtInfo() {
    checkStopping(
        () -> {
          throw new ExecutorProviderClosedException("provider is shut down");
        });
  }

  private void checkStopping(Supplier<ScheduledExecutorService> executor) {
    try (Capture releases = new Capture(ReleaseRetries.class);
        Capture claims = new Capture(ClaimReschedules.class)) {
      ReleaseRetries retries =
          new ReleaseRetries(
              executor, ReleaseRetries.Policy.bounded(Duration.ofSeconds(1), 2), System::nanoTime);
      try {
        Thread.currentThread().interrupt();
        assertDoesNotThrow(
            () ->
                retries.release(
                    new ReleaseRetries.Release("permit", "node cleanup", WARN, () -> {})));
      } finally {
        Thread.interrupted();
      }
      ClaimReschedules reschedules = new ClaimReschedules(mock(JobRetryStore.class), executor);
      assertFalse(
          assertDoesNotThrow(
              () ->
                  reschedules.handOff(
                      UUID.randomUUID(), 1, "release", Instant.now(), 0, () -> false, () -> {})));
      for (Capture capture : List.of(releases, claims)) {
        assertEquals(1, capture.records.size());
        LogRecord record = capture.records.get(0);
        assertEquals(Level.INFO, record.getLevel());
        assertTrue(record.getMessage().contains("This node is stopping"));
        assertTrue(record.getMessage().contains("left to"));
      }
    }
  }

  private static final class Capture implements AutoCloseable {
    final List<LogRecord> records = new ArrayList<>();
    final Logger logger;
    final Level previousLevel;
    final boolean previousParents;
    final Handler handler =
        new Handler() {
          public void publish(LogRecord record) {
            records.add(record);
          }

          public void flush() {}

          public void close() {}
        };

    Capture(Class<?> type) {
      logger = Logger.getLogger(type.getName());
      previousLevel = logger.getLevel();
      previousParents = logger.getUseParentHandlers();
      logger.setLevel(Level.ALL);
      logger.setUseParentHandlers(false);
      logger.addHandler(handler);
    }

    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
      logger.setUseParentHandlers(previousParents);
    }
  }
}

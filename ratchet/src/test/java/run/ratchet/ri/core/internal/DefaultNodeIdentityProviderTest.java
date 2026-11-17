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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import run.ratchet.spi.ExecutorProvider;

class DefaultNodeIdentityProviderTest {
  private final ExecutorProvider executorProvider = mock(ExecutorProvider.class);

  @Test
  void explicitIdIsStable() {
    DefaultNodeIdentityProvider provider =
        new DefaultNodeIdentityProvider(executorProvider, "test-node");
    assertEquals("test-node", provider.getNodeId());
    assertEquals("test-node", provider.getNodeId());
    verifyNoInteractions(executorProvider);
  }

  @Test
  void hostnameFallbackLogsThrowable() {
    RuntimeException failure = new RuntimeException("executor unavailable");
    when(executorProvider.getJobExecutor()).thenThrow(failure);
    DefaultNodeIdentityProvider provider = new DefaultNodeIdentityProvider(executorProvider, null);

    LogCapture logs = LogCapture.start(DefaultNodeIdentityProvider.class);
    try {
      provider.getNodeId();
    } finally {
      logs.close();
    }

    UUID.fromString(provider.getNodeId());
    assertTrue(
        logs.records().stream()
            .anyMatch(
                record ->
                    record.getLevel().intValue() >= Level.WARNING.intValue()
                        && record.getThrown() == failure
                        && record.getMessage().contains("Hostname resolution error")),
        "Hostname fallback warning should include the original exception");
  }

  private static final class LogCapture implements AutoCloseable {
    private final Logger logger;
    private final Handler handler;
    private final Level originalLevel;
    private final boolean originalUseParentHandlers;
    private final List<LogRecord> records = new ArrayList<>();

    private LogCapture(Class<?> type) {
      logger = Logger.getLogger(type.getName());
      originalLevel = logger.getLevel();
      originalUseParentHandlers = logger.getUseParentHandlers();
      handler =
          new Handler() {
            @Override
            public void publish(LogRecord record) {
              records.add(record);
            }

            @Override
            public void flush() {}

            @Override
            public void close() {}
          };
      logger.setLevel(Level.ALL);
      logger.setUseParentHandlers(false);
      logger.addHandler(handler);
    }

    static LogCapture start(Class<?> type) {
      return new LogCapture(type);
    }

    List<LogRecord> records() {
      return records;
    }

    @Override
    public void close() {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
      logger.setUseParentHandlers(originalUseParentHandlers);
    }
  }
}

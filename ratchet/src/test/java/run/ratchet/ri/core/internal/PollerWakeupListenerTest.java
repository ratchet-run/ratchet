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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import run.ratchet.api.JobPriority;
import run.ratchet.api.NodeIdentity;
import run.ratchet.ri.core.PollerScheduler;
import run.ratchet.spi.ClusterCoordinator;
import run.ratchet.spi.JobWakeupHint;
import run.ratchet.spi.MetricsCollector;

@ExtendWith(MockitoExtension.class)
class PollerWakeupListenerTest {

  @Mock private ClusterCoordinator clusterCoordinator;
  @Mock private PollerScheduler pollerScheduler;
  @Mock private MetricsCollector metricsCollector;

  @Test
  void init_registersListenerThatWakesPollerAndRecordsMetric() {
    AtomicReference<Consumer<JobWakeupHint>> listenerRef = new AtomicReference<>();
    doAnswer(
            invocation -> {
              listenerRef.set(invocation.getArgument(0));
              return null;
            })
        .when(clusterCoordinator)
        .registerWakeupListener(any());

    PollerWakeupListener listener =
        new PollerWakeupListener(clusterCoordinator, pollerScheduler, metricsCollector);

    listener.init();
    listenerRef
        .get()
        .accept(new JobWakeupHint(JobPriority.NORMAL, new NodeIdentity("node-1"), null));

    verify(metricsCollector).localWakeup("cluster_listener");
    verify(pollerScheduler).wakeup();
  }

  @Test
  void init_registrationFailureDoesNotPropagate() {
    doThrow(new RuntimeException("cluster unavailable"))
        .when(clusterCoordinator)
        .registerWakeupListener(any());
    PollerWakeupListener listener =
        new PollerWakeupListener(clusterCoordinator, pollerScheduler, metricsCollector);

    assertDoesNotThrow(listener::init);

    verify(pollerScheduler, never()).wakeup();
    verify(metricsCollector, never()).localWakeup(any());
  }

  @Test
  void init_registrationFailureLogsThrowableWithCauseChain() {
    RuntimeException rootCause = new RuntimeException("jndi missing");
    IllegalStateException failure = new IllegalStateException("coordinator init failed", rootCause);
    doThrow(failure).when(clusterCoordinator).registerWakeupListener(any());
    PollerWakeupListener listener =
        new PollerWakeupListener(clusterCoordinator, pollerScheduler, metricsCollector);

    try (LogCapture logs = LogCapture.start(PollerWakeupListener.class)) {
      listener.init();

      LogRecord record =
          logs.records().stream()
              .filter(log -> log.getMessage().contains("Wakeup listener registration error"))
              .findFirst()
              .orElseThrow();
      assertSame(failure, record.getThrown());
      assertSame(rootCause, record.getThrown().getCause());
      assertEquals(
          "Wakeup listener registration error — polling continues without push notifications",
          record.getMessage());
      assertTrue(
          record.getParameters() == null || record.getParameters().length == 0,
          "Throwable must be attached to the log record, not supplied as a formatting parameter");
      assertTrue(record.getLevel().intValue() >= Level.SEVERE.intValue());
    }
  }

  @Test
  void registeredListener_wakeupFailureDoesNotPropagate() {
    AtomicReference<Consumer<JobWakeupHint>> listenerRef = new AtomicReference<>();
    doAnswer(
            invocation -> {
              listenerRef.set(invocation.getArgument(0));
              return null;
            })
        .when(clusterCoordinator)
        .registerWakeupListener(any());
    doThrow(new RuntimeException("scheduler stopped")).when(pollerScheduler).wakeup();
    PollerWakeupListener listener =
        new PollerWakeupListener(clusterCoordinator, pollerScheduler, metricsCollector);

    listener.init();
    assertNotNull(listenerRef.get());
    assertDoesNotThrow(
        () ->
            listenerRef
                .get()
                .accept(new JobWakeupHint(JobPriority.HIGH, new NodeIdentity("node-2"), null)));

    verify(metricsCollector).localWakeup("cluster_listener");
    verify(pollerScheduler).wakeup();
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

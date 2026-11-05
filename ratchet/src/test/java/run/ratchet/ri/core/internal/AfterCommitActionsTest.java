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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import run.ratchet.spi.AfterCommitRegistrar;
import run.ratchet.spi.AfterCommitRegistrar.Result;

class AfterCommitActionsTest {
  @ParameterizedTest
  @EnumSource(Result.class)
  void runOrFailAppliesRegistrationPolicy(Result result) {
    Runnable action = mock(Runnable.class);
    if (result == Result.REGISTRATION_FAILED) {
      assertThrows(
          IllegalStateException.class,
          () -> AfterCommitActions.runOrFail(ignored -> result, "test event", action));
    } else {
      assertDoesNotThrow(
          () -> AfterCommitActions.runOrFail(ignored -> result, "test event", action));
    }
    verify(action, times(result == Result.NO_ACTIVE_TRANSACTION ? 1 : 0)).run();
  }

  @ParameterizedTest
  @EnumSource(Result.class)
  void runOrWarnAppliesRegistrationPolicy(Result result) {
    Runnable action = mock(Runnable.class);
    try (LogCapture logs = LogCapture.start(AfterCommitActions.class)) {
      assertDoesNotThrow(
          () -> AfterCommitActions.runOrWarn(ignored -> result, "test event", action));
      verify(action, times(result == Result.NO_ACTIVE_TRANSACTION ? 1 : 0)).run();
      assertEquals(
          result == Result.REGISTRATION_FAILED ? 1L : 0L,
          logs.records().stream()
              .filter(record -> record.getLevel().intValue() >= Level.WARNING.intValue())
              .count());
    }
  }

  @Test
  void runOrWarnSuppressesRegistrarException() {
    Runnable action = mock(Runnable.class);
    AfterCommitRegistrar registrar =
        ignored -> {
          throw new IllegalStateException("boom");
        };

    assertDoesNotThrow(() -> AfterCommitActions.runOrWarn(registrar, "test event", action));

    verify(action, times(0)).run();
  }

  @Test
  void runOrWarnSuppressesNullResult() {
    Runnable action = mock(Runnable.class);
    AfterCommitRegistrar registrar = ignored -> null;

    assertDoesNotThrow(() -> AfterCommitActions.runOrWarn(registrar, "test event", action));

    verify(action, times(0)).run();
  }

  @Test
  void runOrFailPropagatesRegistrarException() {
    Runnable action = mock(Runnable.class);
    IllegalStateException failure = new IllegalStateException("boom");
    AfterCommitRegistrar registrar =
        ignored -> {
          throw failure;
        };

    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () -> AfterCommitActions.runOrFail(registrar, "test event", action)));

    verify(action, times(0)).run();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void transactionCompletionOnForeignThreadUsesRegisteringLoader(boolean warn) throws Exception {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader application = new URLClassLoader(new URL[0], previous);
        URLClassLoader foreign = new URLClassLoader(new URL[0], null)) {
      AtomicReference<Runnable> registered = new AtomicReference<>();
      AtomicReference<ClassLoader> observed = new AtomicReference<>();
      AfterCommitRegistrar registrar =
          action -> {
            registered.set(action);
            return Result.REGISTERED;
          };
      Thread.currentThread().setContextClassLoader(application);
      Runnable action =
          () -> {
            observed.set(Thread.currentThread().getContextClassLoader());
            throw new IllegalStateException("completion callback failed");
          };
      if (warn) AfterCommitActions.runOrWarn(registrar, "batch completion", action);
      else AfterCommitActions.runOrFail(registrar, "workflow completion", action);
      Thread.currentThread().setContextClassLoader(foreign);
      assertThrows(IllegalStateException.class, registered.get()::run);
      assertSame(foreign, Thread.currentThread().getContextClassLoader());
      assertSame(application, observed.get());
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
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

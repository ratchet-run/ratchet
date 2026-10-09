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
package run.ratchet.ri.cdi.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbException;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import run.ratchet.api.exception.PayloadTooLargeException;
import run.ratchet.ri.security.Utf8Length;

class JsonbPayloadSerializerTest {

  @Test
  void closeLogsJsonbCloseFailures() throws Exception {
    JsonbPayloadSerializer serializer = new JsonbPayloadSerializer();
    Jsonb jsonb = mock(Jsonb.class);
    RuntimeException failure = new RuntimeException("close failed");
    doThrow(failure).when(jsonb).close();
    setJsonb(serializer, jsonb);

    Logger logger = Logger.getLogger(JsonbPayloadSerializer.class.getName());
    List<LogRecord> records = new ArrayList<>();
    Handler handler =
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

    Level originalLevel = logger.getLevel();
    boolean originalUseParentHandlers = logger.getUseParentHandlers();
    logger.setLevel(Level.ALL);
    logger.setUseParentHandlers(false);
    logger.addHandler(handler);
    try {
      serializer.close();
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(originalLevel);
      logger.setUseParentHandlers(originalUseParentHandlers);
    }

    assertTrue(
        records.stream()
            .anyMatch(
                record ->
                    record.getLevel().intValue() >= Level.WARNING.intValue()
                        && record.getThrown() == failure
                        && record.getMessage().contains("Failed to close Jsonb")),
        "Jsonb close failure should be logged with its Throwable");
  }

  private static void setJsonb(JsonbPayloadSerializer serializer, Jsonb jsonb) throws Exception {
    Field field = JsonbPayloadSerializer.class.getDeclaredField("jsonb");
    field.setAccessible(true);
    field.set(serializer, jsonb);
  }

  @Test
  void boundedSerializationAcceptsExactUtf8BudgetAndRejectsOneByteLess() {
    JsonbPayloadSerializer serializer = new JsonbPayloadSerializer();
    try {
      String json = serializer.serialize("é😀");
      long bytes = Utf8Length.utf8Length(json);
      assertEquals(json, serializer.serialize("é😀", bytes));
      PayloadTooLargeException failure =
          assertThrows(
              PayloadTooLargeException.class, () -> serializer.serialize("é😀", bytes - 1));
      assertTrue(failure.actualBytes() > bytes - 1);
      assertTrue(failure.isLowerBound());
      assertTrue(failure.getMessage().contains("more than"));
    } finally {
      serializer.close();
    }
  }

  @Test
  void largeLazyListStopsReadingNearTheByteBudget() {
    AtomicInteger reads = new AtomicInteger();
    List<String> huge =
        new AbstractList<>() {
          @Override
          public String get(int index) {
            reads.incrementAndGet();
            return "abcdefghij";
          }

          @Override
          public int size() {
            return 10_000_000;
          }
        };
    JsonbPayloadSerializer serializer = new JsonbPayloadSerializer();
    try {
      PayloadTooLargeException failure =
          assertThrows(PayloadTooLargeException.class, () -> serializer.serialize(huge, 64));
      assertTrue(failure.actualBytes() > 64 && failure.actualBytes() <= 68);
      assertTrue(reads.get() < 10_000, "Provider must stop reading the oversized input early");
    } finally {
      serializer.close();
    }
  }

  @Test
  void wrappedWriterFailureIsRecognized() throws Exception {
    Jsonb jsonb = mock(Jsonb.class);
    Object payload = new Object();
    doAnswer(
            invocation -> {
              Writer writer = invocation.getArgument(1);
              try {
                writer.write("xxxxxxxxx");
              } catch (RuntimeException failure) {
                throw new JsonbException("provider wrapper", new IllegalStateException(failure));
              }
              return null;
            })
        .when(jsonb)
        .toJson(same(payload), any(Writer.class));
    JsonbPayloadSerializer serializer = new JsonbPayloadSerializer();
    setJsonb(serializer, jsonb);
    PayloadTooLargeException failure =
        assertThrows(PayloadTooLargeException.class, () -> serializer.serialize(payload, 4));
    assertEquals(5, failure.actualBytes());
    assertTrue(failure.isLowerBound());
  }

  @Test
  void boundedWriterCountsSplitPairsAndUnpairedSurrogatesLikeUtf8Length() throws Exception {
    for (String value : List.of("aé\uD83D\uDE00z", "a\uD83Dz\uDC00", "x\uD83D")) {
      Class<?> type = Class.forName(JsonbPayloadSerializer.class.getName() + "$BoundedWriter");
      Constructor<?> constructor = type.getDeclaredConstructor(long.class);
      constructor.setAccessible(true);
      long bytes = Utf8Length.utf8Length(value);
      Writer writer = (Writer) constructor.newInstance(bytes);
      // One-char calls ensure the high/low halves straddle distinct writes.
      for (int i = 0; i < value.length(); i++) {
        if (i % 2 == 0) {
          writer.write(new char[] {value.charAt(i)}, 0, 1);
        } else {
          writer.write(value, i, 1);
        }
      }
      Method finish = type.getDeclaredMethod("finish");
      finish.setAccessible(true);
      assertEquals(value, finish.invoke(writer));
      Field counted = type.getDeclaredField("bytes");
      counted.setAccessible(true);
      assertEquals(bytes, counted.getLong(writer));
    }
  }
}

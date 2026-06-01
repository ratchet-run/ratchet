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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import jakarta.json.bind.Jsonb;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

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
}

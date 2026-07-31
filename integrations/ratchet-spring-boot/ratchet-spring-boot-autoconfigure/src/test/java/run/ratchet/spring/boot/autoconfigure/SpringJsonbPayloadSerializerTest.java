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
package run.ratchet.spring.boot.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.json.bind.Jsonb;
import jakarta.json.bind.JsonbBuilder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SpringJsonbPayloadSerializerTest {

  @Test
  void roundTripsPayloadsWithJsonbSemantics() throws Exception {
    Jsonb jsonb = JsonbBuilder.create();
    SpringJsonbPayloadSerializer serializer = new SpringJsonbPayloadSerializer(jsonb, false);
    SamplePayload payload = new SamplePayload("alpha", 7);

    SamplePayload restored =
        serializer.deserialize(serializer.serialize(payload), SamplePayload.class);

    assertEquals(payload.name, restored.name);
    assertEquals(payload.count, restored.count);
    assertNull(serializer.serialize(null));
    assertNull(serializer.deserialize("", SamplePayload.class));
    jsonb.close();
  }

  @Test
  void closesOwnedJsonbExactlyOnce() throws Exception {
    AtomicInteger closes = new AtomicInteger();
    Jsonb jsonb = closeCountingJsonb(JsonbBuilder.create(), closes);
    SpringJsonbPayloadSerializer serializer = new SpringJsonbPayloadSerializer(jsonb, true);

    serializer.destroy();
    serializer.destroy();

    assertTrue(serializer.ownsJsonb());
    assertEquals(1, closes.get());
  }

  @Test
  void neverClosesBorrowedJsonb() throws Exception {
    AtomicInteger closes = new AtomicInteger();
    Jsonb jsonb = closeCountingJsonb(JsonbBuilder.create(), closes);
    SpringJsonbPayloadSerializer serializer = new SpringJsonbPayloadSerializer(jsonb, false);

    serializer.destroy();
    serializer.destroy();

    assertFalse(serializer.ownsJsonb());
    assertEquals(0, closes.get());
    jsonb.close();
    assertEquals(1, closes.get());
  }

  private static Jsonb closeCountingJsonb(Jsonb delegate, AtomicInteger closes) {
    return (Jsonb)
        Proxy.newProxyInstance(
            Jsonb.class.getClassLoader(),
            new Class<?>[] {Jsonb.class},
            (proxy, method, arguments) -> {
              if (method.getName().equals("close")) {
                closes.incrementAndGet();
              }
              try {
                return method.invoke(delegate, arguments);
              } catch (InvocationTargetException exception) {
                throw exception.getCause();
              }
            });
  }

  public static final class SamplePayload {

    public String name;
    public int count;

    public SamplePayload() {}

    SamplePayload(String name, int count) {
      this.name = name;
      this.count = count;
    }
  }
}

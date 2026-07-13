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
package run.ratchet.ri.payload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.Test;
import run.ratchet.api.SerializableCheckedRunnable;
import run.ratchet.store.entity.JobPayload;

class JobPayloadFactoryTest {

  @Test
  void constructorArgumentDoesNotShiftEarlierCapturedArguments() {
    String first = "first";

    JobPayload payload =
        JobPayloadFactory.fromLambda(
            (SerializableCheckedRunnable)
                () -> PayloadTarget.capture(first, new String("constructed")),
            List.of("second"));

    assertEquals(PayloadTarget.class.getName(), payload.target());
    assertEquals("capture", payload.method());
    assertEquals(List.of(first, "second"), payload.args());
  }

  @Test
  void reflectionLookupsAreCachedAcrossRepeatedConversions() throws Exception {
    StringFunction target = PayloadTarget::uppercase;
    StringFunction wrapper = value -> target.apply("cached");
    // The ClassValue caches are static and shared across tests; start from empty
    // per-class maps so the size assertions below measure only this test's work.
    ConcurrentMap<?, ?> visibility = cachedMap("VISIBILITY_CACHE", PayloadTarget.class);
    ConcurrentMap<?, ?> functional =
        cachedMap("FUNCTIONAL_INTERFACE_METHOD_CACHE", StringFunction.class);
    visibility.clear();
    functional.clear();

    JobPayload first = JobPayloadFactory.fromLambda(wrapper);
    assertEquals(1, visibility.size(), "first conversion must memoize one visibility verdict");
    assertEquals(
        1, functional.size(), "first conversion must memoize one functional-interface lookup");
    JobPayload second = JobPayloadFactory.fromLambda(wrapper);

    assertEquals(PayloadTarget.class.getName(), first.target());
    assertEquals(List.of("cached"), first.args());
    assertEquals(List.of("cached"), second.args());
    assertEquals(1, visibility.size(), "repeat conversions must reuse the memoized verdict");
    assertEquals(1, functional.size(), "repeat conversions must reuse the memoized lookup");
  }

  @Test
  void lambdaSerializationRejectsSerializableReplacementObjectsWithClearMessage() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                LambdaSerialization.toSerializedLambda(
                    new SerializableReplacement(), "Expected a serializable lambda"));

    assertTrue(thrown.getMessage().contains("Expected a serializable lambda"));
    assertInstanceOf(ClassCastException.class, thrown.getCause());
  }

  private static ConcurrentMap<?, ?> cachedMap(String name, Class<?> keyedOn)
      throws ReflectiveOperationException {
    Field field = JobPayloadFactory.class.getDeclaredField(name);
    field.setAccessible(true);
    ClassValue<?> cache = (ClassValue<?>) field.get(null);
    return (ConcurrentMap<?, ?>) cache.get(keyedOn);
  }

  @FunctionalInterface
  interface StringFunction extends Serializable {
    String apply(String value);
  }

  private static final class SerializableReplacement implements Serializable {
    @SuppressWarnings("unused")
    private Object writeReplace() {
      return "replacement";
    }
  }

  public static final class PayloadTarget {
    public static void capture(String first, String second) {}

    public static String uppercase(String value) {
      return value.toUpperCase();
    }
  }
}

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

    JobPayload first = JobPayloadFactory.fromLambda(wrapper);
    JobPayload second = JobPayloadFactory.fromLambda(wrapper);

    assertEquals(PayloadTarget.class.getName(), first.target());
    assertEquals(List.of("cached"), first.args());
    assertEquals(List.of("cached"), second.args());
    assertInstanceOf(ClassValue.class, cache("VISIBILITY_CACHE"));
    assertInstanceOf(ClassValue.class, cache("FUNCTIONAL_INTERFACE_METHOD_CACHE"));
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

  private static Object cache(String name) throws ReflectiveOperationException {
    Field field = JobPayloadFactory.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(null);
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

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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.Type;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobPayload;

class ArgumentMaterializerTest {

  @Test
  void restoresAllowedCustomArgumentFromItsJsonNativeShape() {
    JobPayload payload =
        payload(
            Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(CustomArgument.class)),
            List.of(Map.of("reference", "invoice-42", "attempt", new BigDecimal("3"))));

    JobPayload restored =
        ArgumentMaterializer.materialize(
            payload,
            new JsonbTestPayloadSerializer(),
            className -> className.equals(CustomArgument.class.getName()));

    assertEquals(List.of(new CustomArgument("invoice-42", 3)), restored.args());
  }

  @Test
  void rejectsDeniedCustomArgumentBeforeCallingSerializer() {
    JobPayload payload =
        payload(
            Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(CustomArgument.class)),
            List.of(Map.of("reference", "invoice-42", "attempt", 3)));

    SecurityException failure =
        assertThrows(
            SecurityException.class,
            () ->
                ArgumentMaterializer.materialize(payload, failIfCalledSerializer(), name -> false));

    assertEquals(
        "Class "
            + CustomArgument.class.getName()
            + " is not allowed for job argument deserialization.",
        failure.getMessage());
  }

  @Test
  void preservesJsonNativeArgumentsAndNumericCoercionWithoutTypedDeserialization() {
    Map<String, Object> map = Map.of("key", "value");
    JobPayload payload =
        payload(
            Type.getMethodDescriptor(
                Type.VOID_TYPE,
                Type.getType(String.class),
                Type.BOOLEAN_TYPE,
                Type.LONG_TYPE,
                Type.getType(Map.class)),
            List.of("text", true, new BigDecimal("42"), map));

    JobPayload restored =
        ArgumentMaterializer.materialize(payload, failIfCalledSerializer(), name -> false);

    assertEquals(List.of("text", true, 42L, map), restored.args());
  }

  private static JobPayload payload(String descriptor, List<Object> args) {
    return new JobPayload("allowed.Target", "run", descriptor, true, args);
  }

  private static PayloadSerializer failIfCalledSerializer() {
    return new PayloadSerializer() {
      @Override
      public String serialize(Object payload) {
        throw new AssertionError("serializer must not be called");
      }

      @Override
      public <T> T deserialize(String json, Class<T> type) {
        throw new AssertionError("serializer must not be called");
      }
    };
  }

  public record CustomArgument(String reference, int attempt) {}
}

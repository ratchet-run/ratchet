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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.Type;
import run.ratchet.api.JobContext;
import run.ratchet.ri.testutil.JsonbTestPayloadSerializer;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobPayload;

class ArgumentMaterializerTest {

  @Test
  void restoringCapturedArgumentsPreservesRuntimeSlots() {
    JobPayload payload =
        new JobPayload(
            "allowed.Target",
            "run",
            Type.getMethodDescriptor(
                Type.VOID_TYPE, Type.getType(CustomArgument.class), Type.getType(JobContext.class)),
            true,
            Arrays.asList(Map.of("reference", "invoice-42", "attempt", 3), null),
            Arrays.asList(null, 0));
    JobPayload restored =
        ArgumentMaterializer.materialize(
            payload, null, new JsonbTestPayloadSerializer(), name -> true);
    assertEquals(Arrays.asList(new CustomArgument("invoice-42", 3), null), restored.args());
    assertEquals(Arrays.asList(null, 0), restored.runtimeArgIndexes());
  }

  @Test
  void restoresAllowedCustomArgumentFromItsJsonNativeShape() {
    JobPayload payload =
        payload(
            Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(CustomArgument.class)),
            List.of(Map.of("reference", "invoice-42", "attempt", new BigDecimal("3"))));

    JobPayload restored =
        ArgumentMaterializer.materialize(
            payload,
            null,
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
                ArgumentMaterializer.materialize(
                    payload, null, failIfCalledSerializer(), name -> false));

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
        ArgumentMaterializer.materialize(payload, null, failIfCalledSerializer(), name -> false);

    assertEquals(List.of("text", true, 42L, map), restored.args());
  }

  @Test
  void restoresGenericContainersAndNumericElements() throws Exception {
    CustomArgument expected = new CustomArgument("invoice-42", 3);
    Object shape = Map.of("reference", "invoice-42", "attempt", 3);
    for (String fixture : List.of("list", "set", "map", "nested", "longs")) {
      Object input =
          switch (fixture) {
            case "map" -> Map.of("key", shape);
            case "nested" -> List.of(List.of(shape));
            case "longs" -> List.of(new BigDecimal("42"));
            default -> List.of(shape);
          };
      Object expectedValue =
          switch (fixture) {
            case "set" -> Set.of(expected);
            case "map" -> Map.of("key", expected);
            case "nested" -> List.of(List.of(expected));
            case "longs" -> List.of(42L);
            default -> List.of(expected);
          };
      Method method = fixture(fixture);
      JobPayload restored =
          ArgumentMaterializer.materialize(
              payload(Type.getMethodDescriptor(method), List.of(input)),
              method.getGenericParameterTypes(),
              new JsonbTestPayloadSerializer(),
              name -> name.equals(CustomArgument.class.getName()));
      Object actual = restored.args().get(0);
      assertEquals(expectedValue, actual, fixture);
    }
  }

  @Test
  void preservesNativeAndUnresolvableGenericArguments() throws Exception {
    for (String fixture :
        List.of(
            "strings",
            "objects",
            "nativeNested",
            "wildcard",
            "variable",
            "directVariable",
            "bounded",
            "mixedBounded")) {
      Method method = fixture(fixture);
      Object value =
          fixture.equals("objects") || fixture.equals("mixedBounded")
              ? Map.of("key", "value")
              : List.of("value");
      JobPayload payload = payload(Type.getMethodDescriptor(method), List.of(value));
      assertSame(
          payload,
          ArgumentMaterializer.materialize(
              payload, method.getGenericParameterTypes(), failIfCalledSerializer(), name -> false));
    }
  }

  @Test
  void preservesAssignableInterfaceAndAbstractGenericArguments() throws Exception {
    for (String fixture :
        List.of(
            "numbers",
            "charSequences",
            "serializables",
            "iterableStrings",
            "iterableCustomArguments",
            "comparables",
            "nestedIterables")) {
      Method method = fixture(fixture);
      Object shape = Map.of("reference", "invoice-42", "attempt", 3);
      Object value =
          switch (fixture) {
            case "numbers" -> List.of(new BigDecimal("42"));
            case "iterableCustomArguments" -> List.of(shape);
            case "comparables" -> Map.of("key", "value");
            case "nestedIterables" -> List.of(List.of(shape));
            default -> List.of("value");
          };
      Set<String> checked = new HashSet<>();
      JobPayload payload = payload(Type.getMethodDescriptor(method), List.of(value));

      JobPayload restored =
          ArgumentMaterializer.materialize(
              payload,
              method.getGenericParameterTypes(),
              failIfCalledSerializer(),
              name -> {
                checked.add(name);
                return name.startsWith(ArgumentMaterializerTest.class.getPackageName() + ".");
              });

      assertSame(payload, restored, fixture);
      assertSame(value, restored.args().get(0), fixture);
      assertEquals(Set.of(), checked, fixture);
    }
  }

  @Test
  void restoresConcreteHolderWithoutCheckingAbstractTypeArgument() throws Exception {
    Method method = fixture("holder");
    Set<String> checked = new HashSet<>();
    JobPayload restored =
        ArgumentMaterializer.materialize(
            payload(
                Type.getMethodDescriptor(method),
                List.of(
                    Map.of(
                        "first",
                        Map.of("reference", "invoice-42", "attempt", 3),
                        "second",
                        new BigDecimal("42")))),
            method.getGenericParameterTypes(),
            new JsonbTestPayloadSerializer(),
            name -> {
              checked.add(name);
              return name.startsWith(ArgumentMaterializerTest.class.getPackageName() + ".");
            });

    GenericArgumentHolder<?, ?> holder = (GenericArgumentHolder<?, ?>) restored.args().get(0);
    assertEquals(new CustomArgument("invoice-42", 3), holder.getFirst());
    assertEquals(new BigDecimal("42"), holder.getSecond());
    assertEquals(
        Set.of(GenericArgumentHolder.class.getName(), CustomArgument.class.getName()), checked);
  }

  @Test
  void restoresNativeElementSetFromItsJsonArrayShape() throws Exception {
    Method method = fixture("stringSet");
    JobPayload restored =
        ArgumentMaterializer.materialize(
            payload(Type.getMethodDescriptor(method), List.of(List.of("a", "b"))),
            method.getGenericParameterTypes(),
            new JsonbTestPayloadSerializer(),
            name -> false);
    assertEquals(Set.of("a", "b"), restored.args().get(0));
  }

  @Test
  void rejectsDeniedGenericElementBeforeCallingSerializer() throws Exception {
    Method method = fixture("nested");
    SecurityException failure =
        assertThrows(
            SecurityException.class,
            () ->
                ArgumentMaterializer.materialize(
                    payload(Type.getMethodDescriptor(method), List.of(List.of(List.of(Map.of())))),
                    method.getGenericParameterTypes(),
                    failIfCalledSerializer(),
                    name -> false));
    assertEquals(
        "Class "
            + CustomArgument.class.getName()
            + " is not allowed for job argument deserialization.",
        failure.getMessage());
  }

  @Test
  void missingOrMismatchedGenericTypesUseRawBehavior() throws Exception {
    Method method = fixture("list");
    JobPayload payload = payload(Type.getMethodDescriptor(method), List.of(List.of(Map.of())));
    assertSame(
        payload,
        ArgumentMaterializer.materialize(payload, null, failIfCalledSerializer(), name -> false));
    assertSame(
        payload,
        ArgumentMaterializer.materialize(
            payload,
            Arrays.copyOf(method.getGenericParameterTypes(), 0),
            failIfCalledSerializer(),
            name -> false));
  }

  private static Method fixture(String name) {
    return Arrays.stream(ArgumentMaterializerTest.class.getDeclaredMethods())
        .filter(method -> method.getName().equals(name))
        .findFirst()
        .orElseThrow();
  }

  static void list(List<CustomArgument> value) {}

  static void set(Set<CustomArgument> value) {}

  static void map(Map<String, CustomArgument> value) {}

  static void nested(List<List<CustomArgument>> value) {}

  static void numbers(List<Number> value) {}

  static void charSequences(List<CharSequence> value) {}

  static void serializables(List<Serializable> value) {}

  static void iterableStrings(Iterable<String> value) {}

  static void iterableCustomArguments(Iterable<CustomArgument> value) {}

  static void comparables(Map<String, Comparable<?>> value) {}

  static void nestedIterables(List<Iterable<CustomArgument>> value) {}

  static void holder(GenericArgumentHolder<CustomArgument, Number> value) {}

  static void longs(List<Long> value) {}

  static void strings(List<String> value) {}

  static void stringSet(Set<String> value) {}

  static void objects(Map<String, Object> value) {}

  static void nativeNested(List<List<String>> value) {}

  static void wildcard(List<?> value) {}

  static <T> void variable(List<T> value) {}

  static <T> void directVariable(T value) {}

  static void mixedBounded(Map<CustomArgument, ? extends CustomArgument> value) {}

  static void bounded(List<? extends CustomArgument> value) {}

  private static JobPayload payload(String descriptor, List<Object> args) {
    return new JobPayload("allowed.Target", "run", descriptor, true, args, null);
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

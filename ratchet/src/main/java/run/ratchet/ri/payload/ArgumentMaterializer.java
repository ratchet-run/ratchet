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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import run.ratchet.spi.ClassPolicy;
import run.ratchet.spi.PayloadSerializer;
import run.ratchet.store.entity.JobPayload;

/** Restores persisted invocation arguments to the types declared by their target method. */
public final class ArgumentMaterializer {

  private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_WRAPPER =
      Map.of(
          boolean.class, Boolean.class,
          byte.class, Byte.class,
          char.class, Character.class,
          short.class, Short.class,
          int.class, Integer.class,
          long.class, Long.class,
          float.class, Float.class,
          double.class, Double.class);

  private ArgumentMaterializer() {}

  /**
   * Returns a payload whose arguments are assignable to the raw parameter types encoded in its
   * method descriptor.
   *
   * <p>This method is an internal execution-boundary helper. Callers must first validate the
   * payload target through {@link ClassPolicy} and match the exact method descriptor against that
   * target class. Values that already have an assignable JSON-native type are retained, and numeric
   * values use {@link ArgumentCoercion}. A value that requires typed deserialization is restored
   * only when its declared class is also allowed by the policy. Generic collections and maps are
   * restored when their type tree contains elements requiring typed deserialization. Native Object,
   * String, Boolean and unbounded wildcard elements are retained, including nested standard
   * containers. Type variables, bounded wildcards and generic arrays use raw behavior, as do all
   * arguments when generic parameter types are absent or their count differs from the descriptor.
   * Before generic restoration, the policy checks every declared class except standard containers
   * and JDK value types (primitives, wrappers, String, BigDecimal, BigInteger and Object).
   */
  public static JobPayload materialize(
      JobPayload payload,
      Type[] genericParameterTypes,
      PayloadSerializer serializer,
      ClassPolicy classPolicy) {
    Objects.requireNonNull(payload, "payload");
    List<Object> args = payload.args();
    if (args == null || args.isEmpty()) {
      return payload;
    }

    Class<?>[] parameterTypes = payload.parameterTypes();
    if (parameterTypes.length != args.size()) {
      return payload;
    }

    boolean useGenericTypes =
        genericParameterTypes != null && genericParameterTypes.length == parameterTypes.length;
    Object[] original = args.toArray();
    Object[] restored = ArgumentCoercion.coerce(parameterTypes, original);
    for (int i = 0; i < parameterTypes.length; i++) {
      Object value = restored[i];
      Class<?> rawType = boxed(parameterTypes[i]);
      Type genericType = useGenericTypes ? genericParameterTypes[i] : parameterTypes[i];
      boolean typedRestore =
          genericType instanceof ParameterizedType
              && isRestorable(genericType)
              && (needsTypedRestore(genericType) || (value != null && !rawType.isInstance(value)));
      if (value == null || (!typedRestore && rawType.isInstance(value))) {
        continue;
      }

      if (typedRestore) {
        requireGenericAllowed(genericType, classPolicy);
      } else {
        requireAllowed(parameterTypes[i], classPolicy);
      }
      if (serializer == null) {
        throw new IllegalStateException(
            "PayloadSerializer is required to restore argument " + i + " as " + rawType.getName());
      }
      Object typed =
          typedRestore
              ? serializer.deserialize(serializer.serialize(value), genericType)
              : serializer.deserialize(serializer.serialize(value), rawType);
      if (typed == null) {
        throw new IllegalArgumentException(
            "PayloadSerializer returned null while restoring argument "
                + i
                + " as "
                + rawType.getName());
      }
      if (!rawType.isInstance(typed)) {
        throw new IllegalArgumentException(
            "PayloadSerializer restored argument "
                + i
                + " as "
                + typed.getClass().getName()
                + ", expected "
                + rawType.getName());
      }
      if (restored == original) {
        restored = original.clone();
      }
      restored[i] = typed;
    }

    if (restored == original) {
      return payload;
    }
    return new JobPayload(
        payload.target(),
        payload.method(),
        payload.methodDescriptor(),
        payload.isStatic(),
        Collections.unmodifiableList(Arrays.asList(restored)),
        payload.runtimeArgIndexes());
  }

  private static boolean isRestorable(Type type) {
    if (type instanceof Class<?>) {
      return true;
    }
    if (type instanceof ParameterizedType parameterized) {
      Type owner = parameterized.getOwnerType();
      return (owner == null || isRestorable(owner))
          && Arrays.stream(parameterized.getActualTypeArguments())
              .allMatch(ArgumentMaterializer::isRestorable);
    }
    return type instanceof WildcardType wildcard
        && wildcard.getLowerBounds().length == 0
        && Arrays.equals(wildcard.getUpperBounds(), new Type[] {Object.class});
  }

  private static boolean isContainer(Class<?> type) {
    return type == List.class || type == Set.class || type == Collection.class || type == Map.class;
  }

  private static boolean needsTypedRestore(Type type) {
    if (type instanceof WildcardType) {
      return false;
    }
    if (type instanceof Class<?> clazz) {
      return clazz != Object.class && clazz != String.class && clazz != Boolean.class;
    }
    ParameterizedType parameterized = (ParameterizedType) type;
    return !(parameterized.getRawType() instanceof Class<?> raw && isContainer(raw))
        || Arrays.stream(parameterized.getActualTypeArguments())
            .anyMatch(ArgumentMaterializer::needsTypedRestore);
  }

  private static void requireGenericAllowed(Type type, ClassPolicy classPolicy) {
    if (type instanceof Class<?> clazz) {
      while (clazz.isArray()) {
        clazz = clazz.getComponentType();
      }
      if (!clazz.isPrimitive()
          && !PRIMITIVE_TO_WRAPPER.containsValue(clazz)
          && clazz != Void.class
          && clazz != String.class
          && clazz != BigDecimal.class
          && clazz != BigInteger.class
          && clazz != Object.class
          && !isContainer(clazz)) {
        requireAllowed(clazz, classPolicy);
      }
    } else if (type instanceof ParameterizedType parameterized) {
      requireGenericAllowed(parameterized.getRawType(), classPolicy);
      if (parameterized.getOwnerType() != null) {
        requireGenericAllowed(parameterized.getOwnerType(), classPolicy);
      }
      for (Type argument : parameterized.getActualTypeArguments()) {
        requireGenericAllowed(argument, classPolicy);
      }
    }
  }

  private static Class<?> boxed(Class<?> type) {
    return type.isPrimitive() ? PRIMITIVE_TO_WRAPPER.get(type) : type;
  }

  private static void requireAllowed(Class<?> type, ClassPolicy classPolicy) {
    Class<?> policyType = type;
    while (policyType.isArray()) {
      policyType = policyType.getComponentType();
    }
    if (policyType.isPrimitive()) {
      return;
    }
    if (classPolicy == null || !classPolicy.isAllowed(policyType.getName())) {
      throw new SecurityException(
          "Class " + policyType.getName() + " is not allowed for job argument deserialization.");
    }
  }
}

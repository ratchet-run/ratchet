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

/**
 * Coerces deserialized invocation arguments to a target method's parameter types before reflective
 * invocation.
 *
 * <p>Captured lambda and workflow-condition arguments persist as a {@code List<Object>} with no
 * element type, so JSON-B rehydrates every JSON number as a {@link java.math.BigDecimal}. {@link
 * java.lang.reflect.Method#invoke} does not widen or narrow boxed numbers, so a {@code BigDecimal}
 * handed to a {@code long} parameter fails with {@code argument type mismatch}. This restores the
 * captured numeric value to the type the target method actually declares.
 *
 * <p>Only numeric coercion is performed. Strings, booleans, nulls, and values already assignable to
 * the parameter type pass through untouched; anything else is left for {@code Method.invoke} to
 * reject with its own clearer error.
 */
public final class ArgumentCoercion {

  private ArgumentCoercion() {}

  /**
   * Returns {@code args} with each numeric element converted to its corresponding parameter type.
   * The input array is never mutated; a copy is made only when at least one element changes. When
   * the argument count does not match the parameter count the array is returned unchanged, letting
   * the reflective call surface the arity mismatch.
   */
  public static Object[] coerce(Class<?>[] parameterTypes, Object[] args) {
    if (args == null || parameterTypes.length != args.length) {
      return args;
    }
    Object[] coerced = args;
    for (int i = 0; i < args.length; i++) {
      Object converted = coerceArgument(args[i], parameterTypes[i]);
      if (converted != args[i]) {
        if (coerced == args) {
          coerced = args.clone();
        }
        coerced[i] = converted;
      }
    }
    return coerced;
  }

  private static Object coerceArgument(Object value, Class<?> type) {
    if (!(value instanceof Number number) || type.isInstance(value)) {
      return value;
    }
    if (type == long.class || type == Long.class) {
      return number.longValue();
    }
    if (type == int.class || type == Integer.class) {
      return number.intValue();
    }
    if (type == double.class || type == Double.class) {
      return number.doubleValue();
    }
    if (type == float.class || type == Float.class) {
      return number.floatValue();
    }
    if (type == short.class || type == Short.class) {
      return number.shortValue();
    }
    if (type == byte.class || type == Byte.class) {
      return number.byteValue();
    }
    return value;
  }
}

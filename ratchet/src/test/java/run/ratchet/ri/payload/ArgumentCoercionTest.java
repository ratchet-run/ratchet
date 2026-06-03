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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class ArgumentCoercionTest {

  @Test
  void convertsJsonNumberToEachPrimitiveParameterType() {
    BigDecimal n = new BigDecimal("42");
    assertEquals(42L, coerceOne(long.class, n));
    assertEquals(42, coerceOne(int.class, n));
    assertEquals((short) 42, coerceOne(short.class, n));
    assertEquals((byte) 42, coerceOne(byte.class, n));
    assertEquals(42.0d, coerceOne(double.class, n));
    assertEquals(42.0f, coerceOne(float.class, n));
  }

  @Test
  void convertsJsonNumberToBoxedNumberParameterType() {
    Object coerced = coerceOne(Long.class, new BigDecimal("7"));
    assertEquals(Long.class, coerced.getClass());
    assertEquals(7L, coerced);
  }

  @Test
  void leavesAssignableAndNonNumericArgumentsUntouched() {
    assertEquals("text", coerceOne(String.class, "text"));
    assertEquals(Boolean.TRUE, coerceOne(boolean.class, Boolean.TRUE));
    assertEquals(5L, coerceOne(Number.class, 5L)); // already a Number assignable to Number
  }

  @Test
  void passesNullThrough() {
    Object[] args = {null};
    Object[] result = ArgumentCoercion.coerce(new Class<?>[] {long.class}, args);
    assertArrayEquals(new Object[] {null}, result);
  }

  @Test
  void returnsSameArrayWhenNothingChanges() {
    Object[] args = {"a", Boolean.FALSE};
    Object[] result = ArgumentCoercion.coerce(new Class<?>[] {String.class, boolean.class}, args);
    assertSame(args, result, "no copy should be made when no element needs coercion");
  }

  @Test
  void doesNotMutateInputArrayWhenCoercing() {
    Object[] args = {new BigDecimal("9")};
    Object[] result = ArgumentCoercion.coerce(new Class<?>[] {long.class}, args);
    assertEquals(new BigDecimal("9"), args[0], "input array must be left intact");
    assertEquals(9L, result[0]);
  }

  @Test
  void returnsInputUnchangedOnArityMismatch() {
    Object[] args = {new BigDecimal("1"), new BigDecimal("2")};
    Object[] result = ArgumentCoercion.coerce(new Class<?>[] {long.class}, args);
    assertSame(args, result, "arity mismatch is left for Method.invoke to reject");
  }

  private static Object coerceOne(Class<?> parameterType, Object arg) {
    return ArgumentCoercion.coerce(new Class<?>[] {parameterType}, new Object[] {arg})[0];
  }
}

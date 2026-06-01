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
package run.ratchet.store.entity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class JobPayloadTest {

  @Test
  void noParams_returnsEmptyArray() {
    Class<?>[] types = payload("()V").parameterTypes();
    assertEquals(0, types.length);
  }

  @Test
  void singleInt() {
    assertArrayEquals(new Class<?>[] {int.class}, payload("(I)V").parameterTypes());
  }

  @Test
  void singleBoolean() {
    assertArrayEquals(new Class<?>[] {boolean.class}, payload("(Z)V").parameterTypes());
  }

  @Test
  void singleLong() {
    assertArrayEquals(new Class<?>[] {long.class}, payload("(J)V").parameterTypes());
  }

  @Test
  void singleDouble() {
    assertArrayEquals(new Class<?>[] {double.class}, payload("(D)V").parameterTypes());
  }

  @Test
  void stringParam() {
    assertArrayEquals(
        new Class<?>[] {String.class}, payload("(Ljava/lang/String;)V").parameterTypes());
  }

  @Test
  void intArray() {
    assertArrayEquals(new Class<?>[] {int[].class}, payload("([I)V").parameterTypes());
  }

  @Test
  void objectArray() {
    assertArrayEquals(
        new Class<?>[] {String[].class}, payload("([Ljava/lang/String;)V").parameterTypes());
  }

  @Test
  void multipleParams() {
    Class<?>[] expected = {int.class, String.class, double.class};
    assertArrayEquals(expected, payload("(ILjava/lang/String;D)V").parameterTypes());
  }

  @Test
  void returnTypeDoesNotAffectParams() {
    // Same params, different return type — should parse identically
    Class<?>[] fromVoid = payload("(I)V").parameterTypes();
    Class<?>[] fromString = payload("(I)Ljava/lang/String;").parameterTypes();
    assertArrayEquals(fromVoid, fromString);
  }

  @Test
  void unknownParameterTypeThrowsIllegalStateException() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> payload("(Lcom/example/MissingType;)V").parameterTypes());

    assertEquals(
        "Cannot resolve parameter types from descriptor '(Lcom/example/MissingType;)V'",
        thrown.getMessage());
    assertInstanceOf(ClassNotFoundException.class, thrown.getCause());
  }

  @Test
  void nullDescriptorThrowsRuntimeException() {
    assertThrows(RuntimeException.class, () -> payload(null).parameterTypes());
  }

  @Test
  void emptyDescriptorThrowsRuntimeException() {
    assertThrows(RuntimeException.class, () -> payload("").parameterTypes());
  }

  private JobPayload payload(String descriptor) {
    return new JobPayload("com.example.Foo", "bar", descriptor, false, List.of());
  }
}

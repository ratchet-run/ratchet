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
package run.ratchet.store.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ExtensionValidationTest {

  @Test
  void requireKey_accepts255Characters() {
    assertDoesNotThrow(() -> ExtensionValidation.requireKey("x".repeat(255)));
  }

  @Test
  void requireKey_rejects256Characters() {
    assertThrows(
        IllegalArgumentException.class, () -> ExtensionValidation.requireKey("x".repeat(256)));
  }

  @Test
  void requireNamespace_accepts64Characters() {
    assertDoesNotThrow(() -> ExtensionValidation.requireNamespace("x".repeat(64)));
  }

  @Test
  void requireNamespace_rejects65Characters() {
    assertThrows(
        IllegalArgumentException.class, () -> ExtensionValidation.requireNamespace("x".repeat(65)));
  }

  @Test
  void requireValue_acceptsNull() {
    assertDoesNotThrow(() -> ExtensionValidation.requireValue(null));
  }

  @Test
  void requireValue_accepts1024Characters() {
    assertDoesNotThrow(() -> ExtensionValidation.requireValue("x".repeat(1024)));
  }

  @Test
  void requireValue_rejects1025Characters() {
    assertThrows(
        IllegalArgumentException.class, () -> ExtensionValidation.requireValue("x".repeat(1025)));
  }
}

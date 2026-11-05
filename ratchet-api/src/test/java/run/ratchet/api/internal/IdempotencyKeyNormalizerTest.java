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
package run.ratchet.api.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class IdempotencyKeyNormalizerTest {

  @Test
  void trimsKey() {
    assertEquals("batch-42", IdempotencyKeyNormalizer.normalize("  batch-42 "));
  }

  @Test
  void nullOrBlankIsAbsent() {
    assertNull(IdempotencyKeyNormalizer.normalize(null));
    assertNull(IdempotencyKeyNormalizer.normalize(""));
    assertNull(IdempotencyKeyNormalizer.normalize(" \t\n"));
  }

  @Test
  void keyThatTrimsToNothingIsAbsent() {
    // Not blank by isBlank(), but trim() strips every character below U+0021.
    assertNull(IdempotencyKeyNormalizer.normalize("\u0000"));
    assertNull(IdempotencyKeyNormalizer.normalize(" \u0001\u001f "));
    // Blank by isBlank(), but trim() leaves it in place.
    assertNull(IdempotencyKeyNormalizer.normalize("\u2003"));
  }

  @Test
  void accepts36CharactersAfterTrimming() {
    String key = "k".repeat(36);
    assertEquals(key, IdempotencyKeyNormalizer.normalize(" " + key + " "));
  }

  @Test
  void rejects37CharactersWithExistingMessage() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> IdempotencyKeyNormalizer.normalize(" " + "k".repeat(37) + " "));
    assertEquals("Idempotency key must be at most 36 characters, got 37", exception.getMessage());
  }
}
